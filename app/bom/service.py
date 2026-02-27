"""Servicio de carga y mantenimiento de BOMs con historial de revisiones."""
import hashlib
import json
from datetime import date
from datetime import timedelta
from decimal import Decimal
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.bom.schemas import LoadBomInput, LoadBomResponse, BomComponenteInput
from app.db import crud


def _compute_bom_hash(componentes_ordenados: List[Dict[str, Any]]) -> str:
    """
    Calcula un hash determinista de los componentes para detectar cambios.
    Orden: por (item_no, qty) para que el hash sea estable.
    """
    if not componentes_ordenados:
        return hashlib.sha256(b"[]").hexdigest()
    # Cada elemento: (componente_id, item_no, qty, measure, comm_code, origin)
    payload = [
        (
            c["componente_id"],
            c.get("item_no") or "",
            str(c["qty"]),
            c.get("measure") or "",
            c.get("comm_code") or "",
            c.get("origin") or "",
        )
        for c in componentes_ordenados
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=False).encode()).hexdigest()


def _sort_componentes_for_hash(componentes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ordena componentes por item_no y qty para el cálculo del hash."""
    return sorted(
        componentes,
        key=lambda c: (c.get("item_no") or "", c.get("qty") or Decimal("0")),
    )


async def load_bom(db: AsyncSession, payload: LoadBomInput) -> LoadBomResponse:
    """
    Carga o actualiza un BOM con historial de revisiones.
    - Inserta/actualiza partes (padre y componentes).
    - Obtiene o crea el BOM por (parte_id, plant, usage, alternative).
    - Compara hash con la revisión vigente; si cambió, cierra la vigente y crea nueva con los items.
    - Usa una sola transacción; en error hace rollback.
    """
    today = date.today()
    try:
        # 1) Upsert parte padre
        parte_padre = await crud.upsert_parte(db, payload.parte_no, payload.descripcion_padre)
        parte_id = parte_padre.id

        # 2) Upsert partes de componentes y mapear numero_parte -> id
        numero_to_id: Dict[str, int] = {}
        for comp in payload.componentes:
            p = await crud.upsert_parte(db, comp.componente_no, comp.descripcion)
            numero_to_id[comp.componente_no] = p.id

        # 3) Get or create BOM
        bom = await crud.get_or_create_bom(
            db,
            parte_id=parte_id,
            plant=payload.plant,
            usage=payload.usage,
            alternative=payload.alternative,
            base_qty=payload.base_qty,
            reqd_qty=payload.reqd_qty,
            base_unit=payload.base_unit,
        )

        # 4) Lista de componentes con componente_id para hash e insert
        componentes_con_id: List[Dict[str, Any]] = [
            {
                "componente_id": numero_to_id[c.componente_no],
                "item_no": c.item_no,
                "qty": c.qty,
                "measure": c.measure,
                "comm_code": c.comm_code,
                "origin": c.origin,
            }
            for c in payload.componentes
        ]

        if not componentes_con_id:
            # BOM sin componentes: comparar hash vacío con revisión vigente
            new_hash = _compute_bom_hash([])
        else:
            sorted_for_hash = _sort_componentes_for_hash(componentes_con_id)
            new_hash = _compute_bom_hash(sorted_for_hash)

        # 5) Revisión vigente (solo una por BOM)
        current_rev = await crud.get_current_bom_revision(db, bom.id)

        # Si ya existe revisión vigente, comparar hash para decidir si crea nueva revisión.
        if current_rev is not None:
            if current_rev.hash == new_hash:
                await crud.actualizar_qty_total_parte(db, parte_id)
                await crud.recalcular_diferencia_parte(db, parte_id)
                await db.commit()
                return LoadBomResponse(
                    ok=True,
                    mensaje="BOM sin cambios: se conserva la revisión vigente.",
                    sin_cambios=True,
                    revision_anterior_cerrada=False,
                    nueva_revision_creada=False,
                    revision_no=current_rev.revision_no,
                )

            # Hubo cambios: cerrar revisión vigente y crear siguiente revisión.
            close_date = today
            if current_rev.effective_from is not None and close_date <= current_rev.effective_from:
                # Respeta check constraint: effective_to debe ser > effective_from.
                close_date = current_rev.effective_from + timedelta(days=1)

            await crud.close_bom_revision(db, current_rev.id, close_date)

            next_revision_no = (current_rev.revision_no or 0) + 1
            new_rev = await crud.create_bom_revision(
                db,
                bom_id=bom.id,
                revision_no=next_revision_no,
                effective_from=close_date,
                hash_value=new_hash,
                source="carga",
            )
            n = await crud.insert_bom_items(db, new_rev.id, componentes_con_id)
            await crud.actualizar_qty_total_parte(db, parte_id)
            await crud.recalcular_diferencia_parte(db, parte_id)
            await db.commit()
            return LoadBomResponse(
                ok=True,
                mensaje=f"BOM actualizado: revisión {next_revision_no} creada con {n} items.",
                sin_cambios=False,
                revision_anterior_cerrada=True,
                nueva_revision_creada=True,
                revision_no=next_revision_no,
                items_insertados=n,
            )

        # Primera revisión para este BOM
        new_rev = await crud.create_bom_revision(
            db,
            bom_id=bom.id,
            revision_no=1,
            effective_from=today,
            hash_value=new_hash,
            source="carga",
        )
        n = await crud.insert_bom_items(db, new_rev.id, componentes_con_id)
        await crud.actualizar_qty_total_parte(db, parte_id)
        await crud.recalcular_diferencia_parte(db, parte_id)
        await db.commit()
        return LoadBomResponse(
            ok=True,
            mensaje=f"BOM creado: revisión 1 creada con {n} items.",
            sin_cambios=False,
            revision_anterior_cerrada=False,
            nueva_revision_creada=True,
            revision_no=1,
            items_insertados=n,
        )

    except IntegrityError as e:
        await db.rollback()
        msg = str(e.orig) if hasattr(e, "orig") and e.orig else str(e)
        if "ux_bom_item_unique_component" in msg or "duplicate" in msg.lower():
            return LoadBomResponse(
                ok=False,
                mensaje="Componente duplicado en la misma revisión. No se permiten duplicados por (bom_revision_id, componente_id).",
                sin_cambios=False,
            )
        return LoadBomResponse(ok=False, mensaje=f"Error de integridad: {msg}", sin_cambios=False)
    except Exception as e:
        await db.rollback()
        return LoadBomResponse(ok=False, mensaje=f"Error: {e}", sin_cambios=False)
