#!/usr/bin/env python3
"""
Script para calcular qty_total de una parte (ej: 76828002K).
Muestra el detalle por ítem del BOM según la regla UOM (M vs no M).
Uso: python scripts/calcular_qty_total_parte.py 76828002K
"""
import asyncio
import os
import sys
from decimal import Decimal

# Añadir raíz del proyecto al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import AsyncSessionLocal
from app.db.crud import get_parte_by_numero


async def main():
    numero_parte = (sys.argv[1] or "76828002K").strip()
    async with AsyncSessionLocal() as db:
        parte = await get_parte_by_numero(db, numero_parte)
        if not parte:
            print(f"No se encontró la parte '{numero_parte}'.")
            return

        parte_id = parte.id
        # Detalle: cada ítem del BOM con qty, measure, comp_no, kgm y contribución
        result = await db.execute(text("""
            WITH componentes AS (
                SELECT
                    bi.qty::numeric AS qty,
                    upper(trim(coalesce(bi.measure, ''))) AS measure,
                    pcomp.numero_parte AS comp_no
                FROM bom_item bi
                JOIN bom_revision br
                    ON br.id = bi.bom_revision_id
                   AND br.effective_to IS NULL
                JOIN bom b
                    ON b.id = br.bom_id
                JOIN partes pcomp
                    ON pcomp.id = bi.componente_id
                WHERE b.parte_id = :parte_id
            ),
            componentes_con_kgm AS (
                SELECT
                    c.qty,
                    c.measure,
                    c.comp_no,
                    pn.kgm
                FROM componentes c
                LEFT JOIN peso_neto pn
                    ON pn.numero_parte = c.comp_no
            )
            SELECT
                comp_no,
                qty,
                measure,
                kgm,
                CASE
                    WHEN measure = 'M' THEN (qty / 1000.0) * COALESCE(kgm, 0)
                    ELSE (qty / 1000.0)
                END AS contribucion
            FROM componentes_con_kgm
            ORDER BY comp_no
        """), {"parte_id": parte_id})

        rows = result.all()
        if not rows:
            print(f"Parte: {numero_parte} (id={parte_id})")
            print("No tiene ítems en BOM vigente. qty_total = 0")
            return

        total = Decimal("0")
        print(f"Parte: {numero_parte} (id={parte_id})")
        print("-" * 80)
        print(f"{'Componente':<25} {'qty':>12} {'UOM':>6} {'kgm':>12} {'contribución':>14}")
        print("-" * 80)
        for comp_no, qty, measure, kgm, contribucion in rows:
            total += contribucion or Decimal("0")
            kgm_str = f"{float(kgm):.6f}" if kgm is not None else "NULL"
            print(f"{comp_no:<25} {float(qty):>12.2f} {measure or '':>6} {kgm_str:>12} {float(contribucion or 0):>14.6f}")
        print("-" * 80)
        print(f"qty_total = {float(total):.6f}")
        print()
        # Comparar con el valor guardado en partes
        from app.db.models import Parte
        r = await db.execute(select(Parte.qty_total).where(Parte.id == parte_id))
        guardado = r.scalar_one_or_none()
        if guardado is not None:
            print(f"En tabla partes.qty_total = {float(guardado):.6f}")


if __name__ == "__main__":
    asyncio.run(main())
