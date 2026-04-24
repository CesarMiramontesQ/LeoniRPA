"""Rutas de ventas (vistas y APIs)."""

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.router import get_current_user, require_roles
from app.constants.ventas_export_partes import (
    NUMEROS_PARTE_EXPORT_REGISTROS_VENTAS,
    VENTAS_EXPORT_PRODUCTO_CONDENSADO_PREFIX_LEN,
)
from app.db import crud
from app.db.base import get_db
from app.db.models import User
from app.ventas.service import parse_period_filter, serialize_venta

router = APIRouter(tags=["ventas"])
templates = Jinja2Templates(directory="templates")


def _convertir_a_cdmx(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(ZoneInfo("America/Mexico_City"))
    dt_utc = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt_utc.astimezone(ZoneInfo("America/Mexico_City"))


def _datetime_cdmx_filter(dt: Optional[datetime], format_str: str = "%d/%m/%Y %H:%M") -> str:
    if dt is None:
        return "N/A"
    dt_cdmx = _convertir_a_cdmx(dt)
    return dt_cdmx.strftime(format_str)


def _to_iso_filter(value):
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


templates.env.filters["datetime_cdmx"] = _datetime_cdmx_filter
templates.env.filters["to_iso"] = _to_iso_filter


@router.get("/ventas")
async def ventas(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Página de ventas - requiere autenticación."""
    if current_user.rol == "admin":
        executions = await crud.list_sales_executions(db, limit=5)
    else:
        executions = await crud.list_sales_executions(db, user_id=current_user.id, limit=5)

    return templates.TemplateResponse(
        "ventas.html",
        {
            "request": request,
            "active_page": "ventas",
            "current_user": current_user,
            "executions": executions,
        },
    )


@router.get("/ventas-registros")
async def ventas_registros(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Página de registros de ventas - requiere autenticación."""
    total_ventas = await crud.count_ventas(db, only_with_sales_km=True)

    return templates.TemplateResponse(
        "ventas_registros.html",
        {
            "request": request,
            "active_page": "ventas_registros",
            "current_user": current_user,
            "total_ventas": total_ventas,
            "numeros_parte_export_registros": list(NUMEROS_PARTE_EXPORT_REGISTROS_VENTAS),
            "ventas_export_condensado_prefix_len": VENTAS_EXPORT_PRODUCTO_CONDENSADO_PREFIX_LEN,
        },
    )


@router.get("/api/ventas")
async def api_ventas(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    search: Optional[str] = None,
    cliente: Optional[str] = None,
    codigo_cliente: Optional[int] = None,
    periodo_inicio: Optional[str] = None,
    periodo_fin: Optional[str] = None,
    producto: Optional[str] = None,
    planta: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    """API para obtener ventas con filtros y paginación."""
    del request  # Mantener firma compatible sin usar la variable.
    del current_user

    limit = max(1, min(limit, 1000))
    offset = max(0, offset)

    try:
        periodo_inicio_dt = parse_period_filter(periodo_inicio, "periodo_inicio")
        periodo_fin_dt = parse_period_filter(periodo_fin, "periodo_fin")
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    ventas = await crud.list_ventas(
        db=db,
        limit=limit,
        offset=offset,
        search=search,
        cliente=cliente,
        codigo_cliente=codigo_cliente,
        periodo_inicio=periodo_inicio_dt,
        periodo_fin=periodo_fin_dt,
        producto=producto,
        planta=planta,
        only_with_sales_km=True,
    )
    total = await crud.count_ventas(
        db=db,
        search=search,
        cliente=cliente,
        codigo_cliente=codigo_cliente,
        periodo_inicio=periodo_inicio_dt,
        periodo_fin=periodo_fin_dt,
        producto=producto,
        planta=planta,
        only_with_sales_km=True,
    )

    return JSONResponse(
        {
            "ventas": [serialize_venta(venta) for venta in ventas],
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    )


@router.get("/api/ventas/export-excel-partes-prioritarias")
async def api_ventas_export_excel_partes_prioritarias(
    current_user: User = Depends(require_roles(["GM"])),
    db: AsyncSession = Depends(get_db),
):
    """Exporta ventas prioritarias para BOM breaking y seguimiento comercial."""
    del current_user
    import io

    import pandas as pd

    ventas = await crud.list_ventas_por_productos_in(
        db,
        list(NUMEROS_PARTE_EXPORT_REGISTROS_VENTAS),
        only_with_sales_km=True,
        producto_condensado_prefix_len=VENTAS_EXPORT_PRODUCTO_CONDENSADO_PREFIX_LEN,
    )
    seen_keys = set()
    ventas_sin_dup = []
    for venta in ventas:
        prod_clave = (venta.producto or "").strip() or (venta.producto_condensado or "").strip()
        if venta.codigo_cliente is not None:
            key = (venta.codigo_cliente, prod_clave)
        else:
            key = (None, venta.id, prod_clave)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        ventas_sin_dup.append(venta)

    pares_xref = []
    for venta in ventas_sin_dup:
        if venta.codigo_cliente is None:
            continue
        prod_xref = (venta.producto or "").strip()
        if not prod_xref:
            continue
        pares_xref.append((str(venta.codigo_cliente).strip(), prod_xref))

    xref_map = await crud.map_cross_reference_por_pares_customer_material(db, pares_xref)

    filas = []
    for venta in ventas_sin_dup:
        cr_val = None
        if venta.codigo_cliente is not None:
            pk = (str(venta.codigo_cliente).strip(), (venta.producto or "").strip())
            if pk[1]:
                cr_val = xref_map.get(pk)
        filas.append(
            {
                "ID": venta.id,
                "Cliente": venta.cliente,
                "Código Cliente": venta.codigo_cliente,
                "Grupo": venta.grupo.grupo if venta.grupo else None,
                "Período": venta.periodo.strftime("%Y-%m-%d") if venta.periodo else None,
                "Producto": venta.producto,
                "Producto condensado": venta.producto_condensado,
                "Cross Reference": cr_val,
                "Descripción producto": venta.descripcion_producto,
                "Planta": venta.planta,
                "Sales KM": float(venta.sales_km) if venta.sales_km is not None else None,
                "Turnover w/o metal": float(venta.turnover_wo_metal)
                if venta.turnover_wo_metal is not None
                else None,
                "Precio Full Metal KM": float(venta.precio_full_metal_km)
                if venta.precio_full_metal_km is not None
                else None,
            }
        )

    dataframe = pd.DataFrame(filas)
    buffer = io.BytesIO()
    dataframe.to_excel(buffer, index=False, engine="openpyxl", sheet_name="Ventas")
    buffer.seek(0)
    nombre = f"ventas_numeros_parte_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{nombre}"'},
    )

