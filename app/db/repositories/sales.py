"""Repositorio de lecturas/escrituras de ventas y ejecuciones de ventas."""

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy import desc, func, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models import CrossReference, ExecutionStatus, SalesExecutionHistory, Venta


async def list_sales_executions(
    db: AsyncSession,
    user_id: Optional[int] = None,
    estado: Optional[ExecutionStatus] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[SalesExecutionHistory]:
    """Lista ejecuciones de ventas con filtros opcionales."""
    query = select(SalesExecutionHistory).options(selectinload(SalesExecutionHistory.user))
    if user_id is not None:
        query = query.where(SalesExecutionHistory.user_id == user_id)
    if estado is not None:
        query = query.where(SalesExecutionHistory.estado == estado)
    query = query.order_by(desc(SalesExecutionHistory.created_at)).limit(limit).offset(offset)
    result = await db.execute(query)
    return list(result.scalars().all())


async def map_cross_reference_por_pares_customer_material(
    db: AsyncSession,
    pares: List[Tuple[str, str]],
) -> Dict[Tuple[str, str], str]:
    """Mapea (customer, material) a customer_material."""
    if not pares:
        return {}
    seen_q = []
    seen_set = set()
    for customer, material in pares:
        customer = (customer or "").strip()
        material = (material or "").strip()
        if not customer or not material:
            continue
        if (customer, material) in seen_set:
            continue
        seen_set.add((customer, material))
        seen_q.append((customer, material))
    if not seen_q:
        return {}

    out: Dict[Tuple[str, str], str] = {}
    batch_size = 300
    for idx in range(0, len(seen_q), batch_size):
        batch = seen_q[idx : idx + batch_size]
        stmt = (
            select(
                CrossReference.customer,
                CrossReference.material,
                CrossReference.customer_material,
            )
            .where(tuple_(CrossReference.customer, CrossReference.material).in_(batch))
            .order_by(
                CrossReference.customer,
                CrossReference.material,
                CrossReference.customer_material,
            )
        )
        result = await db.execute(stmt)
        for customer, material, customer_material in result.all():
            key = (customer, material)
            if key not in out and (customer_material or "").strip():
                out[key] = (customer_material or "").strip()
    return out


async def list_ventas(
    db: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    search: Optional[str] = None,
    cliente: Optional[str] = None,
    codigo_cliente: Optional[int] = None,
    periodo_inicio: Optional[datetime] = None,
    periodo_fin: Optional[datetime] = None,
    producto: Optional[str] = None,
    planta: Optional[str] = None,
    only_with_sales_km: bool = False,
) -> List[Venta]:
    """Lista ventas con filtros opcionales."""
    query = select(Venta).options(selectinload(Venta.grupo))
    if only_with_sales_km:
        query = query.where(Venta.sales_km.isnot(None), Venta.sales_km != 0)
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Venta.cliente.ilike(search_pattern),
                Venta.producto.ilike(search_pattern),
                Venta.descripcion_producto.ilike(search_pattern),
                Venta.planta.ilike(search_pattern),
            )
        )
    if cliente:
        query = query.where(Venta.cliente.ilike(f"%{cliente}%"))
    if codigo_cliente is not None:
        query = query.where(Venta.codigo_cliente == codigo_cliente)
    if periodo_inicio:
        query = query.where(
            Venta.periodo >= periodo_inicio.date()
            if isinstance(periodo_inicio, datetime)
            else periodo_inicio
        )
    if periodo_fin:
        query = query.where(
            Venta.periodo <= periodo_fin.date()
            if isinstance(periodo_fin, datetime)
            else periodo_fin
        )
    if producto:
        query = query.where(Venta.producto.ilike(f"%{producto}%"))
    if planta:
        query = query.where(Venta.planta.ilike(f"%{planta}%"))
    query = query.order_by(desc(Venta.created_at)).limit(limit).offset(offset)
    result = await db.execute(query)
    return list(result.scalars().all())


async def list_ventas_por_productos_in(
    db: AsyncSession,
    productos: List[str],
    only_with_sales_km: bool = True,
    producto_condensado_prefix_len: Optional[int] = None,
) -> List[Venta]:
    """Lista ventas por lista de números de parte."""
    if not productos:
        return []
    productos_limpios = [str(p).strip() for p in productos if p is not None and str(p).strip()]
    if not productos_limpios:
        return []
    if producto_condensado_prefix_len is not None and producto_condensado_prefix_len > 0:
        prefijos = list({p[:producto_condensado_prefix_len] for p in productos_limpios})
        cond_match = Venta.producto_condensado.in_(prefijos)
    else:
        cond_match = Venta.producto_condensado.in_(productos_limpios)
    query = select(Venta).options(selectinload(Venta.grupo)).where(
        or_(
            cond_match,
            Venta.producto.in_(productos_limpios),
        )
    )
    if only_with_sales_km:
        query = query.where(Venta.sales_km.isnot(None), Venta.sales_km != 0)
    query = query.order_by(desc(Venta.created_at))
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_ventas(
    db: AsyncSession,
    search: Optional[str] = None,
    cliente: Optional[str] = None,
    codigo_cliente: Optional[int] = None,
    periodo_inicio: Optional[datetime] = None,
    periodo_fin: Optional[datetime] = None,
    producto: Optional[str] = None,
    planta: Optional[str] = None,
    only_with_sales_km: bool = False,
) -> int:
    """Cuenta ventas con filtros opcionales."""
    query = select(func.count(Venta.id))
    if only_with_sales_km:
        query = query.where(Venta.sales_km.isnot(None), Venta.sales_km != 0)
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Venta.cliente.ilike(search_pattern),
                Venta.producto.ilike(search_pattern),
                Venta.descripcion_producto.ilike(search_pattern),
                Venta.planta.ilike(search_pattern),
            )
        )
    if cliente:
        query = query.where(Venta.cliente.ilike(f"%{cliente}%"))
    if codigo_cliente is not None:
        query = query.where(Venta.codigo_cliente == codigo_cliente)
    if periodo_inicio:
        query = query.where(
            Venta.periodo >= periodo_inicio.date()
            if isinstance(periodo_inicio, datetime)
            else periodo_inicio
        )
    if periodo_fin:
        query = query.where(
            Venta.periodo <= periodo_fin.date()
            if isinstance(periodo_fin, datetime)
            else periodo_fin
        )
    if producto:
        query = query.where(Venta.producto.ilike(f"%{producto}%"))
    if planta:
        query = query.where(Venta.planta.ilike(f"%{planta}%"))
    result = await db.execute(query)
    return result.scalar() or 0

