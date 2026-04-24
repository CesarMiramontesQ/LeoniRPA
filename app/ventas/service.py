"""Servicios de dominio para vistas y APIs de ventas."""

from datetime import datetime
from typing import Optional

from app.db.models import Venta


def parse_period_filter(value: Optional[str], field_name: str) -> Optional[datetime]:
    """Convierte YYYY-MM-DD a datetime o lanza ValueError."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Formato inválido en {field_name}. Use YYYY-MM-DD."
        ) from exc


def serialize_venta(venta: Venta) -> dict:
    """Serializa una venta para respuesta JSON."""
    return {
        "id": venta.id,
        "cliente": venta.cliente,
        "codigo_cliente": venta.codigo_cliente,
        "grupo": venta.grupo.grupo if venta.grupo else None,
        "unidad_negocio": venta.unidad_negocio,
        "periodo": venta.periodo.strftime("%Y-%m-%d") if venta.periodo else None,
        "producto_condensado": venta.producto_condensado,
        "region_asc": venta.region_asc,
        "planta": venta.planta,
        "ship_to_party": venta.ship_to_party,
        "producto": venta.producto,
        "descripcion_producto": venta.descripcion_producto,
        "turnover_wo_metal": float(venta.turnover_wo_metal)
        if venta.turnover_wo_metal
        else None,
        "oe_turnover_like_fi": float(venta.oe_turnover_like_fi)
        if venta.oe_turnover_like_fi
        else None,
        "copper_sales_cuv": float(venta.copper_sales_cuv)
        if venta.copper_sales_cuv
        else None,
        "cu_sales_effect": float(venta.cu_sales_effect) if venta.cu_sales_effect else None,
        "cu_result": float(venta.cu_result) if venta.cu_result else None,
        "quantity_oe_to_m": float(venta.quantity_oe_to_m)
        if venta.quantity_oe_to_m
        else None,
        "quantity_oe_to_ft": float(venta.quantity_oe_to_ft)
        if venta.quantity_oe_to_ft
        else None,
        "cu_weight_techn_cut": float(venta.cu_weight_techn_cut)
        if venta.cu_weight_techn_cut
        else None,
        "cu_weight_sales_cuv": float(venta.cu_weight_sales_cuv)
        if venta.cu_weight_sales_cuv
        else None,
        "conversion_ft_a_m": float(venta.conversion_ft_a_m)
        if venta.conversion_ft_a_m
        else None,
        "sales_total_mts": float(venta.sales_total_mts) if venta.sales_total_mts else None,
        "sales_km": float(venta.sales_km) if venta.sales_km else None,
        "precio_exmetal_km": float(venta.precio_exmetal_km)
        if venta.precio_exmetal_km
        else None,
        "precio_full_metal_km": float(venta.precio_full_metal_km)
        if venta.precio_full_metal_km
        else None,
        "precio_exmetal_m": float(venta.precio_exmetal_m)
        if venta.precio_exmetal_m
        else None,
        "precio_full_metal_m": float(venta.precio_full_metal_m)
        if venta.precio_full_metal_m
        else None,
        "created_at": venta.created_at.isoformat() if venta.created_at else None,
    }

