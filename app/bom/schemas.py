"""Schemas Pydantic para carga de BOMs."""
from pydantic import BaseModel, Field
from typing import Optional, List
from decimal import Decimal


class BomComponenteInput(BaseModel):
    """Un componente dentro del BOM."""
    componente_no: str = Field(..., description="Número de parte del componente")
    descripcion: Optional[str] = None
    qty: Decimal = Field(..., ge=0, description="Cantidad")
    measure: Optional[str] = None
    origin: Optional[str] = None
    item_no: Optional[str] = None
    comm_code: Optional[str] = None


class LoadBomInput(BaseModel):
    """Entrada para cargar/actualizar un BOM."""
    parte_no: str = Field(..., description="Número de parte del padre")
    descripcion_padre: Optional[str] = None
    plant: str = Field(..., description="Planta, ej: US10")
    usage: str = Field(..., description="Uso, ej: 1")
    alternative: str = Field(..., description="Alternativa, ej: 01")
    base_qty: Optional[Decimal] = None
    reqd_qty: Optional[Decimal] = None
    base_unit: Optional[str] = None
    componentes: List[BomComponenteInput] = Field(default_factory=list)


class LoadBomResponse(BaseModel):
    """Salida de la carga de BOM."""
    ok: bool
    mensaje: str
    sin_cambios: bool = False
    revision_anterior_cerrada: bool = False
    nueva_revision_creada: bool = False
    revision_no: Optional[int] = None
    items_insertados: int = 0
