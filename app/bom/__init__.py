"""Módulo BOM: carga y mantenimiento de BOMs con historial de revisiones."""
from app.bom.schemas import (
    BomComponenteInput,
    LoadBomInput,
    LoadBomResponse,
)
from app.bom.service import load_bom

__all__ = [
    "BomComponenteInput",
    "LoadBomInput",
    "LoadBomResponse",
    "load_bom",
]
