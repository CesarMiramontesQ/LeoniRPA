"""Schemas Pydantic para ejecuciones."""
from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime
from app.db.models import ExecutionStatus


class ExecutionCreate(BaseModel):
    """Schema para crear una nueva ejecución."""
    fecha_inicio_periodo: datetime
    fecha_fin_periodo: datetime
    sistema_sap: Optional[str] = None
    transaccion: Optional[str] = None
    maquina: Optional[str] = None
    
    @field_validator("fecha_fin_periodo")
    @classmethod
    def validate_fecha_fin(cls, v: datetime, info) -> datetime:
        """Valida que la fecha fin sea posterior a la fecha inicio."""
        if "fecha_inicio_periodo" in info.data:
            if v < info.data["fecha_inicio_periodo"]:
                raise ValueError("La fecha fin del periodo debe ser posterior a la fecha inicio")
        return v


class ExecutionUpdate(BaseModel):
    """Schema para actualizar una ejecución."""
    estado: Optional[ExecutionStatus] = None
    fecha_inicio_ejecucion: Optional[datetime] = None
    fecha_fin_ejecucion: Optional[datetime] = None
    duracion_segundos: Optional[int] = None
    archivo_ruta: Optional[str] = None
    archivo_nombre: Optional[str] = None
    mensaje_error: Optional[str] = None
    stack_trace: Optional[str] = None


class ExecutionResponse(BaseModel):
    """Schema de respuesta con datos de ejecución."""
    id: int
    created_at: datetime
    user_id: int
    user_email: Optional[str] = None
    user_nombre: Optional[str] = None
    fecha_inicio_periodo: datetime
    fecha_fin_periodo: datetime
    archivo_ruta: Optional[str] = None
    archivo_nombre: Optional[str] = None
    estado: ExecutionStatus
    fecha_inicio_ejecucion: Optional[datetime] = None
    fecha_fin_ejecucion: Optional[datetime] = None
    duracion_segundos: Optional[int] = None
    sistema_sap: Optional[str] = None
    transaccion: Optional[str] = None
    maquina: Optional[str] = None
    mensaje_error: Optional[str] = None
    stack_trace: Optional[str] = None
    
    class Config:
        from_attributes = True


class ExecutionListResponse(BaseModel):
    """Schema para respuesta de lista de ejecuciones."""
    total: int
    limit: int
    offset: int
    items: list[ExecutionResponse]

