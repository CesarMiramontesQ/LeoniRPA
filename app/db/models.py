"""Modelos de base de datos."""
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, ForeignKey, Text, Enum as SQLEnum, Numeric, Index, UniqueConstraint, CheckConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from enum import Enum as PyEnum
from app.db.base import Base


class User(Base):
    """Modelo de usuario."""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    nombre = Column(String, nullable=True)
    password_hash = Column(String, nullable=False)
    rol = Column(String, default="operador", nullable=False)  # admin, operador, auditor
    activo = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_login = Column(DateTime(timezone=True), nullable=True)
    
    def __repr__(self):
        return f"<User(id={self.id}, email={self.email}, rol={self.rol})>"


class ExecutionStatus(PyEnum):
    """Estados posibles de una ejecución."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ExecutionHistory(Base):
    """Modelo para el historial de ejecuciones del proceso 'Descargar Compras del Mes'."""
    __tablename__ = "purcharsing_execution_history"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Fecha de creación del registro
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Usuario que ejecutó el proceso
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user = relationship("User", backref="executions")
    
    # Periodo de fechas
    fecha_inicio_periodo = Column(DateTime(timezone=True), nullable=False)
    fecha_fin_periodo = Column(DateTime(timezone=True), nullable=False)
    
    # Archivo generado
    archivo_ruta = Column(String, nullable=True)  # Ruta completa del archivo
    archivo_nombre = Column(String, nullable=True)  # Nombre del archivo
    
    # Estado del proceso
    estado = Column(
        SQLEnum(ExecutionStatus, name="execution_status"),
        nullable=False,
        default=ExecutionStatus.PENDING,
        index=True
    )
    
    # Fechas de ejecución
    fecha_inicio_ejecucion = Column(DateTime(timezone=True), nullable=True)
    fecha_fin_ejecucion = Column(DateTime(timezone=True), nullable=True)
    
    # Duración en segundos
    duracion_segundos = Column(Integer, nullable=True)
    
    # Información técnica
    sistema_sap = Column(String, nullable=True)  # Sistema SAP utilizado
    transaccion = Column(String, nullable=True)  # Transacción SAP ejecutada
    maquina = Column(String, nullable=True)  # Máquina/host donde se ejecutó
    
    # Información de errores
    mensaje_error = Column(Text, nullable=True)  # Mensaje de error descriptivo
    stack_trace = Column(Text, nullable=True)  # Stack trace completo del error
    
    def __repr__(self):
        return f"<ExecutionHistory(id={self.id}, user_id={self.user_id}, estado={self.estado.value}, created_at={self.created_at})>"


class SalesExecutionHistory(Base):
    """Modelo para el historial de ejecuciones del proceso 'Descargar Ventas del Mes'."""
    __tablename__ = "sales_execution_history"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Fecha de creación del registro
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Usuario que ejecutó el proceso
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user = relationship("User", backref="sales_executions")
    
    # Periodo de fechas
    fecha_inicio_periodo = Column(DateTime(timezone=True), nullable=False)
    fecha_fin_periodo = Column(DateTime(timezone=True), nullable=False)
    
    # Archivo generado
    archivo_ruta = Column(String, nullable=True)  # Ruta completa del archivo
    archivo_nombre = Column(String, nullable=True)  # Nombre del archivo
    
    # Estado del proceso
    estado = Column(
        SQLEnum(ExecutionStatus, name="execution_status"),
        nullable=False,
        default=ExecutionStatus.PENDING,
        index=True
    )
    
    # Fechas de ejecución
    fecha_inicio_ejecucion = Column(DateTime(timezone=True), nullable=True)
    fecha_fin_ejecucion = Column(DateTime(timezone=True), nullable=True)
    
    # Duración en segundos
    duracion_segundos = Column(Integer, nullable=True)
    
    # Información técnica
    sistema_sap = Column(String, nullable=True)  # Sistema SAP utilizado
    transaccion = Column(String, nullable=True)  # Transacción SAP ejecutada
    maquina = Column(String, nullable=True)  # Máquina/host donde se ejecutó
    
    # Información de errores
    mensaje_error = Column(Text, nullable=True)  # Mensaje de error descriptivo
    stack_trace = Column(Text, nullable=True)  # Stack trace completo del error
    
    def __repr__(self):
        return f"<SalesExecutionHistory(id={self.id}, user_id={self.user_id}, estado={self.estado.value}, created_at={self.created_at})>"


class PartRole(PyEnum):
    """Roles posibles de una parte."""
    FG = "FG"  # Finished Good (producto terminado)
    COMP = "COMP"  # Component (componente/materia prima)
    UNKNOWN = "UNKNOWN"  # Desconocido


class Part(Base):
    """Catálogo único de números de parte (tanto productos terminados como componentes/materiales)."""
    __tablename__ = "parts"
    
    # Número de parte como clave primaria
    part_no = Column(String, primary_key=True, index=True)
    
    # Descripción de la parte
    description = Column(Text, nullable=True)
    
    # Rol de la parte: FG (Finished Good), COMP (Component), UNKNOWN
    part_role = Column(
        SQLEnum(PartRole, name="part_role_enum"),
        nullable=True,
        default=PartRole.UNKNOWN
    )
    
    # Datos adicionales en formato JSON
    raw_data = Column(JSONB, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relaciones
    bom_as_fg = relationship("BomFlat", foreign_keys="BomFlat.fg_part_no", back_populates="fg_part")
    bom_as_component = relationship("BomFlat", foreign_keys="BomFlat.material", back_populates="material_part")
    
    def __repr__(self):
        return f"<Part(part_no={self.part_no}, description={self.description[:30] if self.description else 'N/A'}...)>"


class BomFlat(Base):
    """Tabla plana de BOM: una fila por cada material/componente dentro de un BOM."""
    __tablename__ = "bom_flat"
    
    # ID autoincremental
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    
    # Número de parte del producto terminado (padre)
    fg_part_no = Column(String, ForeignKey("parts.part_no"), nullable=False, index=True)
    
    # Código de planta
    plant_code = Column(String, nullable=False, index=True)
    
    # Metros base
    base_mts = Column(Numeric(18, 3), nullable=True)
    
    # Req D
    req_d = Column(Numeric(18, 3), nullable=True)
    
    # Material (número de parte del componente/material)
    material = Column(String, ForeignKey("parts.part_no"), nullable=False, index=True)
    
    # Descripción del material
    material_description = Column(Text, nullable=True)
    
    # Cantidad requerida
    qty = Column(Numeric(18, 6), nullable=False)
    
    # Unidad de medida
    uom = Column(String, nullable=False)
    
    # País de origen
    origin_country = Column(String, nullable=True)
    
    # Precio de venta
    sale_price = Column(Numeric(18, 6), nullable=True)
    
    # ID de ejecución para histórico (opcional)
    run_id = Column(BigInteger, nullable=True, index=True)
    
    # Timestamp de creación
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Relaciones
    fg_part = relationship("Part", foreign_keys=[fg_part_no], back_populates="bom_as_fg")
    material_part = relationship("Part", foreign_keys=[material], back_populates="bom_as_component")
    
    # Restricciones
    __table_args__ = (
        # Evitar duplicados: combinación única de fg_part_no, plant_code, material y run_id
        UniqueConstraint(
            'fg_part_no', 'plant_code', 'material',
            name='uq_bom_flat_fg_plant_material_run',
            postgresql_nulls_not_distinct=False
        ),
        # Índice compuesto para búsquedas por planta y producto terminado
        Index('ix_bom_flat_plant_fg', 'plant_code', 'fg_part_no'),
        # Check constraint: cantidad debe ser mayor a 0
        CheckConstraint('qty > 0', name='ck_bom_flat_qty_positive'),
    )
    
    def __repr__(self):
        return f"<BomFlat(id={self.id}, fg={self.fg_part_no}, material={self.material}, qty={self.qty})>"

