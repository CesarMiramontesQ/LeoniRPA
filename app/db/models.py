"""Modelos de base de datos."""
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Text, Enum as SQLEnum
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
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

