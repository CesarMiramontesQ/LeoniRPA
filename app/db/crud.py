"""Operaciones CRUD para usuarios y ejecuciones."""
from sqlalchemy import select, desc, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from typing import Optional, List
from datetime import datetime
from app.db.models import User, ExecutionHistory, ExecutionStatus
from app.core.security import hash_password


async def get_user_by_email(db: AsyncSession, email: str) -> Optional[User]:
    """Obtiene un usuario por email."""
    result = await db.execute(select(User).where(User.email == email))
    return result.scalar_one_or_none()


async def get_user_by_id(db: AsyncSession, user_id: int) -> Optional[User]:
    """Obtiene un usuario por ID."""
    result = await db.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()


async def create_user(
    db: AsyncSession,
    email: str,
    password: str,
    nombre: Optional[str] = None,
    rol: str = "operador"
) -> User:
    """Crea un nuevo usuario."""
    hashed_pwd = hash_password(password)
    db_user = User(
        email=email,
        password_hash=hashed_pwd,
        nombre=nombre,
        rol=rol,
        activo=True
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user


async def update_user_role(
    db: AsyncSession,
    user_id: int,
    rol: str
) -> Optional[User]:
    """Actualiza el rol de un usuario."""
    user = await get_user_by_id(db, user_id)
    if user:
        user.rol = rol
        await db.commit()
        await db.refresh(user)
    return user


async def update_user(
    db: AsyncSession,
    user_id: int,
    email: str,
    nombre: Optional[str] = None,
    rol: str = "operador",
    password: Optional[str] = None
) -> Optional[User]:
    """Actualiza un usuario."""
    user = await get_user_by_id(db, user_id)
    if not user:
        return None
    
    # Verificar si el email ya existe en otro usuario
    existing_user = await get_user_by_email(db, email)
    if existing_user and existing_user.id != user_id:
        raise ValueError("El email ya está registrado")
    
    user.email = email
    user.nombre = nombre
    user.rol = rol
    
    # Solo actualizar contraseña si se proporciona
    if password:
        user.password_hash = hash_password(password)
    
    await db.commit()
    await db.refresh(user)
    return user


# ==================== CRUD para ExecutionHistory ====================

async def create_execution(
    db: AsyncSession,
    user_id: int,
    fecha_inicio_periodo: datetime,
    fecha_fin_periodo: datetime,
    sistema_sap: Optional[str] = None,
    transaccion: Optional[str] = None,
    maquina: Optional[str] = None
) -> ExecutionHistory:
    """Crea un nuevo registro de ejecución."""
    execution = ExecutionHistory(
        user_id=user_id,
        fecha_inicio_periodo=fecha_inicio_periodo,
        fecha_fin_periodo=fecha_fin_periodo,
        estado=ExecutionStatus.PENDING,
        sistema_sap=sistema_sap,
        transaccion=transaccion,
        maquina=maquina
    )
    db.add(execution)
    await db.commit()
    await db.refresh(execution)
    return execution


async def get_execution_by_id(
    db: AsyncSession,
    execution_id: int
) -> Optional[ExecutionHistory]:
    """Obtiene una ejecución por ID con información del usuario."""
    result = await db.execute(
        select(ExecutionHistory)
        .options(selectinload(ExecutionHistory.user))
        .where(ExecutionHistory.id == execution_id)
    )
    return result.scalar_one_or_none()


async def update_execution_status(
    db: AsyncSession,
    execution_id: int,
    estado: ExecutionStatus,
    fecha_inicio_ejecucion: Optional[datetime] = None,
    fecha_fin_ejecucion: Optional[datetime] = None,
    duracion_segundos: Optional[int] = None,
    archivo_ruta: Optional[str] = None,
    archivo_nombre: Optional[str] = None,
    mensaje_error: Optional[str] = None,
    stack_trace: Optional[str] = None
) -> Optional[ExecutionHistory]:
    """Actualiza el estado y otros campos de una ejecución."""
    execution = await get_execution_by_id(db, execution_id)
    if not execution:
        return None
    
    execution.estado = estado
    if fecha_inicio_ejecucion is not None:
        execution.fecha_inicio_ejecucion = fecha_inicio_ejecucion
    if fecha_fin_ejecucion is not None:
        execution.fecha_fin_ejecucion = fecha_fin_ejecucion
    if duracion_segundos is not None:
        execution.duracion_segundos = duracion_segundos
    if archivo_ruta is not None:
        execution.archivo_ruta = archivo_ruta
    if archivo_nombre is not None:
        execution.archivo_nombre = archivo_nombre
    if mensaje_error is not None:
        execution.mensaje_error = mensaje_error
    if stack_trace is not None:
        execution.stack_trace = stack_trace
    
    await db.commit()
    await db.refresh(execution)
    return execution


async def list_executions(
    db: AsyncSession,
    user_id: Optional[int] = None,
    estado: Optional[ExecutionStatus] = None,
    limit: int = 100,
    offset: int = 0
) -> List[ExecutionHistory]:
    """Lista ejecuciones con filtros opcionales."""
    query = select(ExecutionHistory).options(selectinload(ExecutionHistory.user))
    
    if user_id is not None:
        query = query.where(ExecutionHistory.user_id == user_id)
    if estado is not None:
        query = query.where(ExecutionHistory.estado == estado)
    
    query = query.order_by(desc(ExecutionHistory.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_executions(
    db: AsyncSession,
    user_id: Optional[int] = None,
    estado: Optional[ExecutionStatus] = None
) -> int:
    """Cuenta el total de ejecuciones con filtros opcionales."""
    query = select(func.count(ExecutionHistory.id))
    
    if user_id is not None:
        query = query.where(ExecutionHistory.user_id == user_id)
    if estado is not None:
        query = query.where(ExecutionHistory.estado == estado)
    
    result = await db.execute(query)
    return result.scalar_one()

