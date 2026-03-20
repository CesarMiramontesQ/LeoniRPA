"""Operaciones CRUD para usuarios, ejecuciones y BOM."""
import json
import math
from sqlalchemy import select, desc, func, or_, and_, String, text, delete, update, cast, tuple_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.postgresql import insert
from typing import Optional, List, Dict, Any, Set, Tuple
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal, InvalidOperation
from app.db.models import User, ExecutionHistory, SalesExecutionHistory, ExecutionStatus, Part, BomFlat, PartRole, Proveedor, Material, PrecioMaterial, Compra, PaisOrigenMaterial, ProveedorHistorial, ProveedorOperacion, MaterialHistorial, MaterialOperacion, PaisOrigenMaterialHistorial, PaisOrigenMaterialOperacion, PrecioMaterialHistorial, PrecioMaterialOperacion, ClienteGrupo, Venta, CargaProveedor, CargaProveedoresNacional, CargaProveedoresNacionalHistorial, CargaCliente, MasterUnificadoVirtuales, MasterUnificadoVirtualHistorial, MasterUnificadoVirtualOperacion, CargaProveedorHistorial, CargaProveedorOperacion, CargaClienteHistorial, CargaClienteOperacion, Cliente, Parte, TradingGood, TradingGoodHistorial, Bom, BomRevision, BomItem, BomHistorial, PesoNeto, PesoNetoHistorial, CrossReference, CrossReferenceHistorial, PrecioVenta, PrecioVentaHistorial, FraccionArancelariaHistorial
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


async def update_user_password(
    db: AsyncSession,
    user_id: int,
    new_password: str,
) -> Optional[User]:
    """Actualiza solo la contraseña de un usuario (para cambio por el propio usuario)."""
    user = await get_user_by_id(db, user_id)
    if not user:
        return None
    user.password_hash = hash_password(new_password)
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
) -> int:
    """Crea un nuevo registro de ejecución. Retorna el id asignado (evita acceder al objeto tras commit y MissingGreenlet)."""
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
    await db.flush()  # Asigna id; leer antes de commit para no tocar el objeto después
    eid = execution.id
    await db.commit()
    return eid


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
    stack_trace: Optional[str] = None,
    detalles: Optional[str] = None,
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
    if detalles is not None:
        execution.detalles = detalles
    
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


# ==================== CRUD para SalesExecutionHistory ====================

async def create_sales_execution(
    db: AsyncSession,
    user_id: int,
    fecha_inicio_periodo: datetime,
    fecha_fin_periodo: datetime,
    sistema_sap: Optional[str] = None,
    transaccion: Optional[str] = None,
    maquina: Optional[str] = None
) -> int:
    """Crea un nuevo registro de ejecución de ventas. Retorna el id asignado (evita MissingGreenlet tras commit)."""
    execution = SalesExecutionHistory(
        user_id=user_id,
        fecha_inicio_periodo=fecha_inicio_periodo,
        fecha_fin_periodo=fecha_fin_periodo,
        estado=ExecutionStatus.PENDING,
        sistema_sap=sistema_sap,
        transaccion=transaccion,
        maquina=maquina
    )
    db.add(execution)
    await db.flush()
    eid = execution.id
    await db.commit()
    return eid


async def get_sales_execution_by_id(
    db: AsyncSession,
    execution_id: int
) -> Optional[SalesExecutionHistory]:
    """Obtiene una ejecución de ventas por ID con información del usuario."""
    result = await db.execute(
        select(SalesExecutionHistory)
        .options(selectinload(SalesExecutionHistory.user))
        .where(SalesExecutionHistory.id == execution_id)
    )
    return result.scalar_one_or_none()


async def update_sales_execution_status(
    db: AsyncSession,
    execution_id: int,
    estado: ExecutionStatus,
    fecha_inicio_ejecucion: Optional[datetime] = None,
    fecha_fin_ejecucion: Optional[datetime] = None,
    duracion_segundos: Optional[int] = None,
    archivo_ruta: Optional[str] = None,
    archivo_nombre: Optional[str] = None,
    mensaje_error: Optional[str] = None,
    stack_trace: Optional[str] = None,
    detalles: Optional[str] = None,
) -> Optional[SalesExecutionHistory]:
    """Actualiza el estado y otros campos de una ejecución de ventas."""
    execution = await get_sales_execution_by_id(db, execution_id)
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
    if detalles is not None:
        execution.detalles = detalles
    
    await db.commit()
    await db.refresh(execution)
    return execution


async def list_sales_executions(
    db: AsyncSession,
    user_id: Optional[int] = None,
    estado: Optional[ExecutionStatus] = None,
    limit: int = 100,
    offset: int = 0
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


async def count_sales_executions(
    db: AsyncSession,
    user_id: Optional[int] = None,
    estado: Optional[ExecutionStatus] = None
) -> int:
    """Cuenta el total de ejecuciones de ventas con filtros opcionales."""
    query = select(func.count(SalesExecutionHistory.id))
    
    if user_id is not None:
        query = query.where(SalesExecutionHistory.user_id == user_id)
    if estado is not None:
        query = query.where(SalesExecutionHistory.estado == estado)
    
    result = await db.execute(query)
    return result.scalar_one()


# ==================== CRUD para Parts ====================

async def get_part_by_no(db: AsyncSession, part_no: str) -> Optional[Part]:
    """Obtiene una parte por su número."""
    result = await db.execute(select(Part).where(Part.part_no == part_no))
    return result.scalar_one_or_none()


async def create_part(
    db: AsyncSession,
    part_no: str,
    description: Optional[str] = None,
    part_role: Optional[PartRole] = PartRole.UNKNOWN,
    raw_data: Optional[Dict[str, Any]] = None
) -> Part:
    """Crea una nueva parte."""
    part = Part(
        part_no=part_no,
        description=description,
        part_role=part_role,
        raw_data=raw_data
    )
    db.add(part)
    await db.commit()
    await db.refresh(part)
    return part


async def upsert_part(
    db: AsyncSession,
    part_no: str,
    description: Optional[str] = None,
    part_role: Optional[PartRole] = None,
    raw_data: Optional[Dict[str, Any]] = None
) -> Part:
    """Inserta o actualiza una parte (upsert)."""
    stmt = insert(Part).values(
        part_no=part_no,
        description=description,
        part_role=part_role or PartRole.UNKNOWN,
        raw_data=raw_data
    )
    
    # En caso de conflicto, actualizar description si se proporciona uno nuevo
    update_dict = {"updated_at": func.now()}
    if description is not None:
        update_dict["description"] = description
    if part_role is not None:
        update_dict["part_role"] = part_role
    if raw_data is not None:
        update_dict["raw_data"] = raw_data
    
    stmt = stmt.on_conflict_do_update(
        index_elements=["part_no"],
        set_=update_dict
    )
    
    await db.execute(stmt)
    await db.commit()
    
    # Obtener el registro actualizado/insertado
    return await get_part_by_no(db, part_no)


async def bulk_upsert_parts(
    db: AsyncSession,
    parts_data: List[Dict[str, Any]]
) -> int:
    """Inserta o actualiza múltiples partes en batch."""
    if not parts_data:
        return 0
    
    stmt = insert(Part)
    stmt = stmt.on_conflict_do_update(
        index_elements=["part_no"],
        set_={
            "description": stmt.excluded.description,
            "part_role": stmt.excluded.part_role,
            "updated_at": func.now()
        }
    )
    
    await db.execute(stmt, parts_data)
    await db.commit()
    return len(parts_data)


async def list_parts(
    db: AsyncSession,
    part_role: Optional[PartRole] = None,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Part]:
    """Lista partes con filtros opcionales."""
    query = select(Part)
    
    if part_role is not None:
        query = query.where(Part.part_role == part_role)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Part.part_no.ilike(search_pattern),
                Part.description.ilike(search_pattern)
            )
        )
    
    query = query.order_by(Part.part_no).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_parts(
    db: AsyncSession,
    part_role: Optional[PartRole] = None,
    search: Optional[str] = None
) -> int:
    """Cuenta el total de partes con filtros opcionales."""
    query = select(func.count(Part.part_no))
    
    if part_role is not None:
        query = query.where(Part.part_role == part_role)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Part.part_no.ilike(search_pattern),
                Part.description.ilike(search_pattern)
            )
        )
    
    result = await db.execute(query)
    return result.scalar_one()


# ==================== CRUD para Parte, Bom, BomRevision, BomItem ====================

async def get_parte_by_numero(db: AsyncSession, numero_parte: str) -> Optional[Parte]:
    """Obtiene una parte por número de parte."""
    result = await db.execute(select(Parte).where(Parte.numero_parte == numero_parte))
    return result.scalar_one_or_none()


async def get_trading_good_by_numero_parte(db: AsyncSession, numero_parte: str):
    """Obtiene el registro de trading_goods por numero_parte."""
    result = await db.execute(
        select(TradingGood).where(TradingGood.numero_parte == (numero_parte or "").strip())
    )
    return result.scalar_one_or_none()


async def upsert_trading_good(
    db: AsyncSession,
    numero_parte: str,
    is_trading_good: bool,
    user_id: Optional[int] = None,
):
    """Inserta o actualiza is_trading_good para un numero_parte. Registra el cambio en trading_goods_historial. Devuelve el registro."""
    numero_parte = (numero_parte or "").strip()
    if not numero_parte:
        raise ValueError("numero_parte es requerido")
    is_tg = bool(is_trading_good)
    row = await get_trading_good_by_numero_parte(db, numero_parte)
    if row:
        estado_anterior = row.is_trading_good
        if estado_anterior == is_tg:
            return row
        row.is_trading_good = is_tg
        db.add(
            TradingGoodHistorial(
                numero_parte=numero_parte,
                estado_anterior=estado_anterior,
                estado_nuevo=is_tg,
                user_id=user_id,
            )
        )
        await db.commit()
        await db.refresh(row)
        return row
    row = TradingGood(numero_parte=numero_parte, is_trading_good=is_tg)
    db.add(row)
    db.add(
        TradingGoodHistorial(
            numero_parte=numero_parte,
            estado_anterior=None,
            estado_nuevo=is_tg,
            user_id=user_id,
        )
    )
    await db.commit()
    await db.refresh(row)
    return row


async def list_trading_goods(db: AsyncSession) -> List[Dict[str, Any]]:
    """Lista todos los registros de trading_goods ordenados por numero_parte."""
    result = await db.execute(
        select(TradingGood).order_by(TradingGood.numero_parte)
    )
    rows = result.scalars().all()
    return [
        {
            "numero_parte": r.numero_parte,
            "is_trading_good": r.is_trading_good,
            "updated_at": r.updated_at,
        }
        for r in rows
    ]


async def list_trading_goods_historial(
    db: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    numero_parte: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Lista el historial de cambios de trading_goods (estado anterior, nuevo, usuario)."""
    query = (
        select(TradingGoodHistorial)
        .options(selectinload(TradingGoodHistorial.user))
        .order_by(desc(TradingGoodHistorial.created_at))
        .limit(limit)
        .offset(offset)
    )
    if numero_parte and str(numero_parte).strip():
        query = query.where(TradingGoodHistorial.numero_parte == str(numero_parte).strip())
    result = await db.execute(query)
    rows = result.scalars().all()
    return [
        {
            "numero_parte": r.numero_parte,
            "estado_anterior": r.estado_anterior,
            "estado_nuevo": r.estado_nuevo,
            "user_id": r.user_id,
            "usuario_email": r.user.email if r.user else None,
            "usuario_nombre": getattr(r.user, "nombre", None) or (r.user.email if r.user else "—"),
            "created_at": r.created_at,
        }
        for r in rows
    ]


async def update_fraccion_parte(
    db: AsyncSession,
    numero_parte: str,
    fraccion: Optional[str],
) -> Parte:
    """
    Actualiza la fracción arancelaria de una parte por numero_parte.
    Si la parte no existe, crea un registro en partes con numero_parte y fraccion.
    """
    numero_parte = (numero_parte or "").strip()
    if not numero_parte:
        raise ValueError("numero_parte es requerido")
    parte = await get_parte_by_numero(db, numero_parte)
    if parte:
        parte.fraccion = (fraccion or "").strip() or None
        await db.commit()
        await db.refresh(parte)
        return parte
    # Crear parte nueva con solo numero_parte y fraccion
    nueva = Parte(
        numero_parte=numero_parte,
        descripcion=None,
        fraccion=(fraccion or "").strip() or None,
        valido=True,
        qty_total=Decimal("0"),
    )
    db.add(nueva)
    await db.commit()
    await db.refresh(nueva)
    return nueva


async def create_fraccion_arancelaria_historial(
    db: AsyncSession,
    user_id: int,
    numero_parte: str,
    fraccion_anterior: Optional[str],
    fraccion_nueva: Optional[str],
) -> FraccionArancelariaHistorial:
    """Registra un movimiento en el historial de fracciones arancelarias."""
    item = FraccionArancelariaHistorial(
        user_id=user_id,
        numero_parte=(numero_parte or "").strip(),
        fraccion_anterior=(fraccion_anterior or "").strip() or None,
        fraccion_nueva=(fraccion_nueva or "").strip() or None,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)
    return item


async def list_fraccion_arancelaria_historial(
    db: AsyncSession,
    user_id: Optional[int] = None,
    limit: int = 20,
    offset: int = 0,
) -> List[FraccionArancelariaHistorial]:
    """Lista movimientos del historial de fracciones arancelarias. Si user_id no se pasa, lista todos (admin)."""
    query = (
        select(FraccionArancelariaHistorial)
        .options(selectinload(FraccionArancelariaHistorial.user))
        .order_by(desc(FraccionArancelariaHistorial.created_at))
        .limit(limit)
        .offset(offset)
    )
    if user_id is not None:
        query = query.where(FraccionArancelariaHistorial.user_id == user_id)
    result = await db.execute(query)
    return list(result.scalars().all())


async def list_partes_numeros(db: AsyncSession, limit: Optional[int] = None) -> List[str]:
    """Lista numero_parte activos para actualización BOM: valido=True y con revisiones existentes."""
    revision_exists = (
        select(BomRevision.id)
        .join(Bom, Bom.id == BomRevision.bom_id)
        .where(Bom.parte_id == Parte.id)
        .limit(1)
    )
    query = (
        select(Parte.numero_parte)
        .where(
            Parte.valido.is_(True),
            revision_exists.exists(),
        )
        .order_by(Parte.numero_parte)
    )
    if limit is not None:
        query = query.limit(limit)
    result = await db.execute(query)
    return [row[0] for row in result.all()]


async def list_partes_numeros_true_sin_bom(db: AsyncSession, limit: Optional[int] = None) -> List[str]:
    """Lista numero_parte para carga inicial BOM: valido=True y sin revisiones registradas."""
    revision_exists = (
        select(BomRevision.id)
        .join(Bom, Bom.id == BomRevision.bom_id)
        .where(Bom.parte_id == Parte.id)
        .limit(1)
    )
    query = (
        select(Parte.numero_parte)
        .where(
            Parte.valido.is_(True),
            ~revision_exists.exists(),
        )
        .order_by(Parte.numero_parte)
    )
    if limit is not None:
        query = query.limit(limit)
    result = await db.execute(query)
    return [row[0] for row in result.all()]


async def list_partes_numeros_no_validos(db: AsyncSession, limit: Optional[int] = None) -> List[str]:
    """Lista numero_parte marcados como no válidos (valido=False)."""
    query = (
        select(Parte.numero_parte)
        .where(Parte.valido.is_(False))
        .order_by(Parte.numero_parte)
    )
    if limit is not None:
        query = query.limit(limit)
    result = await db.execute(query)
    return [row[0] for row in result.all()]


async def list_partes_numeros_diferencia_gt0(db: AsyncSession, limit: Optional[int] = None) -> List[str]:
    """Lista numero_parte válidos con BOM/revisión y diferencia > 0."""
    revision_exists = (
        select(BomRevision.id)
        .join(Bom, Bom.id == BomRevision.bom_id)
        .where(Bom.parte_id == Parte.id)
        .limit(1)
    )
    query = (
        select(Parte.numero_parte)
        .where(
            Parte.valido.is_(True),
            Parte.diferencia.is_not(None),
            Parte.diferencia > 0,
            revision_exists.exists(),
        )
        .order_by(Parte.numero_parte)
    )
    if limit is not None:
        query = query.limit(limit)
    result = await db.execute(query)
    return [row[0] for row in result.all()]


async def reset_bom_para_reproceso(db: AsyncSession) -> Dict[str, int]:
    """
    Reinicia datos BOM para reprocesar desde cero sin borrar la tabla partes.
    - Elimina bom (y por cascada bom_revision + bom_item).
    - Reactiva partes (valido=true).
    No hace commit.
    """
    total_bom = (await db.execute(select(func.count()).select_from(Bom))).scalar_one()
    total_rev = (await db.execute(select(func.count()).select_from(BomRevision))).scalar_one()
    total_items = (await db.execute(select(func.count()).select_from(BomItem))).scalar_one()
    total_partes = (await db.execute(select(func.count()).select_from(Parte))).scalar_one()

    await db.execute(delete(Bom))
    await db.execute(update(Parte).values(valido=True, qty_total=0, diferencia=None))
    await db.flush()

    return {
        "boms_eliminados": int(total_bom or 0),
        "revisiones_eliminadas": int(total_rev or 0),
        "items_eliminados": int(total_items or 0),
        "partes_reactivadas": int(total_partes or 0),
    }


async def set_parte_valido(db: AsyncSession, numero_parte: str, valido: bool) -> None:
    """Marca una parte como válida o no (p. ej. cuando SAP no encuentra el material). No hace commit."""
    parte = await get_parte_by_numero(db, numero_parte)
    if parte is not None:
        parte.valido = valido
        await db.flush()


async def actualizar_qty_total_parte(db: AsyncSession, parte_id: int) -> Decimal:
    """
    Recalcula y persiste qty_total para una parte con regla de UOM:
    - Si measure = 'M': (qty/1000) * kgm(componente)
    - Si measure != 'M': (qty/1000)
    Si falta kgm(componente) para un item en 'M', su contribución es 0.
    """
    await db.execute(text("""
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
                pn.kgm
            FROM componentes c
            LEFT JOIN peso_neto pn
                ON pn.numero_parte = c.comp_no
        ),
        total AS (
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN measure = 'M' THEN (qty / 1000.0) * COALESCE(kgm, 0)
                            ELSE (qty / 1000.0)
                        END
                    ),
                    0
                )::numeric(18, 6) AS qty_total_nuevo
            FROM componentes_con_kgm
        )
        UPDATE partes p
        SET qty_total = (SELECT qty_total_nuevo FROM total)
        WHERE p.id = :parte_id
    """), {"parte_id": parte_id})

    result = await db.execute(select(Parte.qty_total).where(Parte.id == parte_id))
    qty_total = result.scalar_one_or_none()
    await db.flush()
    return qty_total if qty_total is not None else Decimal("0")


async def recalcular_diferencia_parte(db: AsyncSession, parte_id: int) -> Optional[Decimal]:
    """
    Recalcula diferencia para una parte:
    - NULL si hay algún componente 'M' sin kgm
    - NULL si no existe kgm para la parte padre
    - En otro caso: qty_total - kgm(parte padre)
    """
    await db.execute(text("""
        UPDATE partes p
        SET diferencia = CASE
            WHEN EXISTS (
                SELECT 1
                FROM bom_item bi
                JOIN bom_revision br
                    ON br.id = bi.bom_revision_id
                   AND br.effective_to IS NULL
                JOIN bom b
                    ON b.id = br.bom_id
                JOIN partes pcomp
                    ON pcomp.id = bi.componente_id
                LEFT JOIN peso_neto pn_comp
                    ON pn_comp.numero_parte = pcomp.numero_parte
                WHERE b.parte_id = p.id
                  AND upper(trim(coalesce(bi.measure, ''))) = 'M'
                  AND pn_comp.kgm IS NULL
            ) THEN NULL
            WHEN (
                SELECT pn_padre.kgm
                FROM peso_neto pn_padre
                WHERE pn_padre.numero_parte = p.numero_parte
                LIMIT 1
            ) IS NULL THEN NULL
            ELSE p.qty_total - (
                SELECT pn_padre.kgm
                FROM peso_neto pn_padre
                WHERE pn_padre.numero_parte = p.numero_parte
                LIMIT 1
            )
        END
        WHERE p.id = :parte_id
    """), {"parte_id": parte_id})

    result = await db.execute(select(Parte.diferencia).where(Parte.id == parte_id))
    diferencia = result.scalar_one_or_none()
    await db.flush()
    return diferencia


async def recalcular_qty_total_partes(db: AsyncSession) -> Dict[str, int]:
    """
    Recalcula y persiste qty_total para TODAS las partes con regla de UOM:
    - measure = 'M' => (qty/1000) * kgm(componente)
    - measure != 'M' => qty/1000
    """
    update_with_bom = await db.execute(text("""
        WITH componentes AS (
            SELECT
                b.parte_id,
                bi.qty::numeric AS qty,
                upper(trim(coalesce(bi.measure, ''))) AS measure,
                pcomp.numero_parte AS comp_no
            FROM bom b
            JOIN bom_revision br ON br.bom_id = b.id
            JOIN bom_item bi ON bi.bom_revision_id = br.id
            JOIN partes pcomp ON pcomp.id = bi.componente_id
            WHERE br.effective_to IS NULL
        ),
        componentes_con_kgm AS (
            SELECT
                c.parte_id,
                c.qty,
                c.measure,
                pn.kgm
            FROM componentes c
            LEFT JOIN peso_neto pn
                ON pn.numero_parte = c.comp_no
        ),
        agg AS (
            SELECT
                parte_id,
                SUM(
                    CASE
                        WHEN measure = 'M' THEN (qty / 1000.0) * coalesce(kgm, 0)
                        ELSE (qty / 1000.0)
                    END
                )::numeric(18, 6) AS total_qty
            FROM componentes_con_kgm
            GROUP BY parte_id
        )
        UPDATE partes p
        SET qty_total = COALESCE(agg.total_qty, 0)
        FROM agg
        WHERE agg.parte_id = p.id
    """))

    update_without_bom = await db.execute(text("""
        UPDATE partes p
        SET qty_total = 0
        WHERE NOT EXISTS (
            SELECT 1
            FROM bom b
            JOIN bom_revision br ON br.bom_id = b.id
            JOIN bom_item bi ON bi.bom_revision_id = br.id
            WHERE br.effective_to IS NULL
              AND b.parte_id = p.id
        )
    """))

    await db.flush()
    return {
        "qty_total_actualizado_con_bom": int(update_with_bom.rowcount or 0),
        "qty_total_en_cero_sin_bom": int(update_without_bom.rowcount or 0),
    }


async def recalcular_diferencia_partes(db: AsyncSession) -> Dict[str, int]:
    """
    Recalcula y persiste partes.diferencia = qty_total - kgm (peso_neto).
    Regla:
    - Si hay componentes 'M' sin kgm, diferencia = NULL.
    - Si no existe kgm para la parte padre, diferencia = NULL.
    - En otro caso, diferencia = qty_total - kgm(parte padre).
    """
    update_with_kgm = await db.execute(text("""
        WITH faltantes_m AS (
            SELECT DISTINCT b.parte_id
            FROM bom_item bi
            JOIN bom_revision br
                ON br.id = bi.bom_revision_id
               AND br.effective_to IS NULL
            JOIN bom b
                ON b.id = br.bom_id
            JOIN partes pcomp
                ON pcomp.id = bi.componente_id
            LEFT JOIN peso_neto pn_comp
                ON pn_comp.numero_parte = pcomp.numero_parte
            WHERE upper(trim(coalesce(bi.measure, ''))) = 'M'
              AND pn_comp.kgm IS NULL
        ),
        estado AS (
            SELECT
                p.id AS parte_id,
                pn_padre.kgm AS kgm_padre,
                (fm.parte_id IS NOT NULL) AS tiene_faltante_m
            FROM partes p
            LEFT JOIN peso_neto pn_padre
                ON pn_padre.numero_parte = p.numero_parte
            LEFT JOIN faltantes_m fm
                ON fm.parte_id = p.id
        )
        UPDATE partes p
        SET diferencia = CASE
            WHEN e.tiene_faltante_m THEN NULL
            WHEN e.kgm_padre IS NULL THEN NULL
            ELSE p.qty_total - e.kgm_padre
        END
        FROM estado e
        WHERE e.parte_id = p.id
    """))

    update_without_kgm = await db.execute(text("""
        UPDATE partes p
        SET diferencia = NULL
        WHERE NOT EXISTS (
            SELECT 1
            FROM peso_neto pn
            WHERE pn.numero_parte = p.numero_parte
              AND pn.kgm IS NOT NULL
        )
    """))

    await db.flush()
    return {
        "actualizados_con_kgm": int(update_with_kgm.rowcount or 0),
        "sin_kgm_en_null": int(update_without_kgm.rowcount or 0),
    }


async def upsert_parte(db: AsyncSession, numero_parte: str, descripcion: Optional[str] = None) -> Parte:
    """Inserta o actualiza una parte. Si existe, actualiza descripción si cambió. No hace commit (para usar en transacción)."""
    existing = await get_parte_by_numero(db, numero_parte)
    if existing:
        if descripcion is not None:
            existing.descripcion = descripcion
        await db.flush()
        return existing
    parte = Parte(numero_parte=numero_parte, descripcion=descripcion)
    db.add(parte)
    await db.flush()
    await db.refresh(parte)
    return parte


async def get_bom_by_part_plant_usage_alt(
    db: AsyncSession,
    parte_id: int,
    plant: str,
    usage: str,
    alternative: str,
) -> Optional[Bom]:
    """Obtiene un BOM por (parte_id, plant, usage, alternative)."""
    result = await db.execute(
        select(Bom).where(
            Bom.parte_id == parte_id,
            Bom.plant == plant,
            Bom.usage == usage,
            Bom.alternative == alternative,
        )
    )
    return result.scalar_one_or_none()


async def create_bom(
    db: AsyncSession,
    parte_id: int,
    plant: str,
    usage: str,
    alternative: str,
    base_qty: Optional[Decimal] = None,
    reqd_qty: Optional[Decimal] = None,
    base_unit: Optional[str] = None,
) -> Bom:
    """Crea un BOM."""
    bom = Bom(
        parte_id=parte_id,
        plant=plant,
        usage=usage,
        alternative=alternative,
        base_qty=base_qty,
        reqd_qty=reqd_qty,
        base_unit=base_unit,
    )
    db.add(bom)
    await db.flush()
    return bom


async def get_or_create_bom(
    db: AsyncSession,
    parte_id: int,
    plant: str,
    usage: str,
    alternative: str,
    base_qty: Optional[Decimal] = None,
    reqd_qty: Optional[Decimal] = None,
    base_unit: Optional[str] = None,
) -> Bom:
    """Obtiene el BOM por (parte_id, plant, usage, alternative) o lo crea."""
    existing = await get_bom_by_part_plant_usage_alt(db, parte_id, plant, usage, alternative)
    if existing:
        if base_qty is not None:
            existing.base_qty = base_qty
        if reqd_qty is not None:
            existing.reqd_qty = reqd_qty
        if base_unit is not None:
            existing.base_unit = base_unit
        await db.flush()
        return existing
    return await create_bom(db, parte_id, plant, usage, alternative, base_qty, reqd_qty, base_unit)


async def get_current_bom_revision(db: AsyncSession, bom_id: int) -> Optional[BomRevision]:
    """Obtiene la revisión vigente (effective_to IS NULL) de un BOM."""
    result = await db.execute(
        select(BomRevision).where(
            BomRevision.bom_id == bom_id,
            BomRevision.effective_to.is_(None),
        )
    )
    return result.scalar_one_or_none()


async def create_bom_revision(
    db: AsyncSession,
    bom_id: int,
    revision_no: int,
    effective_from: date,
    hash_value: str,
    source: Optional[str] = None,
) -> BomRevision:
    """Crea una nueva revisión de BOM."""
    rev = BomRevision(
        bom_id=bom_id,
        revision_no=revision_no,
        effective_from=effective_from,
        effective_to=None,
        source=source,
        hash=hash_value,
    )
    db.add(rev)
    await db.flush()
    return rev


async def close_bom_revision(db: AsyncSession, revision_id: int, effective_to: date) -> None:
    """Cierra una revisión asignando effective_to."""
    result = await db.execute(select(BomRevision).where(BomRevision.id == revision_id))
    rev = result.scalar_one_or_none()
    if rev:
        rev.effective_to = effective_to
        await db.flush()


async def insert_bom_items(
    db: AsyncSession,
    bom_revision_id: int,
    items: List[Dict[str, Any]],
) -> int:
    """Inserta items de una revisión. items: list of {componente_id, item_no, qty, measure, comm_code, origin}."""
    if not items:
        return 0
    for it in items:
        bi = BomItem(
            bom_revision_id=bom_revision_id,
            componente_id=it["componente_id"],
            item_no=it.get("item_no"),
            qty=it["qty"],
            measure=it.get("measure"),
            comm_code=it.get("comm_code"),
            origin=it.get("origin"),
        )
        db.add(bi)
    await db.flush()
    return len(items)


async def get_bom_vigente_by_parte(
    db: AsyncSession,
    parte_no: str,
    plant: str,
    usage: str,
    alternative: str,
):
    """
    Obtiene el BOM vigente (revisión con effective_to IS NULL) de una parte.
    Retorna (Bom, BomRevision, list[BomItem]) o (None, None, []) si no existe.
    """
    parte = await get_parte_by_numero(db, parte_no)
    if not parte:
        return None, None, []
    bom = await get_bom_by_part_plant_usage_alt(db, parte.id, plant, usage, alternative)
    if not bom:
        return None, None, []
    rev = await get_current_bom_revision(db, bom.id)
    if not rev:
        return bom, None, []
    result = await db.execute(
        select(BomItem).where(BomItem.bom_revision_id == rev.id).order_by(BomItem.item_no, BomItem.id)
    )
    items = list(result.scalars().all())
    return bom, rev, items


# Solo USA, Mexico y Canada se consideran "originating". Origen desde tabla pais_origen_material.
# Cualquier otro valor (Turkey, China, etc.) es non-originating.


def _is_originating_pais(pais: Optional[str]) -> bool:
    """
    True solo si pais es USA, Mexico o Canada (según valores en pais_origen_material).
    Cualquier otro país o valor es non-originating.
    """
    if not pais:
        return False
    p = (pais.strip() or "").lower()
    if not p:
        return False
    # USA: códigos y nombres habituales
    if p in ("usa", "us", "u.s.a.", "u.s.", "united states", "estados unidos") or "united states" in p or "estados unidos" in p:
        return True
    # Mexico: códigos y nombres habituales
    if p in ("mexico", "méxico", "mex", "mx"):
        return True
    if "mexico" in p or "méxico" in p:
        return True
    # Canada: solo Canada/Canadá (evitar que "ca" suelto coincida con otros países)
    if p in ("canada", "canadá", "ca"):
        return True
    if p.startswith("canada") or p.startswith("canadá"):
        return True
    return False


async def get_bom_items_for_parte(
    db: AsyncSession,
    numero_parte: str,
) -> Dict[str, Any]:
    """
    Obtiene los ítems del BOM de un número de parte (primer BOM vigente encontrado).
    Retorna dict con: parte (info), items_originating, items_non_originating.
    El país de origen viene de la tabla pais_origen_material (por numero_material + codigo_proveedor).
    Solo USA, Mexico y Canada se consideran originating; todo lo demás es non-originating.
    """
    parte = await get_parte_by_numero(db, numero_parte)
    if not parte:
        return {"parte": None, "items_originating": [], "items_non_originating": []}

    parte_info = {
        "numero_parte": parte.numero_parte,
        "descripcion": parte.descripcion,
        "fraccion": parte.fraccion,
    }
    rows: List[Any] = []
    bom = None
    rev = None
    alguna_compra_antigua = False

    # Cualquier BOM de esta parte (primer registro). Si no hay BOM, se trata como ítem único (número de materia).
    result_bom = await db.execute(
        select(Bom).where(Bom.parte_id == parte.id).limit(1)
    )
    bom = result_bom.scalar_one_or_none()
    if bom:
        rev = await get_current_bom_revision(db, bom.id)
        if rev:
            result_items = await db.execute(
                select(BomItem, Parte)
                .join(Parte, BomItem.componente_id == Parte.id)
                .where(BomItem.bom_revision_id == rev.id)
                .order_by(BomItem.item_no, BomItem.id)
            )
            rows = result_items.all()

    # Si no hay ítems de BOM (material sin BOM o sin revisión), usamos el número de materia como único ítem.
    numeros_parte = (
        list({str((comp.numero_parte or "").strip()) for _, comp in rows if (comp.numero_parte or "").strip()})
        if rows
        else ([str(parte.numero_parte).strip()] if (parte.numero_parte and str(parte.numero_parte).strip()) else [])
    )

    # Proveedores, porcentaje de compra, país de origen y comentario desde pais_origen_material (solo con % > 0).
    # PU/Kg (precio_compra) = amount_in_lc / quantity_in_opun desde el último registro de compras por (numero_material, codigo_proveedor).
    proveedores_by_material: Dict[str, List[Dict[str, Any]]] = {}
    if numeros_parte:
        result_pais = await db.execute(
            select(
                PaisOrigenMaterial.numero_material,
                PaisOrigenMaterial.codigo_proveedor,
                PaisOrigenMaterial.pais_origen,
                PaisOrigenMaterial.porcentaje_compra,
                PaisOrigenMaterial.comentario,
                PaisOrigenMaterial.tipo,
            )
            .where(
                PaisOrigenMaterial.numero_material.in_(numeros_parte),
                PaisOrigenMaterial.porcentaje_compra.isnot(None),
                PaisOrigenMaterial.porcentaje_compra > 0,
            )
            .order_by(PaisOrigenMaterial.numero_material, PaisOrigenMaterial.codigo_proveedor)
        )
        pais_rows = result_pais.all()
        # Datos por (material, proveedor): pais_origen, porcentaje_compra y comentario desde pais_origen_material
        pais_data_by_key: Dict[tuple, Dict[str, Any]] = {}
        for r in pais_rows:
            num = str((r[0] or "").strip())
            cod = int(r[1]) if r[1] is not None else None
            if not num or cod is None:
                continue
            pct = float(r[3]) if r[3] is not None else None
            if pct is None or pct == 0:
                continue
            key = (num, cod)
            pais_data_by_key[key] = {
                "pais_origen": (r[2] or "").strip() if r[2] is not None else "",
                "porcentaje_compra": pct,
                "comentario": (r[4] or "").strip() if r[4] else None,
                "tipo": (r[5] or "").strip() if r[5] else None,
            }
        # Último PU/Kg por (numero_material, codigo_proveedor): PU/Kg = amount_in_lc / quantity_in_opun (desde compras).
        # Si la última compra es anterior a 2 años calendario al actual, no usar precio (0) y marcar para ICR 0%.
        año_actual_bom = datetime.now().year
        limite_fecha_compra = date(año_actual_bom - 2, 1, 1)  # compras antes de esta fecha se consideran obsoletas
        alguna_compra_antigua = False
        ultimo_precio_by_key: Dict[tuple, Dict[str, Any]] = {}
        if pais_data_by_key:
            keys_list = list(pais_data_by_key.keys())
            numeros_set = list({k[0] for k in keys_list})
            codigos_set = list({k[1] for k in keys_list})
            result_compras = await db.execute(
                select(
                    Compra.numero_material,
                    Compra.codigo_proveedor,
                    Compra.amount_in_lc,
                    Compra.quantity_in_opun,
                    Compra.currency,
                    Compra.posting_date,
                )
                .where(
                    Compra.numero_material.in_(numeros_set),
                    Compra.codigo_proveedor.in_(codigos_set),
                    Compra.amount_in_lc.isnot(None),
                    Compra.quantity_in_opun.isnot(None),
                    Compra.quantity_in_opun > 0,
                )
                .order_by(
                    Compra.numero_material,
                    Compra.codigo_proveedor,
                    desc(Compra.posting_date).nulls_last(),
                    desc(Compra.id),
                )
            )
            for row in result_compras.all():
                num = str((row[0] or "").strip())
                cod_prov = int(row[1]) if row[1] is not None else None
                if not num or cod_prov is None:
                    continue
                key = (num, cod_prov)
                if key not in pais_data_by_key or key in ultimo_precio_by_key:
                    continue
                amt = row[2]
                qty = row[3]
                posting_date_val = row[5]
                pd_date = posting_date_val.date() if hasattr(posting_date_val, "date") and posting_date_val else None
                compra_antigua = pd_date is None or pd_date < limite_fecha_compra
                if compra_antigua:
                    alguna_compra_antigua = True
                pu_kg = None
                if not compra_antigua and amt is not None and qty is not None and int(qty) != 0:
                    try:
                        pu_kg = float(Decimal(str(amt)) / int(qty))
                    except (ZeroDivisionError, ValueError, InvalidOperation):
                        pass
                ultimo_precio_by_key[key] = {
                    "price": pu_kg,
                    "currency": (row[4] or "").strip() if row[4] else None,
                    "old_purchase": compra_antigua,
                }
        # Nombres de proveedor
        codigos_proveedor = list({k[1] for k in pais_data_by_key.keys()})
        proveedor_nombres: Dict[int, str] = {}
        if codigos_proveedor:
            result_prov = await db.execute(
                select(Proveedor.codigo_proveedor, Proveedor.nombre).where(
                    Proveedor.codigo_proveedor.in_(codigos_proveedor)
                )
            )
            for row in result_prov.all():
                if row[0] is not None:
                    proveedor_nombres[int(row[0])] = (row[1] or "").strip() or str(row[0])
        # Armar lista de proveedores por material (pais_origen_material + último precio de compras)
        if pais_data_by_key:
            for (num, cod), data in pais_data_by_key.items():
                ultimo = ultimo_precio_by_key.get((num, cod)) or {}
                # Si la última compra es anterior a 2 años, no poner precio de compra (0)
                precio_final = 0 if ultimo.get("old_purchase") else ultimo.get("price")
                prov_entry = {
                    "nombre_proveedor": proveedor_nombres.get(cod) or str(cod),
                    "codigo_proveedor": cod,
                    "pais_origen": data["pais_origen"],
                    "porcentaje_compra": data["porcentaje_compra"],
                    "precio_compra": precio_final,
                    "currency_uom": ultimo.get("currency"),
                    "comentario": data.get("comentario"),
                    "tipo": data.get("tipo"),
                }
                if num not in proveedores_by_material:
                    proveedores_by_material[num] = []
                proveedores_by_material[num].append(prov_entry)

    if numeros_parte and proveedores_by_material:

        # Para el número de parte "310004003", PU/Kg = precio del material + último PU/Kg de "310004000" del mismo proveedor (PU/Kg = amount_in_lc / quantity_in_opun)
        if "310004003" in proveedores_by_material:
            proveedores_310004003 = list({int(p["codigo_proveedor"]) for p in proveedores_by_material["310004003"] if p.get("codigo_proveedor") is not None})
            precio_310004000_by_proveedor: Dict[int, float] = {}
            if proveedores_310004003:
                result_310004000 = await db.execute(
                    select(Compra.codigo_proveedor, Compra.amount_in_lc, Compra.quantity_in_opun, Compra.posting_date)
                    .where(
                        Compra.numero_material == "310004000",
                        Compra.codigo_proveedor.in_(proveedores_310004003),
                        Compra.amount_in_lc.isnot(None),
                        Compra.quantity_in_opun.isnot(None),
                        Compra.quantity_in_opun > 0,
                    )
                    .order_by(Compra.codigo_proveedor, desc(Compra.posting_date).nulls_last(), desc(Compra.id))
                )
                for row in result_310004000.all():
                    if row[0] is None or int(row[0]) in precio_310004000_by_proveedor:
                        continue
                    amt, qty = row[1], row[2]
                    pd = row[3]
                    pd_date = pd.date() if hasattr(pd, "date") and pd else None
                    if pd_date is None or pd_date < limite_fecha_compra:
                        alguna_compra_antigua = True
                        precio_310004000_by_proveedor[int(row[0])] = 0
                        continue
                    if amt is not None and qty is not None and int(qty) != 0:
                        try:
                            precio_310004000_by_proveedor[int(row[0])] = float(Decimal(str(amt)) / int(qty))
                        except (ZeroDivisionError, ValueError, InvalidOperation):
                            pass
            for p in proveedores_by_material["310004003"]:
                cod = int(p["codigo_proveedor"]) if p.get("codigo_proveedor") is not None else None
                extra = precio_310004000_by_proveedor.get(cod, 0) if cod is not None else 0
                base = p.get("precio_compra")
                p["precio_compra"] = (float(base) if base is not None else 0) + extra

        # Materiales con comentario "MAS COBRE": si no tienen precio para ese proveedor, usar último PU/Kg de 310004000 proveedor 521348 (PU/Kg = amount_in_lc / quantity_in_opun)
        precio_mas_cobre_fallback: Optional[float] = None
        if any(
            (p.get("comentario") or "").strip().upper() == "MAS COBRE"
            for prov_list in proveedores_by_material.values()
            for p in prov_list
        ):
            res_mas_cobre = await db.execute(
                select(Compra.amount_in_lc, Compra.quantity_in_opun, Compra.posting_date).where(
                    Compra.numero_material == "310004000",
                    Compra.codigo_proveedor == 521348,
                    Compra.amount_in_lc.isnot(None),
                    Compra.quantity_in_opun.isnot(None),
                    Compra.quantity_in_opun > 0,
                ).order_by(desc(Compra.posting_date).nulls_last(), desc(Compra.id)).limit(1)
            )
            p_row = res_mas_cobre.fetchone()
            if p_row and p_row[0] is not None and p_row[1] is not None and int(p_row[1]) != 0:
                pd = p_row[2] if len(p_row) > 2 else None
                pd_date = pd.date() if hasattr(pd, "date") and pd else None
                if pd_date is not None and pd_date >= limite_fecha_compra:
                    try:
                        precio_mas_cobre_fallback = float(Decimal(str(p_row[0])) / int(p_row[1]))
                    except (ZeroDivisionError, ValueError, InvalidOperation):
                        pass
                else:
                    alguna_compra_antigua = True
        if precio_mas_cobre_fallback is not None:
            for prov_list in proveedores_by_material.values():
                for p in prov_list:
                    if (p.get("comentario") or "").strip().upper() == "MAS COBRE" and (
                        p.get("precio_compra") is None or (isinstance(p.get("precio_compra"), (int, float)) and p.get("precio_compra") == 0)
                    ):
                        p["precio_compra"] = precio_mas_cobre_fallback

        # Para el material "310905000" usar última compra con PU/Kg = amount_in_lc / quantity_in_opun > 10
        if "310905000" in proveedores_by_material:
            ultima_compra_310905000 = await db.execute(
                select(Compra.amount_in_lc, Compra.quantity_in_opun, Compra.posting_date).where(
                    Compra.numero_material == "310905000",
                    Compra.amount_in_lc.isnot(None),
                    Compra.quantity_in_opun.isnot(None),
                    Compra.quantity_in_opun > 0,
                ).order_by(desc(Compra.posting_date).nulls_last(), desc(Compra.id))
            )
            precio_310905000: Optional[float] = None
            for row in ultima_compra_310905000.all():
                amt, qty = row[0], row[1]
                pd = row[2] if len(row) > 2 else None
                pd_date = pd.date() if hasattr(pd, "date") and pd else None
                if amt is not None and qty is not None and int(qty) != 0:
                    try:
                        pu = float(Decimal(str(amt)) / int(qty))
                        if pu > 10:
                            if pd_date is None or pd_date < limite_fecha_compra:
                                alguna_compra_antigua = True
                                break  # última compra con pu>10 es antigua, no usar precio
                            precio_310905000 = pu
                            break
                    except (ZeroDivisionError, ValueError, InvalidOperation):
                        pass
            if precio_310905000 is not None:
                for p in proveedores_by_material["310905000"]:
                    p["precio_compra"] = precio_310905000
            elif "310905000" in proveedores_by_material:
                for p in proveedores_by_material["310905000"]:
                    p["precio_compra"] = 0

    originating = []
    non_originating = []
    items_bom = []  # Una fila por material con TODOS los proveedores (para tabla Ítems del BOM)
    for bi, comp in rows:
        numero_comp = str((comp.numero_parte or "").strip())
        proveedores_raw = (proveedores_by_material.get(numero_comp) or []) if numero_comp else []
        # Solo incluir proveedores con porcentaje de compra válido (> 0); excluir null/0
        proveedores = [p for p in proveedores_raw if (p.get("porcentaje_compra") or 0) > 0]
        name = (comp.numero_parte or "") + (" " + (comp.descripcion or "") if comp.descripcion else "")
        name = name.strip() or "—"
        qty_val = float(bi.qty) if bi.qty is not None else None
        # Si la medida es M (metros), la cantidad se divide entre 1000
        if qty_val is not None and (str(bi.measure or "").strip().upper() == "M"):
            qty_val = qty_val / 1000
        # Clasificar por cada proveedor: USA/México/Canadá -> Originating; resto (p. ej. TURQUIA) -> Non-Originating
        prov_orig = [p for p in proveedores if _is_originating_pais((p.get("pais_origen") or "").strip())]
        prov_non_orig = [p for p in proveedores if not _is_originating_pais((p.get("pais_origen") or "").strip())]
        if proveedores:
            origin_display = (proveedores[0].get("pais_origen") or "").strip() or "—"
        else:
            origin_display = (bi.origin or "").strip() or "—"
        if prov_orig:
            originating.append({
                "name": name,
                "numero_parte": comp.numero_parte,
                "descripcion": comp.descripcion,
                "qty": qty_val,
                "measure": bi.measure,
                "comm_code": bi.comm_code,
                "origin": origin_display,
                "proveedores": prov_orig,
            })
        if prov_non_orig:
            non_originating.append({
                "name": name,
                "numero_parte": comp.numero_parte,
                "descripcion": comp.descripcion,
                "qty": qty_val,
                "measure": bi.measure,
                "comm_code": bi.comm_code,
                "origin": origin_display,
                "proveedores": prov_non_orig,
            })
        # Una sola fila en Ítems del BOM con todos los proveedores; tipo Mixto si hay de ambos orígenes
        if proveedores:
            tipo_bom = "Mixto" if (prov_orig and prov_non_orig) else ("Originating" if prov_orig else "Non-Originating")
            items_bom.append({
                "name": name,
                "numero_parte": comp.numero_parte,
                "descripcion": comp.descripcion,
                "qty": qty_val,
                "measure": bi.measure,
                "comm_code": bi.comm_code,
                "origin": origin_display,
                "proveedores": proveedores,
                "tipo": tipo_bom,
            })
        else:
            # Sin proveedores: usar bi.origin para clasificar
            is_originating = _is_originating_pais(bi.origin)
            item_dict = {
                "name": name,
                "numero_parte": comp.numero_parte,
                "descripcion": comp.descripcion,
                "qty": qty_val,
                "measure": bi.measure,
                "comm_code": bi.comm_code,
                "origin": origin_display,
                "proveedores": [],
            }
            if is_originating:
                originating.append(item_dict)
            else:
                non_originating.append(item_dict)
            items_bom.append(dict(item_dict, tipo="Originating" if is_originating else "Non-Originating"))

    # Si no hay filas de BOM pero sí número de materia (ítem único), construir ítem(es) con proveedores y origen.
    if not rows and numeros_parte:
        numero_comp = str((parte.numero_parte or "").strip())
        proveedores_raw = (proveedores_by_material.get(numero_comp) or []) if numero_comp else []
        proveedores = [p for p in proveedores_raw if (p.get("porcentaje_compra") or 0) > 0]
        name = (parte.numero_parte or "") + (" " + (parte.descripcion or "") if parte.descripcion else "")
        name = name.strip() or "—"
        origin_display = (proveedores[0].get("pais_origen") or "").strip() or "—" if proveedores else "—"
        prov_orig = [p for p in proveedores if _is_originating_pais((p.get("pais_origen") or "").strip())]
        prov_non_orig = [p for p in proveedores if not _is_originating_pais((p.get("pais_origen") or "").strip())]
        if prov_orig:
            originating.append({
                "name": name,
                "numero_parte": parte.numero_parte,
                "descripcion": parte.descripcion,
                "qty": None,
                "measure": None,
                "comm_code": None,
                "origin": origin_display,
                "proveedores": prov_orig,
            })
        if prov_non_orig:
            non_originating.append({
                "name": name,
                "numero_parte": parte.numero_parte,
                "descripcion": parte.descripcion,
                "qty": None,
                "measure": None,
                "comm_code": None,
                "origin": origin_display,
                "proveedores": prov_non_orig,
            })
        if proveedores:
            tipo_bom = "Mixto" if (prov_orig and prov_non_orig) else ("Originating" if prov_orig else "Non-Originating")
            items_bom.append({
                "name": name,
                "numero_parte": parte.numero_parte,
                "descripcion": parte.descripcion,
                "qty": None,
                "measure": None,
                "comm_code": None,
                "origin": origin_display,
                "proveedores": proveedores,
                "tipo": tipo_bom,
            })
        if not proveedores:
            item_dict = {
                "name": name,
                "numero_parte": parte.numero_parte,
                "descripcion": parte.descripcion,
                "qty": None,
                "measure": None,
                "comm_code": None,
                "origin": origin_display,
                "proveedores": [],
            }
            non_originating.append(item_dict)
            items_bom.append(dict(item_dict, tipo="Non-Originating"))

    return {
        "parte": parte_info,
        "items_bom": items_bom,
        "items_originating": originating,
        "items_non_originating": non_originating,
        "alguna_compra_antigua": alguna_compra_antigua,
    }


async def list_bom_revisiones(db: AsyncSession, bom_id: int, limit: int = 100) -> List[BomRevision]:
    """Lista el historial de revisiones de un BOM (por bom_id)."""
    result = await db.execute(
        select(BomRevision).where(BomRevision.bom_id == bom_id).order_by(desc(BomRevision.revision_no)).limit(limit)
    )
    return list(result.scalars().all())


async def list_boms_where_componente(
    db: AsyncSession,
    componente_no: str,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Lista los BOMs en los que aparece un componente (por número de parte).
    Retorna lista de dict con bom_id, parte_no (padre), plant, usage, alternative, revision_no, vigente.
    """
    parte = await get_parte_by_numero(db, componente_no)
    if not parte:
        return []
    # Items que usan este componente; join a revision y bom y parte (padre)
    result = await db.execute(
        select(BomItem, BomRevision, Bom, Parte)
        .join(BomRevision, BomItem.bom_revision_id == BomRevision.id)
        .join(Bom, BomRevision.bom_id == Bom.id)
        .join(Parte, Bom.parte_id == Parte.id)
        .where(BomItem.componente_id == parte.id)
        .order_by(Bom.id, desc(BomRevision.revision_no))
        .limit(limit)
    )
    rows = result.all()
    seen = set()
    out = []
    for item, rev, bom, parte_padre in rows:
        key = (bom.id, rev.id)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "bom_id": bom.id,
            "parte_no": parte_padre.numero_parte,
            "plant": bom.plant,
            "usage": bom.usage,
            "alternative": bom.alternative,
            "revision_no": rev.revision_no,
            "vigente": rev.effective_to is None,
        })
    return out


async def get_bom_revision_with_items(db: AsyncSession, revision_id: int):
    """Obtiene una revisión y sus items (para comparar dos revisiones)."""
    result = await db.execute(
        select(BomRevision).where(BomRevision.id == revision_id)
    )
    rev = result.scalar_one_or_none()
    if not rev:
        return None, []
    result2 = await db.execute(
        select(BomItem).where(BomItem.bom_revision_id == revision_id).order_by(BomItem.item_no, BomItem.id)
    )
    items = list(result2.scalars().all())
    return rev, items


# ==================== CRUD para BomFlat ====================

async def get_bom_flat_by_id(db: AsyncSession, bom_id: int) -> Optional[BomFlat]:
    """Obtiene un registro BOM por ID."""
    result = await db.execute(
        select(BomFlat)
        .options(selectinload(BomFlat.fg_part), selectinload(BomFlat.material_part))
        .where(BomFlat.id == bom_id)
    )
    return result.scalar_one_or_none()


async def create_bom_flat(
    db: AsyncSession,
    fg_part_no: str,
    plant_code: str,
    material: str,
    qty: Decimal,
    uom: str,
    base_mts: Optional[Decimal] = None,
    req_d: Optional[Decimal] = None,
    material_description: Optional[str] = None,
    origin_country: Optional[str] = None,
    sale_price: Optional[Decimal] = None,
    run_id: Optional[int] = None
) -> BomFlat:
    """Crea un nuevo registro BOM."""
    bom = BomFlat(
        fg_part_no=fg_part_no,
        plant_code=plant_code,
        material=material,
        material_description=material_description,
        qty=qty,
        uom=uom,
        base_mts=base_mts,
        req_d=req_d,
        origin_country=origin_country,
        sale_price=sale_price,
        run_id=run_id
    )
    db.add(bom)
    await db.commit()
    await db.refresh(bom)
    return bom


async def bulk_insert_bom_flat(
    db: AsyncSession,
    bom_data: List[Dict[str, Any]]
) -> int:
    """Inserta múltiples registros BOM en batch."""
    if not bom_data:
        return 0
    
    stmt = insert(BomFlat).values(bom_data)
    await db.execute(stmt)
    await db.commit()
    return len(bom_data)


async def list_bom_flat(
    db: AsyncSession,
    fg_part_no: Optional[str] = None,
    material: Optional[str] = None,
    plant_code: Optional[str] = None,
    search: Optional[str] = None,
    run_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0
) -> List[BomFlat]:
    """Lista registros BOM con filtros opcionales."""
    query = select(BomFlat).options(
        selectinload(BomFlat.fg_part),
        selectinload(BomFlat.material_part)
    )
    
    if fg_part_no:
        query = query.where(BomFlat.fg_part_no == fg_part_no)
    
    if material:
        query = query.where(BomFlat.material == material)
    
    if plant_code:
        query = query.where(BomFlat.plant_code == plant_code)
    
    if run_id is not None:
        query = query.where(BomFlat.run_id == run_id)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                BomFlat.fg_part_no.ilike(search_pattern),
                BomFlat.material.ilike(search_pattern),
                BomFlat.material_description.ilike(search_pattern)
            )
        )
    
    query = query.order_by(BomFlat.fg_part_no, BomFlat.material).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_bom_flat(
    db: AsyncSession,
    fg_part_no: Optional[str] = None,
    material: Optional[str] = None,
    plant_code: Optional[str] = None,
    search: Optional[str] = None,
    run_id: Optional[int] = None
) -> int:
    """Cuenta el total de registros BOM con filtros opcionales."""
    query = select(func.count(BomFlat.id))
    
    if fg_part_no:
        query = query.where(BomFlat.fg_part_no == fg_part_no)
    
    if material:
        query = query.where(BomFlat.material == material)
    
    if plant_code:
        query = query.where(BomFlat.plant_code == plant_code)
    
    if run_id is not None:
        query = query.where(BomFlat.run_id == run_id)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                BomFlat.fg_part_no.ilike(search_pattern),
                BomFlat.material.ilike(search_pattern),
                BomFlat.material_description.ilike(search_pattern)
            )
        )
    
    result = await db.execute(query)
    return result.scalar_one()


async def get_bom_for_fg(
    db: AsyncSession,
    fg_part_no: str,
    plant_code: Optional[str] = None
) -> List[BomFlat]:
    """Obtiene todos los materiales de un producto terminado."""
    query = select(BomFlat).options(
        selectinload(BomFlat.material_part)
    ).where(BomFlat.fg_part_no == fg_part_no)
    
    if plant_code:
        query = query.where(BomFlat.plant_code == plant_code)
    
    query = query.order_by(BomFlat.material)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def delete_bom_flat_by_run_id(db: AsyncSession, run_id: int) -> int:
    """Elimina todos los registros BOM de un run_id específico."""
    from sqlalchemy import delete
    
    stmt = delete(BomFlat).where(BomFlat.run_id == run_id)
    result = await db.execute(stmt)
    await db.commit()
    return result.rowcount


async def delete_all_bom_flat(db: AsyncSession) -> int:
    """Elimina todos los registros BOM (usar con precaución)."""
    from sqlalchemy import delete
    
    stmt = delete(BomFlat)
    result = await db.execute(stmt)
    await db.commit()
    return result.rowcount


# ==================== CRUD para Proveedores ====================

async def get_proveedor_by_codigo_proveedor(db: AsyncSession, codigo_proveedor) -> Optional[Proveedor]:
    """Obtiene un proveedor por código de proveedor."""
    result = await db.execute(select(Proveedor).where(Proveedor.codigo_proveedor == int(codigo_proveedor)))
    return result.scalar_one_or_none()


def _proveedor_to_dict(proveedor: Proveedor) -> Dict[str, Any]:
    """Convierte un objeto Proveedor a diccionario para el historial."""
    return {
        "codigo_proveedor": proveedor.codigo_proveedor,
        "nombre": proveedor.nombre,
        "pais": proveedor.pais,
        "domicilio": proveedor.domicilio,
        "poblacion": proveedor.poblacion,
        "cp": proveedor.cp,
        "estatus": proveedor.estatus,
        "estatus_compras": proveedor.estatus_compras,
        "created_at": proveedor.created_at.isoformat() if proveedor.created_at else None,
        "updated_at": proveedor.updated_at.isoformat() if proveedor.updated_at else None
    }


async def create_proveedor(
    db: AsyncSession,
    codigo_proveedor: str,
    nombre: str,
    pais: Optional[str] = None,
    domicilio: Optional[str] = None,
    poblacion: Optional[str] = None,
    cp: Optional[str] = None,
    estatus: bool = True,
    estatus_compras: Optional[str] = None,
    user_id: Optional[int] = None
) -> Proveedor:
    """Crea un nuevo proveedor."""
    proveedor = Proveedor(
        codigo_proveedor=codigo_proveedor,
        nombre=nombre,
        pais=pais,
        domicilio=domicilio,
        poblacion=poblacion,
        cp=cp,
        estatus=estatus,
        estatus_compras=estatus_compras
    )
    db.add(proveedor)
    await db.commit()
    await db.refresh(proveedor)
    
    # Registrar en historial
    if user_id is not None:
        historial = ProveedorHistorial(
            codigo_proveedor=codigo_proveedor,
            operacion=ProveedorOperacion.CREATE,
            user_id=user_id,
            datos_antes=None,
            datos_despues=_proveedor_to_dict(proveedor),
            campos_modificados=None
        )
        db.add(historial)
        await db.commit()
    
    return proveedor


async def update_proveedor(
    db: AsyncSession,
    codigo_proveedor: str,
    nombre: Optional[str] = None,
    pais: Optional[str] = None,
    domicilio: Optional[str] = None,
    poblacion: Optional[str] = None,
    cp: Optional[str] = None,
    estatus: Optional[bool] = None,
    estatus_compras: Optional[str] = None,
    user_id: Optional[int] = None
) -> Optional[Proveedor]:
    """Actualiza un proveedor."""
    proveedor = await get_proveedor_by_codigo_proveedor(db, codigo_proveedor)
    if not proveedor:
        return None
    
    # Guardar datos antes del cambio
    datos_antes = _proveedor_to_dict(proveedor)
    campos_modificados = []
    
    if nombre is not None and proveedor.nombre != nombre:
        proveedor.nombre = nombre
        campos_modificados.append("nombre")
    if pais is not None and proveedor.pais != pais:
        proveedor.pais = pais
        campos_modificados.append("pais")
    if domicilio is not None and proveedor.domicilio != domicilio:
        proveedor.domicilio = domicilio
        campos_modificados.append("domicilio")
    if poblacion is not None and proveedor.poblacion != poblacion:
        proveedor.poblacion = poblacion
        campos_modificados.append("poblacion")
    if cp is not None and proveedor.cp != cp:
        proveedor.cp = cp
        campos_modificados.append("cp")
    if estatus is not None and proveedor.estatus != estatus:
        proveedor.estatus = estatus
        campos_modificados.append("estatus")
    if estatus_compras is not None and proveedor.estatus_compras != estatus_compras:
        proveedor.estatus_compras = estatus_compras
        campos_modificados.append("estatus_compras")
    
    # Solo registrar en historial si hubo cambios
    if campos_modificados and user_id is not None:
        await db.flush()  # Para obtener los datos actualizados
        await db.refresh(proveedor)
        datos_despues = _proveedor_to_dict(proveedor)
        
        historial = ProveedorHistorial(
            codigo_proveedor=codigo_proveedor,
            operacion=ProveedorOperacion.UPDATE,
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=datos_despues,
            campos_modificados=campos_modificados
        )
        db.add(historial)
    
    await db.commit()
    await db.refresh(proveedor)
    return proveedor


async def delete_proveedor(
    db: AsyncSession, 
    codigo_proveedor: str,
    user_id: Optional[int] = None
) -> bool:
    """Elimina un proveedor."""
    from sqlalchemy import delete
    
    proveedor = await get_proveedor_by_codigo_proveedor(db, codigo_proveedor)
    if not proveedor:
        return False
    
    # Guardar datos antes de eliminar
    datos_antes = _proveedor_to_dict(proveedor)
    
    stmt = delete(Proveedor).where(Proveedor.codigo_proveedor == int(codigo_proveedor))
    await db.execute(stmt)
    
    # Registrar en historial
    if user_id is not None:
        historial = ProveedorHistorial(
            codigo_proveedor=codigo_proveedor,
            operacion=ProveedorOperacion.DELETE,
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=None,
            campos_modificados=None
        )
        db.add(historial)
    
    await db.commit()
    return True


async def list_proveedores(
    db: AsyncSession,
    estatus: Optional[bool] = None,
    pais: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Proveedor]:
    """Lista proveedores con filtros opcionales."""
    query = select(Proveedor)
    
    if estatus is not None:
        query = query.where(Proveedor.estatus == estatus)
    
    if pais:
        query = query.where(Proveedor.pais == pais)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Proveedor.nombre.ilike(search_pattern),
                Proveedor.codigo_proveedor.cast(String).ilike(search_pattern),
                Proveedor.domicilio.ilike(search_pattern)
            )
        )
    
    query = query.order_by(desc(Proveedor.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_proveedores(
    db: AsyncSession,
    estatus: Optional[bool] = None,
    pais: Optional[str] = None,
    search: Optional[str] = None
) -> int:
    """Cuenta el total de proveedores con filtros opcionales."""
    query = select(func.count(Proveedor.codigo_proveedor))
    
    if estatus is not None:
        query = query.where(Proveedor.estatus == estatus)
    
    if pais:
        query = query.where(Proveedor.pais == pais)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Proveedor.nombre.ilike(search_pattern),
                Proveedor.codigo_proveedor.cast(String).ilike(search_pattern),
                Proveedor.domicilio.ilike(search_pattern)
            )
        )
    
    result = await db.execute(query)
    return result.scalar() or 0


# ==================== CRUD para Historial de Proveedores ====================

async def list_proveedor_historial(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    operacion: Optional[ProveedorOperacion] = None,
    user_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0
) -> List[ProveedorHistorial]:
    """Lista el historial de cambios en proveedores con filtros opcionales."""
    query = select(ProveedorHistorial).options(selectinload(ProveedorHistorial.user))
    
    if codigo_proveedor:
        query = query.where(ProveedorHistorial.codigo_proveedor == int(codigo_proveedor))
    
    if operacion:
        query = query.where(ProveedorHistorial.operacion == operacion)
    
    if user_id:
        query = query.where(ProveedorHistorial.user_id == user_id)
    
    query = query.order_by(desc(ProveedorHistorial.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_proveedor_historial_by_id(
    db: AsyncSession,
    historial_id: int
) -> Optional[ProveedorHistorial]:
    """Obtiene un registro del historial por ID."""
    result = await db.execute(
        select(ProveedorHistorial).where(ProveedorHistorial.id == historial_id)
    )
    return result.scalar_one_or_none()


async def count_proveedor_historial(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    operacion: Optional[ProveedorOperacion] = None,
    user_id: Optional[int] = None
) -> int:
    """Cuenta el total de registros en el historial con filtros opcionales."""
    query = select(func.count(ProveedorHistorial.id))
    
    if codigo_proveedor:
        query = query.where(ProveedorHistorial.codigo_proveedor == int(codigo_proveedor))
    
    if operacion:
        query = query.where(ProveedorHistorial.operacion == operacion)
    
    if user_id:
        query = query.where(ProveedorHistorial.user_id == user_id)
    
    result = await db.execute(query)
    return result.scalar() or 0


async def sincronizar_proveedores_desde_compras(
    db: AsyncSession,
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Sincroniza proveedores desde la tabla compras.
    Busca todos los codigo_proveedor únicos en compras que no estén registrados
    en proveedores y los crea usando el nombre_proveedor de compras.
    
    Returns:
        Dict con estadísticas de la sincronización:
        - total_encontrados: Total de códigos únicos en compras
        - nuevos_creados: Cantidad de proveedores nuevos creados
        - errores: Lista de errores encontrados
    """
    from sqlalchemy import distinct
    
    errores = []
    nuevos_creados = 0
    
    try:
        # 1. Obtener todos los codigo_proveedor únicos de compras que no sean NULL
        # Usamos GROUP BY para obtener el nombre_proveedor más común (o el primero no nulo)
        query_codigos = select(
            Compra.codigo_proveedor,
            func.max(Compra.nombre_proveedor).label('nombre_proveedor')
        ).where(
            Compra.codigo_proveedor.isnot(None)
        ).group_by(Compra.codigo_proveedor)
        
        result = await db.execute(query_codigos)
        proveedores_en_compras = result.all()
        
        total_encontrados = len(proveedores_en_compras)
        
        # 2. Obtener todos los códigos de proveedores existentes para comparación rápida
        query_existentes = select(Proveedor.codigo_proveedor)
        result_existentes = await db.execute(query_existentes)
        codigos_existentes = {row[0] for row in result_existentes.all() if row[0]}
        
        # 3. Para cada codigo_proveedor, verificar si existe en proveedores
        for codigo_proveedor, nombre_proveedor in proveedores_en_compras:
            if codigo_proveedor is None or str(codigo_proveedor).strip() == '':
                continue
            
            # Verificar si el proveedor ya existe (usando el set para mejor rendimiento)
            if codigo_proveedor not in codigos_existentes:
                # Crear el proveedor con el nombre de compras
                # Si nombre_proveedor es None o vacío, usar el código como nombre
                nombre = nombre_proveedor if nombre_proveedor and nombre_proveedor.strip() else codigo_proveedor
                
                try:
                    await create_proveedor(
                        db=db,
                        codigo_proveedor=codigo_proveedor,
                        nombre=nombre,
                        pais=None,
                        domicilio=None,
                        poblacion=None,
                        cp=None,
                        estatus=True,
                        estatus_compras=None,
                        user_id=user_id
                    )
                    nuevos_creados += 1
                    # Agregar a la lista de existentes para evitar duplicados en la misma ejecución
                    codigos_existentes.add(codigo_proveedor)
                except Exception as e:
                    error_msg = f"Error al crear proveedor {codigo_proveedor}: {str(e)}"
                    errores.append(error_msg)
                    # Continuar con el siguiente proveedor
                    continue
        
        return {
            "total_encontrados": total_encontrados,
            "nuevos_creados": nuevos_creados,
            "errores": errores,
            "exitoso": len(errores) == 0
        }
        
    except Exception as e:
        error_msg = f"Error general en sincronización: {str(e)}"
        errores.append(error_msg)
        return {
            "total_encontrados": 0,
            "nuevos_creados": 0,
            "errores": errores,
            "exitoso": False
        }


async def actualizar_estatus_proveedores_por_compras(
    db: AsyncSession,
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """Actualiza el estatus_compras de proveedores basándose en compras y fecha de creación.
    
    Reglas:
    - Cliente existente con compras en los últimos 6 meses → estatus_compras = "activo"
    - Cliente existente sin compras en los últimos 6 meses → estatus_compras = "baja"
    - Cliente nuevo creado este mes → estatus_compras = "alta"
    """
    errores = []
    proveedores_actualizados = 0
    proveedores_marcados_baja = 0
    proveedores_marcados_activo = 0
    proveedores_marcados_alta = 0
    
    try:
        # Fecha límite para compras (últimos 6 meses)
        fecha_limite_compras = datetime.now(timezone.utc) - timedelta(days=180)  # 6 meses aproximadamente
        
        # Fecha límite para nuevo cliente (inicio del mes actual)
        ahora = datetime.now(timezone.utc)
        fecha_inicio_mes = datetime(ahora.year, ahora.month, 1, tzinfo=timezone.utc)
        
        # Obtener todos los proveedores
        query_proveedores = select(Proveedor)
        result_proveedores = await db.execute(query_proveedores)
        todos_proveedores = result_proveedores.scalars().all()
        
        # Para cada proveedor, determinar su estatus
        for proveedor in todos_proveedores:
            try:
                # Determinar el nuevo estatus_compras
                nuevo_estatus_compras = None
                
                # Verificar si es un nuevo cliente (creado este mes)
                es_nuevo_cliente = False
                if proveedor.created_at and proveedor.created_at >= fecha_inicio_mes:
                    es_nuevo_cliente = True
                
                if es_nuevo_cliente:
                    # Cliente nuevo creado este mes → alta
                    nuevo_estatus_compras = "alta"
                else:
                    # Cliente existente: verificar compras en los últimos 6 meses
                    query_compra_reciente = select(func.max(Compra.posting_date)).where(
                        Compra.codigo_proveedor == proveedor.codigo_proveedor,
                        Compra.posting_date.isnot(None)
                    )
                    result_compra_reciente = await db.execute(query_compra_reciente)
                    fecha_ultima_compra = result_compra_reciente.scalar_one_or_none()
                    
                    # Si tiene compras en los últimos 6 meses → activo, sino → baja
                    if fecha_ultima_compra and fecha_ultima_compra >= fecha_limite_compras:
                        nuevo_estatus_compras = "activo"
                    else:
                        nuevo_estatus_compras = "baja"
                
                # Actualizar estatus_compras si es diferente al actual
                if proveedor.estatus_compras != nuevo_estatus_compras:
                    max_retries = 2
                    retry_count = 0
                    success = False
                    
                    while retry_count < max_retries and not success:
                        try:
                            await update_proveedor(
                                db=db,
                                codigo_proveedor=proveedor.codigo_proveedor,
                                estatus_compras=nuevo_estatus_compras,
                                user_id=user_id
                            )
                            proveedores_actualizados += 1
                            
                            # Contar por tipo de estatus
                            if nuevo_estatus_compras == "baja":
                                proveedores_marcados_baja += 1
                            elif nuevo_estatus_compras == "activo":
                                proveedores_marcados_activo += 1
                            elif nuevo_estatus_compras == "alta":
                                proveedores_marcados_alta += 1
                            
                            success = True
                        except Exception as update_error:
                            error_str = str(update_error)
                            retry_count += 1
                            
                            # Hacer rollback
                            try:
                                await db.rollback()
                            except Exception:
                                pass
                            
                            # Si es un error de secuencia desincronizada y es el primer intento, intentar corregirla
                            if (retry_count == 1 and 
                                "duplicate key value violates unique constraint" in error_str and 
                                "proveedores_historial_pkey" in error_str):
                                try:
                                    # Obtener el máximo ID actual del historial
                                    # Usar una nueva consulta después del rollback
                                    result = await db.execute(
                                        select(func.max(ProveedorHistorial.id))
                                    )
                                    max_id = result.scalar() or 0
                                    
                                    # Corregir la secuencia - ejecutar directamente sin commit adicional
                                    # El rollback ya limpió la transacción, así que esto debería funcionar
                                    stmt = text(f"SELECT setval('proveedores_historial_id_seq', {max_id + 1}, false)")
                                    await db.execute(stmt)
                                    await db.commit()
                                    # Continuar el loop para reintentar
                                except Exception as seq_error:
                                    try:
                                        await db.rollback()
                                    except Exception:
                                        pass
                                    error_msg = f"Error al corregir secuencia para proveedor {proveedor.codigo_proveedor}: {str(seq_error)}. La secuencia puede necesitar corrección manual."
                                    errores.append(error_msg)
                                    break  # Salir del loop de reintentos
                            else:
                                # Otro tipo de error o segundo intento fallido
                                error_msg = f"Error al actualizar proveedor {proveedor.codigo_proveedor}: {str(update_error)}"
                                errores.append(error_msg)
                                break  # Salir del loop de reintentos
                    
            except Exception as e:
                # Hacer rollback en caso de error
                try:
                    await db.rollback()
                except Exception:
                    pass
                error_msg = f"Error al procesar proveedor {proveedor.codigo_proveedor}: {str(e)}"
                errores.append(error_msg)
                continue
        
        return {
            "total_proveedores": len(todos_proveedores),
            "proveedores_actualizados": proveedores_actualizados,
            "proveedores_marcados_baja": proveedores_marcados_baja,
            "proveedores_marcados_activo": proveedores_marcados_activo,
            "proveedores_marcados_alta": proveedores_marcados_alta,
            "errores": errores,
            "exitoso": len(errores) == 0
        }
        
    except Exception as e:
        error_msg = f"Error general al actualizar estatus: {str(e)}"
        errores.append(error_msg)
        return {
            "total_proveedores": 0,
            "proveedores_actualizados": 0,
            "proveedores_marcados_baja": 0,
            "proveedores_marcados_activo": 0,
            "proveedores_marcados_alta": 0,
            "errores": errores,
            "exitoso": False
        }


# ==================== CRUD para Materiales ====================

def _material_to_dict(material: Material) -> Dict[str, Any]:
    """Convierte un objeto Material a diccionario para el historial."""
    return {
        "id": material.id,
        "numero_material": material.numero_material,
        "descripcion_material": material.descripcion_material,
        "created_at": material.created_at.isoformat() if material.created_at else None,
        "updated_at": material.updated_at.isoformat() if material.updated_at else None
    }


async def get_material_by_id(db: AsyncSession, material_id: int) -> Optional[Material]:
    """Obtiene un material por ID."""
    result = await db.execute(select(Material).where(Material.id == material_id))
    return result.scalar_one_or_none()


async def get_material_by_numero(db: AsyncSession, numero_material: str) -> Optional[Material]:
    """Obtiene un material por número de material."""
    result = await db.execute(select(Material).where(Material.numero_material == numero_material))
    return result.scalar_one_or_none()


async def create_material(
    db: AsyncSession,
    numero_material: str,
    descripcion_material: Optional[str] = None,
    user_id: Optional[int] = None
) -> Material:
    """Crea un nuevo material."""
    # Verificar que no exista antes de crear
    material_existente = await get_material_by_numero(db, numero_material)
    if material_existente:
        raise ValueError(f"El material {numero_material} ya existe")
    
    material = Material(
        numero_material=numero_material,
        descripcion_material=descripcion_material
    )
    db.add(material)
    await db.flush()  # Flush para obtener el ID sin hacer commit
    await db.refresh(material)
    
    # Registrar en historial
    if user_id is not None:
        historial = MaterialHistorial(
            numero_material=numero_material,
            material_id=material.id,
            operacion=MaterialOperacion.CREATE,
            user_id=user_id,
            datos_antes=None,
            datos_despues=_material_to_dict(material),
            campos_modificados=None
        )
        db.add(historial)
    
    await db.commit()
    return material


async def update_material(
    db: AsyncSession,
    material_id: int,
    descripcion_material: Optional[str] = None,
    user_id: Optional[int] = None
) -> Optional[Material]:
    """Actualiza un material."""
    material = await get_material_by_id(db, material_id)
    if not material:
        return None
    
    # Guardar datos antes del cambio
    datos_antes = _material_to_dict(material)
    campos_modificados = []
    
    if descripcion_material is not None and material.descripcion_material != descripcion_material:
        material.descripcion_material = descripcion_material
        campos_modificados.append("descripcion_material")
    
    # Solo registrar en historial si hubo cambios
    if campos_modificados and user_id is not None:
        await db.flush()  # Para obtener los datos actualizados
        await db.refresh(material)
        datos_despues = _material_to_dict(material)
        
        historial = MaterialHistorial(
            numero_material=material.numero_material,
            material_id=material.id,
            operacion=MaterialOperacion.UPDATE,
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=datos_despues,
            campos_modificados=campos_modificados
        )
        db.add(historial)
    
    await db.commit()
    await db.refresh(material)
    return material


async def list_materiales(
    db: AsyncSession,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Material]:
    """Lista materiales con filtros opcionales."""
    query = select(Material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Material.numero_material.ilike(search_pattern),
                Material.descripcion_material.ilike(search_pattern)
            )
        )
    
    query = query.order_by(desc(Material.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def list_pesos_netos(
    db: AsyncSession,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[PesoNeto]:
    """Lista pesos netos con filtros opcionales."""
    query = select(PesoNeto)

    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PesoNeto.numero_parte.ilike(search_pattern),
                PesoNeto.descripcion.ilike(search_pattern),
            )
        )

    query = query.order_by(PesoNeto.numero_parte).limit(limit).offset(offset)
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_pesos_netos(
    db: AsyncSession,
    search: Optional[str] = None,
) -> int:
    """Cuenta el total de registros de peso_neto con filtros opcionales."""
    query = select(func.count(PesoNeto.numero_parte))

    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PesoNeto.numero_parte.ilike(search_pattern),
                PesoNeto.descripcion.ilike(search_pattern),
            )
        )

    result = await db.execute(query)
    return result.scalar() or 0


async def create_peso_neto_historial(
    db: AsyncSession,
    estado: str,
    detalle: str,
    user_id: Optional[int] = None,
    accion: str = "ACTUALIZAR",
    archivo_nombre: Optional[str] = None,
    filas_archivo: Optional[int] = None,
    filas_invalidas: Optional[int] = None,
    duplicados_archivo: Optional[int] = None,
    candidatos_unicos: Optional[int] = None,
    upserts: Optional[int] = None,
    insertados: Optional[int] = None,
    actualizados: Optional[int] = None,
) -> PesoNetoHistorial:
    """Crea una entrada en historial de actualización de pesos netos."""
    item = PesoNetoHistorial(
        user_id=user_id,
        accion=accion,
        estado=estado,
        archivo_nombre=archivo_nombre,
        filas_archivo=filas_archivo,
        filas_invalidas=filas_invalidas,
        duplicados_archivo=duplicados_archivo,
        candidatos_unicos=candidatos_unicos,
        upserts=upserts,
        insertados=insertados,
        actualizados=actualizados,
        detalle=detalle,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)
    return item


async def list_peso_neto_historial(
    db: AsyncSession,
    user_id: Optional[int] = None,
    limit: int = 5,
    offset: int = 0,
) -> List[PesoNetoHistorial]:
    """Lista historial de pesos netos más reciente. Si user_id se pasa, solo movimientos de ese usuario."""
    query = (
        select(PesoNetoHistorial)
        .options(selectinload(PesoNetoHistorial.user))
        .order_by(desc(PesoNetoHistorial.created_at))
        .limit(limit)
        .offset(offset)
    )
    if user_id is not None:
        query = query.where(PesoNetoHistorial.user_id == user_id)
    result = await db.execute(query)
    return list(result.scalars().all())


async def list_cross_reference(
    db: AsyncSession,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[CrossReference]:
    """Lista registros de cross_reference con filtros opcionales."""
    query = select(CrossReference)

    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                CrossReference.customer.ilike(search_pattern),
                CrossReference.material.ilike(search_pattern),
                CrossReference.customer_material.ilike(search_pattern),
            )
        )

    query = (
        query.order_by(CrossReference.customer, CrossReference.material, CrossReference.customer_material)
        .limit(limit)
        .offset(offset)
    )
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_cross_reference_por_cliente_materiales(
    db: AsyncSession,
    customer: str,
    materiales: List[str],
) -> dict:
    """
    Devuelve un diccionario material -> customer_material para el cliente y la lista de materiales.
    Si un material tiene varias entradas en cross_reference, se usa la primera (por customer_material).
    """
    if not materiales:
        return {}
    customer_str = str(customer).strip()
    materiales_set = [m.strip() for m in materiales if (m or "").strip()]
    if not materiales_set:
        return {}
    query = (
        select(CrossReference.material, CrossReference.customer_material)
        .where(CrossReference.customer == customer_str)
        .where(CrossReference.material.in_(materiales_set))
        .order_by(CrossReference.material, CrossReference.customer_material)
    )
    result = await db.execute(query)
    rows = result.all()
    # Primera aparición por material (puede haber varios customer_material por material).
    out = {}
    for material, customer_material in rows:
        if material not in out and (customer_material or "").strip():
            out[material] = (customer_material or "").strip()
    return out


async def map_cross_reference_por_pares_customer_material(
    db: AsyncSession,
    pares: List[Tuple[str, str]],
) -> Dict[Tuple[str, str], str]:
    """
    Para pares (customer, material) coincide con cross_reference.customer y cross_reference.material.
    Devuelve mapa (customer, material) -> customer_material; si hay varias filas, la primera por orden de customer_material.
    """
    if not pares:
        return {}
    seen_q = []
    seen_set = set()
    for c, m in pares:
        c = (c or "").strip()
        m = (m or "").strip()
        if not c or not m:
            continue
        if (c, m) in seen_set:
            continue
        seen_set.add((c, m))
        seen_q.append((c, m))
    if not seen_q:
        return {}
    # Evitar un IN gigante (límite de parámetros del driver / tamaño de consulta).
    _BATCH = 300
    out: Dict[Tuple[str, str], str] = {}
    for i in range(0, len(seen_q), _BATCH):
        batch = seen_q[i : i + _BATCH]
        stmt = (
            select(CrossReference.customer, CrossReference.material, CrossReference.customer_material)
            .where(tuple_(CrossReference.customer, CrossReference.material).in_(batch))
            .order_by(
                CrossReference.customer,
                CrossReference.material,
                CrossReference.customer_material,
            )
        )
        result = await db.execute(stmt)
        for cust, mat, cm in result.all():
            k = (cust, mat)
            if k not in out and (cm or "").strip():
                out[k] = (cm or "").strip()
    return out


async def count_cross_reference(
    db: AsyncSession,
    search: Optional[str] = None,
) -> int:
    """Cuenta el total de registros de cross_reference con filtros opcionales."""
    query = select(func.count()).select_from(CrossReference)

    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                CrossReference.customer.ilike(search_pattern),
                CrossReference.material.ilike(search_pattern),
                CrossReference.customer_material.ilike(search_pattern),
            )
        )

    result = await db.execute(query)
    return result.scalar() or 0


async def create_cross_reference_historial(
    db: AsyncSession,
    estado: str,
    detalle: Optional[str] = None,
    user_id: Optional[int] = None,
    accion: str = "ACTUALIZAR",
    clientes_total: Optional[int] = None,
    clientes_ok: Optional[int] = None,
    upserts: Optional[int] = None,
    errores: Optional[int] = None,
    detalle_errores: Optional[list] = None,
) -> CrossReferenceHistorial:
    """Crea una entrada en el historial de actualización de Cross Reference."""
    item = CrossReferenceHistorial(
        user_id=user_id,
        accion=accion,
        estado=estado,
        clientes_total=clientes_total,
        clientes_ok=clientes_ok,
        upserts=upserts,
        errores=errores,
        detalle=detalle,
        detalle_errores=detalle_errores,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)
    return item


async def list_cross_reference_historial(
    db: AsyncSession,
    user_id: Optional[int] = None,
    limit: int = 20,
    offset: int = 0,
) -> List[CrossReferenceHistorial]:
    """Lista el historial de movimientos de Cross Reference. Si user_id se pasa, solo movimientos de ese usuario."""
    query = (
        select(CrossReferenceHistorial)
        .options(selectinload(CrossReferenceHistorial.user))
        .order_by(desc(CrossReferenceHistorial.created_at))
        .limit(limit)
        .offset(offset)
    )
    if user_id is not None:
        query = query.where(CrossReferenceHistorial.user_id == user_id)
    result = await db.execute(query)
    return list(result.scalars().all())


async def create_bom_historial(
    db: AsyncSession,
    estado: str,
    detalle: Optional[str] = None,
    user_id: Optional[int] = None,
    accion: str = "ACTUALIZAR",
    total: Optional[int] = None,
    procesados: Optional[int] = None,
    con_cambios: Optional[int] = None,
    sin_cambios: Optional[int] = None,
    errores: Optional[int] = None,
    detalle_json: Optional[list] = None,
) -> BomHistorial:
    """Crea una entrada en el historial de actualización de BOMs."""
    item = BomHistorial(
        user_id=user_id,
        accion=accion,
        estado=estado,
        total=total,
        procesados=procesados,
        con_cambios=con_cambios,
        sin_cambios=sin_cambios,
        errores=errores,
        detalle=detalle,
        detalle_json=detalle_json,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)
    return item


async def list_bom_historial(
    db: AsyncSession,
    user_id: Optional[int] = None,
    limit: int = 20,
    offset: int = 0,
) -> List[BomHistorial]:
    """Lista el historial de movimientos de actualización de BOMs. Si user_id se pasa, solo movimientos de ese usuario."""
    query = (
        select(BomHistorial)
        .options(selectinload(BomHistorial.user))
        .order_by(desc(BomHistorial.created_at))
        .limit(limit)
        .offset(offset)
    )
    if user_id is not None:
        query = query.where(BomHistorial.user_id == user_id)
    result = await db.execute(query)
    return list(result.scalars().all())


async def list_precios_venta(
    db: AsyncSession,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[PrecioVenta]:
    """Lista registros de precios_venta con filtros opcionales."""
    query = (
        select(PrecioVenta)
        .options(
            selectinload(PrecioVenta.cliente),
            selectinload(PrecioVenta.parte),
        )
    )

    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PrecioVenta.codigo_cliente.cast(String).ilike(search_pattern),
                PrecioVenta.numero_parte.ilike(search_pattern),
                PrecioVenta.tipo_cable.ilike(search_pattern),
                PrecioVenta.comentario.ilike(search_pattern),
                PrecioVenta.comentario_2.ilike(search_pattern),
                PrecioVenta.comentario_3.ilike(search_pattern),
            )
        )

    query = (
        query.order_by(
            PrecioVenta.codigo_cliente.asc(),
            PrecioVenta.numero_parte.asc(),
            PrecioVenta.tipo_cable.asc(),
            desc(PrecioVenta.updated_at),
        )
        .limit(limit)
        .offset(offset)
    )
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_precios_venta(
    db: AsyncSession,
    search: Optional[str] = None,
) -> int:
    """Cuenta el total de registros de precios_venta con filtros opcionales."""
    query = select(func.count()).select_from(PrecioVenta)

    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PrecioVenta.codigo_cliente.cast(String).ilike(search_pattern),
                PrecioVenta.numero_parte.ilike(search_pattern),
                PrecioVenta.tipo_cable.ilike(search_pattern),
                PrecioVenta.comentario.ilike(search_pattern),
                PrecioVenta.comentario_2.ilike(search_pattern),
                PrecioVenta.comentario_3.ilike(search_pattern),
            )
        )

    result = await db.execute(query)
    return result.scalar() or 0


async def get_precio_venta_by_id(
    db: AsyncSession,
    precio_venta_id: int,
) -> Optional[PrecioVenta]:
    """Obtiene un precio de venta por ID."""
    result = await db.execute(
        select(PrecioVenta).where(PrecioVenta.id == precio_venta_id)
    )
    return result.scalar_one_or_none()


def _venta_precio_icr_filters():
    """Filtros para ventas con precios ICR válidos (no null ni 0)."""
    return and_(
        Venta.precio_exmetal_km.isnot(None),
        Venta.precio_exmetal_km != 0,
        Venta.precio_exmetal_m.isnot(None),
        Venta.precio_exmetal_m != 0,
        Venta.precio_full_metal_km.isnot(None),
        Venta.precio_full_metal_km != 0,
        Venta.precio_full_metal_m.isnot(None),
        Venta.precio_full_metal_m != 0,
    )


async def get_ultimo_precio_venta_cliente_parte(
    db: AsyncSession,
    codigo_cliente: int,
    numero_parte: str,
) -> Optional[float]:
    """
    Obtiene el precio del último registro de venta de ese número de parte para el cliente.
    Fuente: tabla ventas (última venta por id con producto_condensado = numero_parte y precios ICR válidos).
    Se usa precio_full_metal_km como precio de referencia.
    """
    numero_parte = str(numero_parte).strip()
    if not numero_parte:
        return None
    result = await db.execute(
        select(Venta.precio_full_metal_km)
        .where(
            Venta.codigo_cliente == int(codigo_cliente),
            Venta.producto_condensado == numero_parte,
            _venta_precio_icr_filters(),
        )
        .order_by(desc(Venta.id))
        .limit(1)
    )
    row = result.scalar_one_or_none()
    if row is None:
        return None
    return float(row)


async def get_ultimo_precio_venta_parte(
    db: AsyncSession,
    numero_parte: str,
) -> Optional[float]:
    """
    Obtiene el último precio de venta (precio_full_metal_km) para un número de parte,
    sin filtrar por cliente. Útil para Simulacion Producto.
    """
    numero_parte = str(numero_parte).strip()
    if not numero_parte:
        return None
    result = await db.execute(
        select(Venta.precio_full_metal_km)
        .where(
            Venta.producto_condensado == numero_parte,
            _venta_precio_icr_filters(),
        )
        .order_by(desc(Venta.id))
        .limit(1)
    )
    row = result.scalar_one_or_none()
    if row is None:
        return None
    return float(row)


async def _ensure_precios_venta_historial_detalle_column(db: AsyncSession) -> None:
    """Asegura columna detalle en precios_venta_historial para compatibilidad."""
    await db.execute(
        text(
            """
            ALTER TABLE IF EXISTS precios_venta_historial
            ADD COLUMN IF NOT EXISTS detalle TEXT
            """
        )
    )
    await db.commit()


async def update_precio_venta(
    db: AsyncSession,
    precio_venta_id: int,
    precio_venta: Optional[Decimal] = None,
    comentario: Optional[str] = None,
    comentario_2: Optional[str] = None,
    comentario_3: Optional[str] = None,
    user_id: Optional[int] = None,
) -> Optional[PrecioVenta]:
    """Actualiza únicamente precio_venta y comentarios de un registro de precios_venta."""
    await _ensure_precios_venta_historial_detalle_column(db)

    item = await get_precio_venta_by_id(db, precio_venta_id)
    if not item:
        return None

    # Guardar estado previo para historial.
    datos_antes = {
        "precio_venta": float(item.precio_venta) if item.precio_venta is not None else None,
        "comentario": item.comentario,
        "comentario_2": item.comentario_2,
        "comentario_3": item.comentario_3,
    }

    campos_modificados = []
    if item.precio_venta != precio_venta:
        item.precio_venta = precio_venta
        campos_modificados.append("precio_venta")
    if (item.comentario or None) != (comentario or None):
        item.comentario = comentario
        campos_modificados.append("comentario")
    if (item.comentario_2 or None) != (comentario_2 or None):
        item.comentario_2 = comentario_2
        campos_modificados.append("comentario_2")
    if (item.comentario_3 or None) != (comentario_3 or None):
        item.comentario_3 = comentario_3
        campos_modificados.append("comentario_3")

    if campos_modificados and user_id is not None:
        datos_despues = {
            "precio_venta": float(item.precio_venta) if item.precio_venta is not None else None,
            "comentario": item.comentario,
            "comentario_2": item.comentario_2,
            "comentario_3": item.comentario_3,
        }
        historial = PrecioVentaHistorial(
            precio_venta_id=item.id,
            codigo_cliente=item.codigo_cliente,
            numero_parte=item.numero_parte,
            tipo_cable=item.tipo_cable,
            operacion="UPDATE",
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=datos_despues,
            campos_modificados=campos_modificados,
        )
        db.add(historial)

    await db.commit()
    await db.refresh(item)
    return item


async def update_comentarios_precio_venta(
    db: AsyncSession,
    precio_venta_id: int,
    comentario: Optional[str] = None,
    comentario_2: Optional[str] = None,
    comentario_3: Optional[str] = None,
    user_id: Optional[int] = None,
) -> Optional[PrecioVenta]:
    """Actualiza únicamente comentarios de un registro de precios_venta."""
    await _ensure_precios_venta_historial_detalle_column(db)

    item = await get_precio_venta_by_id(db, precio_venta_id)
    if not item:
        return None

    datos_antes = {
        "comentario": item.comentario,
        "comentario_2": item.comentario_2,
        "comentario_3": item.comentario_3,
    }

    campos_modificados = []
    if (item.comentario or None) != (comentario or None):
        item.comentario = comentario
        campos_modificados.append("comentario")
    if (item.comentario_2 or None) != (comentario_2 or None):
        item.comentario_2 = comentario_2
        campos_modificados.append("comentario_2")
    if (item.comentario_3 or None) != (comentario_3 or None):
        item.comentario_3 = comentario_3
        campos_modificados.append("comentario_3")

    if campos_modificados and user_id is not None:
        datos_despues = {
            "comentario": item.comentario,
            "comentario_2": item.comentario_2,
            "comentario_3": item.comentario_3,
        }
        historial = PrecioVentaHistorial(
            precio_venta_id=item.id,
            codigo_cliente=item.codigo_cliente,
            numero_parte=item.numero_parte,
            tipo_cable=item.tipo_cable,
            operacion="UPDATE",
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=datos_despues,
            campos_modificados=campos_modificados,
        )
        db.add(historial)

    await db.commit()
    await db.refresh(item)
    return item


async def sincronizar_precios_venta_desde_ventas(
    db: AsyncSession,
    user_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Revisa todas las ventas y actualiza/crea precios_venta con el último precio_full_metal_km
    por (codigo_cliente, producto_condensado=numero_parte). Crea registros nuevos cuando
    hay ventas que aún no están en precios_venta (cliente + número de parte existentes).
    """
    await _ensure_precios_venta_historial_detalle_column(db)

    # Últimas ventas por clave (cliente, producto_condensado = numero de parte).
    ventas_query = (
        select(
            Venta.id,
            Venta.codigo_cliente,
            Venta.producto_condensado,
            Venta.sales_km,
            Venta.precio_full_metal_km,
            Venta.periodo,
        )
        .where(
            Venta.codigo_cliente.isnot(None),
            Venta.producto_condensado.isnot(None),
            Venta.sales_km.isnot(None),
            Venta.precio_full_metal_km.isnot(None),
        )
        .order_by(desc(Venta.periodo), desc(Venta.id))
    )
    ventas_rows = (await db.execute(ventas_query)).all()

    ultimas_ventas_por_clave: Dict[tuple[int, str], Dict[str, Any]] = {}
    for row in ventas_rows:
        key = (int(row.codigo_cliente), str(row.producto_condensado).strip())
        if key in ultimas_ventas_por_clave:
            continue
        ultimas_ventas_por_clave[key] = {
            "venta_id": row.id,
            "precio_full_metal_km": row.precio_full_metal_km,
            "sales_km": row.sales_km,
            "periodo": row.periodo,
        }

    precios_rows = (await db.execute(select(PrecioVenta))).scalars().all()
    existing_keys = {(int(item.codigo_cliente), str(item.numero_parte).strip()) for item in precios_rows}

    total_precios_venta = len(precios_rows)
    con_match = 0
    actualizados = 0
    sin_cambios = 0
    sin_venta = 0
    creados = 0
    creados_omitidos = 0

    # Actualizar registros existentes
    for item in precios_rows:
        key = (int(item.codigo_cliente), str(item.numero_parte).strip())
        venta_ref = ultimas_ventas_por_clave.get(key)
        if not venta_ref:
            sin_venta += 1
            continue

        con_match += 1
        nuevo_precio = venta_ref["precio_full_metal_km"]
        if item.precio_venta == nuevo_precio:
            sin_cambios += 1
            continue

        precio_anterior = float(item.precio_venta) if item.precio_venta is not None else None
        precio_nuevo = float(nuevo_precio) if nuevo_precio is not None else None
        item.precio_venta = nuevo_precio
        actualizados += 1

        if user_id is not None:
            periodo_txt = venta_ref["periodo"].isoformat() if venta_ref["periodo"] is not None else "N/A"
            detalle = (
                "Actualización automática desde ventas "
                f"(venta_id={venta_ref['venta_id']}, periodo={periodo_txt}, "
                f"sales_km={venta_ref['sales_km']}, precio_full_metal_km={precio_nuevo})."
            )
            historial = PrecioVentaHistorial(
                precio_venta_id=item.id,
                codigo_cliente=item.codigo_cliente,
                numero_parte=item.numero_parte,
                tipo_cable=item.tipo_cable,
                operacion="UPDATE",
                user_id=user_id,
                datos_antes={"precio_venta": precio_anterior},
                datos_despues={"precio_venta": precio_nuevo},
                campos_modificados=["precio_venta"],
                detalle=detalle,
            )
            db.add(historial)

    # Crear registros faltantes: ventas con (cliente, numero_parte) que no están en precios_venta
    keys_solo_en_ventas = [k for k in ultimas_ventas_por_clave if k not in existing_keys]
    if keys_solo_en_ventas:
        codigos_cliente = list({k[0] for k in keys_solo_en_ventas})
        numeros_parte = list({k[1] for k in keys_solo_en_ventas})
        clientes_result = await db.execute(select(Cliente.codigo_cliente).where(Cliente.codigo_cliente.in_(codigos_cliente)))
        clientes_set = {int(r[0]) for r in clientes_result.all()}
        partes_result = await db.execute(select(Parte.numero_parte).where(Parte.numero_parte.in_(numeros_parte)))
        partes_set = {str(r[0]).strip() for r in partes_result.all()}

        for key in keys_solo_en_ventas:
            codigo_cliente, numero_parte = key[0], key[1]
            if codigo_cliente not in clientes_set or numero_parte not in partes_set:
                creados_omitidos += 1
                continue
            venta_ref = ultimas_ventas_por_clave[key]
            nuevo_precio = venta_ref["precio_full_metal_km"]
            precio_nuevo = float(nuevo_precio) if nuevo_precio is not None else None
            nuevo = PrecioVenta(
                codigo_cliente=codigo_cliente,
                numero_parte=numero_parte,
                tipo_cable=None,
                precio_venta=Decimal(str(precio_nuevo)) if precio_nuevo is not None else None,
            )
            db.add(nuevo)
            await db.flush()
            creados += 1
            if user_id is not None:
                periodo_txt = venta_ref["periodo"].isoformat() if venta_ref["periodo"] is not None else "N/A"
                detalle = (
                    "Creado desde ventas "
                    f"(venta_id={venta_ref['venta_id']}, periodo={periodo_txt}, "
                    f"precio_full_metal_km={precio_nuevo})."
                )
                historial = PrecioVentaHistorial(
                    precio_venta_id=nuevo.id,
                    codigo_cliente=nuevo.codigo_cliente,
                    numero_parte=nuevo.numero_parte,
                    tipo_cable=nuevo.tipo_cable,
                    operacion="CREATE",
                    user_id=user_id,
                    datos_antes=None,
                    datos_despues={"precio_venta": precio_nuevo},
                    campos_modificados=None,
                    detalle=detalle,
                )
                db.add(historial)

    await db.commit()
    return {
        "total_precios_venta": total_precios_venta,
        "ventas_consideradas": len(ultimas_ventas_por_clave),
        "con_match": con_match,
        "actualizados": actualizados,
        "sin_cambios": sin_cambios,
        "sin_venta": sin_venta,
        "creados": creados,
        "creados_omitidos": creados_omitidos,
    }


async def list_precio_venta_historial(
    db: AsyncSession,
    user_id: Optional[int] = None,
    limit: int = 5,
    offset: int = 0,
) -> List[PrecioVentaHistorial]:
    """Lista historial reciente de cambios en precios_venta. Si user_id se pasa, solo movimientos de ese usuario."""
    await _ensure_precios_venta_historial_detalle_column(db)

    query = (
        select(PrecioVentaHistorial)
        .options(selectinload(PrecioVentaHistorial.user))
        .order_by(desc(PrecioVentaHistorial.created_at))
        .limit(limit)
        .offset(offset)
    )
    if user_id is not None:
        query = query.where(PrecioVentaHistorial.user_id == user_id)
    result = await db.execute(query)
    return list(result.scalars().all())


async def delete_material(
    db: AsyncSession,
    material_id: int,
    user_id: Optional[int] = None
) -> bool:
    """Elimina un material."""
    from sqlalchemy import delete
    
    material = await get_material_by_id(db, material_id)
    if not material:
        return False
    
    # Guardar datos antes de eliminar
    datos_antes = _material_to_dict(material)
    numero_material = material.numero_material
    
    stmt = delete(Material).where(Material.id == material_id)
    await db.execute(stmt)
    
    # Registrar en historial
    if user_id is not None:
        historial = MaterialHistorial(
            numero_material=numero_material,
            material_id=material_id,
            operacion=MaterialOperacion.DELETE,
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=None,
            campos_modificados=None
        )
        db.add(historial)
    
    await db.commit()
    return True


async def count_materiales(
    db: AsyncSession,
    search: Optional[str] = None
) -> int:
    """Cuenta el total de materiales con filtros opcionales."""
    query = select(func.count(Material.id))
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Material.numero_material.ilike(search_pattern),
                Material.descripcion_material.ilike(search_pattern)
            )
        )
    
    result = await db.execute(query)
    return result.scalar() or 0


# ==================== CRUD para Historial de Materiales ====================

async def list_material_historial(
    db: AsyncSession,
    numero_material: Optional[str] = None,
    material_id: Optional[int] = None,
    operacion: Optional[MaterialOperacion] = None,
    user_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0
) -> List[MaterialHistorial]:
    """Lista el historial de cambios en materiales con filtros opcionales."""
    query = select(MaterialHistorial).options(selectinload(MaterialHistorial.user))
    
    if numero_material:
        query = query.where(MaterialHistorial.numero_material == numero_material)
    
    if material_id:
        query = query.where(MaterialHistorial.material_id == material_id)
    
    if operacion:
        query = query.where(MaterialHistorial.operacion == operacion)
    
    if user_id:
        query = query.where(MaterialHistorial.user_id == user_id)
    
    query = query.order_by(desc(MaterialHistorial.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_material_historial_by_id(
    db: AsyncSession,
    historial_id: int
) -> Optional[MaterialHistorial]:
    """Obtiene un registro del historial por ID."""
    result = await db.execute(
        select(MaterialHistorial).where(MaterialHistorial.id == historial_id)
    )
    return result.scalar_one_or_none()


async def count_material_historial(
    db: AsyncSession,
    numero_material: Optional[str] = None,
    material_id: Optional[int] = None,
    operacion: Optional[MaterialOperacion] = None,
    user_id: Optional[int] = None
) -> int:
    """Cuenta el total de registros en el historial con filtros opcionales."""
    query = select(func.count(MaterialHistorial.id))
    
    if numero_material:
        query = query.where(MaterialHistorial.numero_material == numero_material)
    
    if material_id:
        query = query.where(MaterialHistorial.material_id == material_id)
    
    if operacion:
        query = query.where(MaterialHistorial.operacion == operacion)
    
    if user_id:
        query = query.where(MaterialHistorial.user_id == user_id)
    
    result = await db.execute(query)
    return result.scalar() or 0


async def sincronizar_materiales_desde_compras(
    db: AsyncSession,
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Sincroniza materiales desde la tabla compras.
    Busca todos los numero_material únicos en compras que no estén registrados
    en materiales y los crea usando la descripcion_material de compras.
    Siempre guarda numero_material sin notación científica (ej. 342410000000 en vez de 3.4241E+11).
    
    Returns:
        Dict con estadísticas de la sincronización:
        - total_encontrados: Total de números únicos en compras
        - nuevos_creados: Cantidad de materiales nuevos creados
        - errores: Lista de errores encontrados
    """
    errores = []
    nuevos_creados = 0
    
    try:
        # 1. Obtener todos los numero_material únicos de compras que no sean NULL
        # Usamos GROUP BY para obtener la descripcion_material más común
        query_codigos = select(
            Compra.numero_material,
            func.max(Compra.descripcion_material).label('descripcion_material')
        ).where(
            Compra.numero_material.isnot(None),
            Compra.numero_material != ''
        ).group_by(Compra.numero_material)
        
        result = await db.execute(query_codigos)
        materiales_en_compras = result.all()
        
        total_encontrados = len(materiales_en_compras)
        
        # 2. Números de materiales existentes por número normalizado (evita duplicados con notación científica)
        query_existentes = select(Material.numero_material)
        result_existentes = await db.execute(query_existentes)
        numeros_existentes = set()
        for row in result_existentes.all():
            if row[0]:
                n = _normalizar_numero_material(row[0])
                if n:
                    numeros_existentes.add(n)
        
        # 3. Para cada numero_material de compras, normalizar y crear si no existe
        for numero_material_raw, descripcion_material in materiales_en_compras:
            if not numero_material_raw or str(numero_material_raw).strip() == '':
                continue
            numero_material = _normalizar_numero_material(numero_material_raw)
            if not numero_material:
                continue
            
            # Verificar si el material ya existe (por número normalizado)
            if numero_material not in numeros_existentes:
                # Verificar nuevamente en la BD antes de crear (por si acaso)
                material_existente = await get_material_by_numero(db, numero_material)
                if material_existente:
                    numeros_existentes.add(numero_material)
                    continue
                
                # Crear el material con número sin notación científica y descripción de compras
                try:
                    material = Material(
                        numero_material=numero_material,
                        descripcion_material=descripcion_material
                    )
                    db.add(material)
                    await db.flush()  # Flush para obtener el ID sin hacer commit
                    await db.refresh(material)
                    
                    # Registrar en historial si se proporciona user_id
                    if user_id is not None:
                        historial = MaterialHistorial(
                            numero_material=numero_material,
                            material_id=material.id,
                            operacion=MaterialOperacion.CREATE,
                            user_id=user_id,
                            datos_antes=None,
                            datos_despues=_material_to_dict(material),
                            campos_modificados=None
                        )
                        db.add(historial)
                    
                    await db.commit()
                    nuevos_creados += 1
                    # Agregar a la lista de existentes para evitar duplicados en la misma ejecución
                    numeros_existentes.add(numero_material)
                except Exception as e:
                    error_str = str(e)
                    # Hacer rollback de la transacción para poder continuar
                    try:
                        await db.rollback()
                    except:
                        pass
                    
                    # Si es un error de secuencia, intentar corregirla
                    if "duplicate key value violates unique constraint" in error_str and "materiales_pkey" in error_str:
                        try:
                            # Obtener el máximo ID actual
                            result = await db.execute(
                                select(func.max(Material.id))
                            )
                            max_id = result.scalar() or 0
                            
                            # Corregir la secuencia
                            await db.execute(
                                text(f"SELECT setval('materiales_id_seq', {max_id + 1}, false)")
                            )
                            await db.commit()
                            
                            # Intentar crear el material nuevamente
                            try:
                                material = Material(
                                    numero_material=numero_material,
                                    descripcion_material=descripcion_material
                                )
                                db.add(material)
                                await db.flush()
                                await db.refresh(material)
                                
                                if user_id is not None:
                                    historial = MaterialHistorial(
                                        numero_material=numero_material,
                                        material_id=material.id,
                                        operacion=MaterialOperacion.CREATE,
                                        user_id=user_id,
                                        datos_antes=None,
                                        datos_despues=_material_to_dict(material),
                                        campos_modificados=None
                                    )
                                    db.add(historial)
                                
                                await db.commit()
                                nuevos_creados += 1
                                numeros_existentes.add(numero_material)
                                continue
                            except Exception as e2:
                                await db.rollback()
                        except Exception as seq_error:
                            await db.rollback()
                    
                    # Verificar si el material se creó de todas formas (race condition)
                    material_existente = await get_material_by_numero(db, numero_material)
                    if material_existente:
                        # El material ya existe, no es un error real
                        numeros_existentes.add(numero_material)
                        continue
                    
                    # Solo reportar error si realmente no se pudo crear
                    error_msg = f"Error al crear material {numero_material}: {error_str}"
                    errores.append(error_msg)
                    # Continuar con el siguiente material
                    continue
        
        return {
            "total_encontrados": total_encontrados,
            "nuevos_creados": nuevos_creados,
            "errores": errores,
            "exitoso": len(errores) == 0
        }
        
    except Exception as e:
        await db.rollback()
        error_msg = f"Error general en sincronización: {str(e)}"
        errores.append(error_msg)
        return {
            "total_encontrados": 0,
            "nuevos_creados": 0,
            "errores": errores,
            "exitoso": False
        }


def _es_notacion_cientifica(val: Any) -> bool:
    """True si el valor está en notación científica (ej. 3.4241E+11)."""
    if val is None:
        return False
    s = str(val).strip()
    if not s:
        return False
    s_lower = s.lower()
    if "e" not in s_lower:
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False


def _normalizar_numero_material(val: Any) -> str:
    """
    Convierte numero_material a string sin notación científica.
    Ej: 3.4241E+11 o '3.4241E+11' -> '342410000000'.
    Así se evita violar FK con materiales donde el número está guardado completo.
    """
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    try:
        f = float(s)
        if f == int(f):
            return str(int(f))
        return s
    except ValueError:
        return s


async def normalizar_numero_material_cientifico_en_bd(db: AsyncSession) -> Dict[str, Any]:
    """
    Reemplaza todos los numero_material en notación científica por el número entero
    (ej. 3.4241E+11 -> 342410000000) en materiales, compras, precios_materiales,
    pais_origen_material y sus tablas de historial. No elimina datos; solo normaliza.
    """
    resultado: Dict[str, Any] = {
        "materiales_actualizados": 0,
        "materiales_eliminados_duplicados": 0,
        "compras_actualizadas": 0,
        "precios_materiales_actualizados": 0,
        "pais_origen_material_actualizados": 0,
        "historiales_actualizados": 0,
        "errores": [],
    }
    try:
        # 1. Materiales con numero_material en notación científica
        query_mat = select(Material).where(Material.numero_material.isnot(None))
        res_mat = await db.execute(query_mat)
        materiales = res_mat.scalars().all()
        cientificos = [(m, _normalizar_numero_material(m.numero_material)) for m in materiales if _es_notacion_cientifica(m.numero_material)]
        if not cientificos:
            return resultado

        for material, numero_norm in cientificos:
            if not numero_norm:
                continue
            try:
                existente = await get_material_by_numero(db, numero_norm)
                if existente and existente.id != material.id:
                    # Ya existe un material con el número normalizado: referencias al científico -> normalizado, luego borrar material científico
                    await db.execute(
                        update(PrecioMaterial).where(PrecioMaterial.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(PaisOrigenMaterial).where(PaisOrigenMaterial.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(Compra).where(Compra.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(MaterialHistorial).where(MaterialHistorial.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(PaisOrigenMaterialHistorial).where(PaisOrigenMaterialHistorial.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(PrecioMaterialHistorial).where(PrecioMaterialHistorial.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.delete(material)
                    resultado["materiales_eliminados_duplicados"] += 1
                else:
                    # No hay duplicado: actualizar el material y todas las referencias
                    await db.execute(
                        update(Material).where(Material.id == material.id).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(PrecioMaterial).where(PrecioMaterial.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(PaisOrigenMaterial).where(PaisOrigenMaterial.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(Compra).where(Compra.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(MaterialHistorial).where(MaterialHistorial.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(PaisOrigenMaterialHistorial).where(PaisOrigenMaterialHistorial.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    await db.execute(
                        update(PrecioMaterialHistorial).where(PrecioMaterialHistorial.numero_material == material.numero_material).values(numero_material=numero_norm)
                    )
                    resultado["materiales_actualizados"] += 1
            except Exception as e:
                resultado["errores"].append(f"Material {material.numero_material}: {e}")
                try:
                    await db.rollback()
                except Exception:
                    pass
                continue

        await db.commit()

        # Valores en notación científica huérfanos (compras/historial sin material correspondiente)
        for model in [Compra, PrecioMaterial, PaisOrigenMaterial, MaterialHistorial, PaisOrigenMaterialHistorial, PrecioMaterialHistorial]:
            try:
                q = select(model.numero_material).where(model.numero_material.isnot(None)).distinct()
                r = await db.execute(q)
                for (val,) in r.all():
                    if val and _es_notacion_cientifica(val):
                        norm = _normalizar_numero_material(val)
                        if norm:
                            stmt = update(model).where(model.numero_material == val).values(numero_material=norm)
                            await db.execute(stmt)
                            if model == Compra:
                                resultado["compras_actualizadas"] += 1
                            elif model == PrecioMaterial:
                                resultado["precios_materiales_actualizados"] += 1
                            elif model == PaisOrigenMaterial:
                                resultado["pais_origen_material_actualizados"] += 1
                            else:
                                resultado["historiales_actualizados"] += 1
            except Exception as e:
                resultado["errores"].append(f"Tabla {model.__tablename__}: {e}")
                try:
                    await db.rollback()
                except Exception:
                    pass

        await db.commit()
    except Exception as e:
        await db.rollback()
        resultado["errores"].append(str(e))
    return resultado


async def sincronizar_paises_origen_desde_compras(
    db: AsyncSession,
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Sincroniza países de origen desde la tabla compras.
    Busca combinaciones únicas de codigo_proveedor y numero_material en compras
    que no estén registradas en pais_origen_material y las crea con pais_origen = "Pendiente".
    Solo crea registros cuando el material existe en la tabla materiales (por la FK).
    
    Returns:
        Dict con estadísticas de la sincronización:
        - total_encontrados: Total de combinaciones únicas en compras
        - nuevos_creados: Cantidad de registros nuevos creados
        - errores: Lista de errores encontrados
    """
    errores = []
    nuevos_creados = 0
    
    try:
        # 0. Asegurar que la secuencia y DEFAULT del id existen (evita NULL identity key)
        try:
            await db.execute(text("CREATE SEQUENCE IF NOT EXISTS pais_origen_material_id_seq"))
            await db.execute(text("""
                SELECT setval('pais_origen_material_id_seq',
                    COALESCE((SELECT MAX(id) FROM pais_origen_material), 0) + 1,
                    false)
            """))
            await db.execute(text("""
                ALTER TABLE pais_origen_material
                ALTER COLUMN id SET DEFAULT nextval('pais_origen_material_id_seq'::regclass)
            """))
            await db.commit()
        except Exception as seq_err:
            await db.rollback()
            # Continuar; si falla el insert se manejará en el except del loop
            pass

        # 1. Obtener todas las combinaciones únicas de proveedor/material en compras
        query_pares = select(
            Compra.codigo_proveedor,
            Compra.numero_material
        ).where(
            Compra.codigo_proveedor.isnot(None),
            Compra.numero_material.isnot(None),
            Compra.numero_material != ''
        ).group_by(Compra.codigo_proveedor, Compra.numero_material)
        
        result = await db.execute(query_pares)
        pares_en_compras = result.all()
        
        total_encontrados = len(pares_en_compras)
        
        # 2. Obtener todas las combinaciones existentes para comparación rápida (numero_material normalizado)
        query_existentes = select(
            PaisOrigenMaterial.codigo_proveedor,
            PaisOrigenMaterial.numero_material
        )
        result_existentes = await db.execute(query_existentes)
        existentes = {
            (str(row[0]).strip(), _normalizar_numero_material(row[1]))
            for row in result_existentes.all()
            if row[0] and row[1]
        }
        
        # 2b. Índice numero_material (FK) por número normalizado; solo escalares para evitar MissingGreenlet tras commit
        query_materiales = select(Material.numero_material)
        result_materiales = await db.execute(query_materiales)
        material_numero_by_normalized: Dict[str, str] = {}
        for row in result_materiales.all():
            if row[0]:
                material_numero_by_normalized[_normalizar_numero_material(row[0])] = row[0]
        
        # 3. Para cada par, verificar si existe y crear si no
        for codigo_proveedor, numero_material_raw in pares_en_compras:
            if not codigo_proveedor or not numero_material_raw:
                continue
            
            codigo_proveedor = str(codigo_proveedor).strip()
            numero_material = _normalizar_numero_material(numero_material_raw)
            
            if not codigo_proveedor or not numero_material:
                continue
            
            if (codigo_proveedor, numero_material) in existentes:
                continue
            
            # La FK exige que el material exista: buscar por número normalizado o crearlo desde compras
            numero_material_fk = material_numero_by_normalized.get(numero_material)
            if not numero_material_fk:
                material_existente = await get_material_by_numero(db, numero_material)
                if material_existente:
                    numero_material_fk = material_existente.numero_material
                    material_numero_by_normalized[numero_material] = numero_material_fk
            if not numero_material_fk:
                # Obtener descripción desde compras (puede estar como raw o normalizado)
                descripcion = None
                try:
                    codigo_int = int(codigo_proveedor)
                except (TypeError, ValueError):
                    codigo_int = None
                if codigo_int is not None:
                    q_desc = select(Compra.descripcion_material).where(
                        Compra.codigo_proveedor == codigo_int,
                        or_(
                            Compra.numero_material == numero_material_raw,
                            Compra.numero_material == numero_material,
                        ),
                        Compra.descripcion_material.isnot(None),
                    ).limit(1)
                    r_desc = await db.execute(q_desc)
                    row_desc = r_desc.scalar_one_or_none()
                    if row_desc:
                        descripcion = row_desc
                try:
                    await create_material(
                        db, numero_material, descripcion_material=descripcion, user_id=user_id
                    )
                    material_numero_by_normalized[numero_material] = numero_material
                    numero_material_fk = numero_material
                except Exception as e_crear:
                    try:
                        await db.rollback()
                    except Exception:
                        pass
                    # Por si ya existía (race condition)
                    material_existente = await get_material_by_numero(db, numero_material)
                    if material_existente:
                        numero_material_fk = material_existente.numero_material
                        material_numero_by_normalized[numero_material] = numero_material_fk
                    else:
                        errores.append(
                            f"Material {numero_material} (proveedor {codigo_proveedor}) no pudo crearse: {e_crear}"
                        )
                        continue
            
            # Usar el numero_material tal como está en la tabla materiales (FK exige coincidencia exacta)
            if not numero_material_fk:
                numero_material_fk = material_numero_by_normalized.get(numero_material, numero_material)
            
            # Verificar nuevamente en la BD antes de crear (por si acaso)
            existente = await get_pais_origen_material_by_proveedor_material(
                db,
                codigo_proveedor,
                numero_material_fk
            )
            if existente:
                existentes.add((codigo_proveedor, numero_material))
                continue
            
            try:
                codigo_int = int(codigo_proveedor)
                pais_origen_material = PaisOrigenMaterial(
                    codigo_proveedor=codigo_int,
                    numero_material=numero_material_fk,
                    pais_origen="Pendiente"
                )
                db.add(pais_origen_material)
                await db.flush()
                pais_origen_id = pais_origen_material.id
                datos_despues = {
                    "id": pais_origen_id,
                    "codigo_proveedor": codigo_int,
                    "numero_material": numero_material_fk,
                    "pais_origen": "Pendiente",
                    "porcentaje_compra": None,
                    "comentario": None,
                    "tipo": None,
                    "created_at": None,
                    "updated_at": None,
                }
                if user_id is not None:
                    historial = PaisOrigenMaterialHistorial(
                        pais_origen_id=pais_origen_id,
                        codigo_proveedor=codigo_int,
                        numero_material=numero_material_fk,
                        operacion=PaisOrigenMaterialOperacion.CREATE,
                        user_id=user_id,
                        datos_antes=None,
                        datos_despues=datos_despues,
                        campos_modificados=None
                    )
                    db.add(historial)
                await db.commit()
                nuevos_creados += 1
                existentes.add((codigo_proveedor, numero_material))
            except Exception as e:
                error_str = str(e)
                try:
                    await db.rollback()
                except Exception:
                    pass
                
                # Si es NULL identity key, asegurar que la secuencia existe y es DEFAULT del id
                is_null_identity = "NULL identity key" in error_str or "has a NULL identity" in error_str
                if is_null_identity:
                    try:
                        await db.execute(text("CREATE SEQUENCE IF NOT EXISTS pais_origen_material_id_seq"))
                        await db.execute(text("""
                            SELECT setval('pais_origen_material_id_seq',
                                COALESCE((SELECT MAX(id) FROM pais_origen_material), 0) + 1,
                                false)
                        """))
                        await db.execute(text("""
                            ALTER TABLE pais_origen_material
                            ALTER COLUMN id SET DEFAULT nextval('pais_origen_material_id_seq'::regclass)
                        """))
                        await db.commit()
                        # Reintentar crear este registro
                        try:
                            codigo_int = int(codigo_proveedor)
                            pais_origen_material = PaisOrigenMaterial(
                                codigo_proveedor=codigo_int,
                                numero_material=numero_material_fk,
                                pais_origen="Pendiente"
                            )
                            db.add(pais_origen_material)
                            await db.flush()
                            po_id = pais_origen_material.id
                            if user_id is not None:
                                historial = PaisOrigenMaterialHistorial(
                                    pais_origen_id=po_id,
                                    codigo_proveedor=codigo_int,
                                    numero_material=numero_material_fk,
                                    operacion=PaisOrigenMaterialOperacion.CREATE,
                                    user_id=user_id,
                                    datos_antes=None,
                                    datos_despues={"id": po_id, "codigo_proveedor": codigo_int, "numero_material": numero_material_fk, "pais_origen": "Pendiente", "porcentaje_compra": None, "comentario": None, "tipo": None, "created_at": None, "updated_at": None},
                                    campos_modificados=None
                                )
                                db.add(historial)
                            await db.commit()
                            nuevos_creados += 1
                            existentes.add((codigo_proveedor, numero_material))
                            continue
                        except Exception as e2:
                            await db.rollback()
                            error_str = str(e2)
                    except Exception as seq_err:
                        await db.rollback()
                
                # Si es un error de secuencia, intentar corregirla
                if "duplicate key value violates unique constraint" in error_str and "pais_origen_material_pkey" in error_str:
                    try:
                        # Obtener el máximo ID actual
                        result = await db.execute(
                            select(func.max(PaisOrigenMaterial.id))
                        )
                        max_id = result.scalar() or 0
                        
                        # Corregir la secuencia
                        await db.execute(
                            text(f"SELECT setval('pais_origen_material_id_seq', {max_id + 1}, false)")
                        )
                        await db.commit()
                        
                        # Intentar crear el registro nuevamente
                        try:
                            codigo_int = int(codigo_proveedor)
                            pais_origen_material = PaisOrigenMaterial(
                                codigo_proveedor=codigo_int,
                                numero_material=numero_material_fk,
                                pais_origen="Pendiente"
                            )
                            db.add(pais_origen_material)
                            await db.flush()
                            po_id = pais_origen_material.id
                            if user_id is not None:
                                historial = PaisOrigenMaterialHistorial(
                                    pais_origen_id=po_id,
                                    codigo_proveedor=codigo_int,
                                    numero_material=numero_material_fk,
                                    operacion=PaisOrigenMaterialOperacion.CREATE,
                                    user_id=user_id,
                                    datos_antes=None,
                                    datos_despues={"id": po_id, "codigo_proveedor": codigo_int, "numero_material": numero_material_fk, "pais_origen": "Pendiente", "porcentaje_compra": None, "comentario": None, "tipo": None, "created_at": None, "updated_at": None},
                                    campos_modificados=None
                                )
                                db.add(historial)
                            await db.commit()
                            nuevos_creados += 1
                            existentes.add((codigo_proveedor, numero_material))
                            continue
                        except Exception as e2:
                            await db.rollback()
                    except Exception as seq_error:
                        await db.rollback()
                
                # Verificar si el registro se creó de todas formas (race condition)
                existente = await get_pais_origen_material_by_proveedor_material(
                    db,
                    codigo_proveedor,
                    numero_material_fk
                )
                if existente:
                    existentes.add((codigo_proveedor, numero_material))
                    continue
                
                error_msg = f"Error al crear país de origen {codigo_proveedor}/{numero_material}: {error_str}"
                errores.append(error_msg)
                continue
        
        return {
            "total_encontrados": total_encontrados,
            "nuevos_creados": nuevos_creados,
            "errores": errores,
            "exitoso": len(errores) == 0
        }
        
    except Exception as e:
        await db.rollback()
        error_msg = f"Error general en sincronización: {str(e)}"
        errores.append(error_msg)
        return {
            "total_encontrados": 0,
            "nuevos_creados": 0,
            "errores": errores,
            "exitoso": False
        }


# ==================== CRUD para Precios Materiales ====================

def _precio_material_to_dict(precio_material: PrecioMaterial) -> Dict[str, Any]:
    """Convierte un objeto PrecioMaterial a diccionario para el historial."""
    return {
        "id": precio_material.id,
        "codigo_proveedor": precio_material.codigo_proveedor,
        "numero_material": precio_material.numero_material,
        "precio": float(precio_material.precio) if precio_material.precio else None,
        "currency_uom": precio_material.currency_uom,
        "country_origin": precio_material.country_origin,
        "Porcentaje_Compra": float(precio_material.Porcentaje_Compra) if precio_material.Porcentaje_Compra else None,
        "Comentario": precio_material.Comentario,
        "updated_at": precio_material.updated_at.isoformat() if precio_material.updated_at else None
    }


async def get_precio_material_by_id(db: AsyncSession, precio_id: int) -> Optional[PrecioMaterial]:
    """Obtiene un precio de material por ID."""
    result = await db.execute(select(PrecioMaterial).where(PrecioMaterial.id == precio_id))
    return result.scalar_one_or_none()


async def get_precio_material_by_proveedor_material(
    db: AsyncSession,
    codigo_proveedor: str,
    numero_material: str
) -> Optional[PrecioMaterial]:
    """Obtiene un precio de material por código de proveedor y número de material."""
    result = await db.execute(
        select(PrecioMaterial).where(
            PrecioMaterial.codigo_proveedor == int(codigo_proveedor),
            PrecioMaterial.numero_material == numero_material
        )
    )
    return result.scalar_one_or_none()


async def create_precio_material(
    db: AsyncSession,
    codigo_proveedor: str,
    numero_material: str,
    precio: Decimal,
    currency_uom: Optional[str] = None,
    country_origin: Optional[str] = None,
    Porcentaje_Compra: Optional[Decimal] = None,
    Comentario: Optional[str] = None,
    user_id: Optional[int] = None,
    _commit: bool = True,
) -> PrecioMaterial:
    """Crea un nuevo precio de material. Si _commit=False (p. ej. desde sincronización masiva), no hace commit."""
    precio_material = PrecioMaterial(
        codigo_proveedor=codigo_proveedor,
        numero_material=numero_material,
        precio=precio,
        currency_uom=currency_uom,
        country_origin=country_origin,
        Porcentaje_Compra=Porcentaje_Compra,
        Comentario=Comentario
    )
    db.add(precio_material)
    await db.flush()  # Flush para obtener el ID sin hacer commit
    await db.refresh(precio_material)
    
    # Registrar en historial
    if user_id is not None:
        historial = PrecioMaterialHistorial(
            precio_material_id=precio_material.id,
            codigo_proveedor=codigo_proveedor,
            numero_material=numero_material,
            operacion=PrecioMaterialOperacion.CREATE,
            user_id=user_id,
            datos_antes=None,
            datos_despues=_precio_material_to_dict(precio_material),
            campos_modificados=None
        )
        db.add(historial)
    
    if _commit:
        await db.commit()
    return precio_material


async def update_precio_material(
    db: AsyncSession,
    precio_id: int,
    precio: Optional[Decimal] = None,
    currency_uom: Optional[str] = None,
    country_origin: Optional[str] = None,
    Porcentaje_Compra: Optional[Decimal] = None,
    Comentario: Optional[str] = None,
    user_id: Optional[int] = None,
    _commit: bool = True,
) -> Optional[PrecioMaterial]:
    """Actualiza un precio de material. Si _commit=False (p. ej. desde sincronización masiva), no hace commit."""
    precio_material = await get_precio_material_by_id(db, precio_id)
    if not precio_material:
        return None
    
    # Guardar datos antes del cambio
    datos_antes = _precio_material_to_dict(precio_material)
    campos_modificados = []
    
    if precio is not None and precio_material.precio != precio:
        precio_material.precio = precio
        campos_modificados.append("precio")
    if currency_uom is not None and precio_material.currency_uom != currency_uom:
        precio_material.currency_uom = currency_uom
        campos_modificados.append("currency_uom")
    if country_origin is not None and precio_material.country_origin != country_origin:
        precio_material.country_origin = country_origin
        campos_modificados.append("country_origin")
    if Porcentaje_Compra is not None and precio_material.Porcentaje_Compra != Porcentaje_Compra:
        precio_material.Porcentaje_Compra = Porcentaje_Compra
        campos_modificados.append("Porcentaje_Compra")
    if Comentario is not None and precio_material.Comentario != Comentario:
        precio_material.Comentario = Comentario
        campos_modificados.append("Comentario")
    
    # Solo registrar en historial si hubo cambios
    if campos_modificados and user_id is not None:
        await db.flush()  # Para obtener los datos actualizados
        await db.refresh(precio_material)
        datos_despues = _precio_material_to_dict(precio_material)
        
        historial = PrecioMaterialHistorial(
            precio_material_id=precio_material.id,
            codigo_proveedor=precio_material.codigo_proveedor,
            numero_material=precio_material.numero_material,
            operacion=PrecioMaterialOperacion.UPDATE,
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=datos_despues,
            campos_modificados=campos_modificados
        )
        db.add(historial)
    
    if _commit:
        await db.commit()
        await db.refresh(precio_material)
    return precio_material


async def upsert_precio_material(
    db: AsyncSession,
    codigo_proveedor: str,
    numero_material: str,
    precio: Decimal,
    currency_uom: Optional[str] = None,
    country_origin: Optional[str] = None,
    Porcentaje_Compra: Optional[Decimal] = None,
    Comentario: Optional[str] = None,
    user_id: Optional[int] = None
) -> PrecioMaterial:
    """Crea o actualiza un precio de material (upsert)."""
    existing = await get_precio_material_by_proveedor_material(db, codigo_proveedor, numero_material)
    
    if existing:
        # Actualizar existente
        return await update_precio_material(
            db,
            precio_id=existing.id,
            precio=precio,
            currency_uom=currency_uom,
            country_origin=country_origin,
            Porcentaje_Compra=Porcentaje_Compra,
            Comentario=Comentario,
            user_id=user_id
        )
    else:
        # Crear nuevo
        return await create_precio_material(
            db,
            codigo_proveedor=codigo_proveedor,
            numero_material=numero_material,
            precio=precio,
            currency_uom=currency_uom,
            country_origin=country_origin,
            Porcentaje_Compra=Porcentaje_Compra,
            Comentario=Comentario,
            user_id=user_id
        )


async def list_precios_materiales(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[PrecioMaterial]:
    """Lista precios de materiales con filtros opcionales."""
    query = select(
        PrecioMaterial,
        Proveedor.nombre.label("proveedor_nombre")
    ).outerjoin(
        Proveedor,
        cast(Proveedor.codigo_proveedor, String) == cast(PrecioMaterial.codigo_proveedor, String)
    ).options(
        selectinload(PrecioMaterial.material)
    )
    
    if codigo_proveedor:
        query = query.where(PrecioMaterial.codigo_proveedor == int(codigo_proveedor))
    
    if numero_material:
        query = query.where(PrecioMaterial.numero_material == numero_material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PrecioMaterial.codigo_proveedor.cast(String).ilike(search_pattern),
                PrecioMaterial.numero_material.ilike(search_pattern),
                PrecioMaterial.currency_uom.ilike(search_pattern),
                PrecioMaterial.country_origin.ilike(search_pattern)
            )
        )
    
    query = query.order_by(desc(PrecioMaterial.updated_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    precios = []
    for precio, proveedor_nombre in result.all():
        # Atributo temporal para la vista, evita resolver relación incompatible por tipo.
        precio.proveedor_nombre = proveedor_nombre
        precios.append(precio)
    return precios


async def count_precios_materiales(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    search: Optional[str] = None
) -> int:
    """Cuenta el total de precios de materiales con filtros opcionales."""
    query = select(func.count(PrecioMaterial.id))
    
    if codigo_proveedor:
        query = query.where(PrecioMaterial.codigo_proveedor == int(codigo_proveedor))
    
    if numero_material:
        query = query.where(PrecioMaterial.numero_material == numero_material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PrecioMaterial.codigo_proveedor.cast(String).ilike(search_pattern),
                PrecioMaterial.numero_material.ilike(search_pattern),
                PrecioMaterial.currency_uom.ilike(search_pattern),
                PrecioMaterial.country_origin.ilike(search_pattern)
            )
        )
    
    result = await db.execute(query)
    return result.scalar_one()


async def sincronizar_precios_materiales_desde_compras(
    db: AsyncSession,
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Sincroniza precios de materiales desde la tabla compras.
    Para cada combinación única de numero_material + codigo_proveedor,
    obtiene el precio de la última compra registrada (más reciente por posting_date)
    y lo guarda o actualiza en precios_materiales.
    
    Returns:
        Dict con estadísticas de la sincronización:
        - total_encontrados: Total de combinaciones únicas en compras
        - nuevos_creados: Cantidad de precios nuevos creados
        - actualizados: Cantidad de precios actualizados
        - errores: Lista de errores encontrados
    """
    errores = []
    nuevos_creados = 0
    actualizados = 0
    
    try:
        # 0. Corregir la secuencia de precios_materiales para evitar errores de clave duplicada
        # (puede ocurrir si hubo inserciones manuales o importaciones con IDs explícitos)
        try:
            result_max_id = await db.execute(select(func.max(PrecioMaterial.id)))
            max_id = result_max_id.scalar() or 0
            await db.execute(
                text(f"SELECT setval('precios_materiales_id_seq', {max_id + 1}, false)")
            )
            await db.commit()
        except Exception as seq_error:
            try:
                await db.rollback()
            except Exception:
                pass
        
        # 1. Obtener todas las combinaciones únicas de proveedor/material con precio válido
        query_pares = select(
            Compra.codigo_proveedor,
            Compra.numero_material
        ).where(
            Compra.codigo_proveedor.isnot(None),
            Compra.numero_material.isnot(None),
            Compra.numero_material != '',
            Compra.price.isnot(None),
            Compra.price > 0
        ).group_by(Compra.codigo_proveedor, Compra.numero_material)
        
        result = await db.execute(query_pares)
        pares_en_compras = result.all()
        
        total_encontrados = len(pares_en_compras)
        
        # 2. Para cada par, obtener el precio de la última compra (más reciente)
        for codigo_proveedor, numero_material in pares_en_compras:
            if not codigo_proveedor or not numero_material:
                continue
            
            codigo_proveedor = str(codigo_proveedor).strip()
            numero_material = _normalizar_numero_material(numero_material)
            if numero_material:
                numero_material = numero_material.strip()
            
            if not codigo_proveedor or not numero_material:
                continue
            
            try:
                # Obtener la última compra registrada para esta combinación
                # Ordenamos por posting_date descendente y luego por id para obtener la más reciente
                query_ultima_compra = select(
                    Compra.price,
                    Compra.currency,
                    Compra.order_unit
                ).where(
                    Compra.codigo_proveedor == codigo_proveedor,
                    Compra.numero_material == numero_material,
                    Compra.price.isnot(None),
                    Compra.price > 0
                ).order_by(desc(Compra.posting_date), desc(Compra.id)).limit(1)
                
                result_compra = await db.execute(query_ultima_compra)
                compra_row = result_compra.first()
                
                if not compra_row or not compra_row[0]:
                    continue
                
                # Precio de la última compra registrada para esta combinación
                precio_valor = Decimal(str(compra_row[0]))
                currency = compra_row[1] if compra_row[1] else None
                order_unit = compra_row[2] if compra_row[2] else None
                
                # Construir currency_uom si tenemos ambos
                currency_uom = None
                if currency and order_unit:
                    currency_uom = f"{currency}/{order_unit}"
                elif currency:
                    currency_uom = currency
                elif order_unit:
                    currency_uom = order_unit
                
                # Verificar si ya existe un precio para esta combinación
                precio_existente = await get_precio_material_by_proveedor_material(
                    db,
                    codigo_proveedor,
                    numero_material
                )
                # Capturar id y precio como escalares de inmediato (evita MissingGreenlet al tocar el objeto tras commit)
                precio_id_val = precio_existente.id if precio_existente else None
                precio_actual_val = float(precio_existente.precio) if (precio_existente and precio_existente.precio is not None) else None
                
                if precio_id_val is not None:
                    # Actualizar solo si el precio es diferente (precio de la última compra; incluye cuando actual es None)
                    if precio_actual_val is None or precio_actual_val != float(precio_valor):
                        await update_precio_material(
                            db,
                            precio_id=precio_id_val,
                            precio=precio_valor,
                            currency_uom=currency_uom,
                            user_id=user_id,
                            _commit=False,
                        )
                        actualizados += 1
                else:
                    # Crear nuevo precio usando el precio de la última compra registrada
                    await create_precio_material(
                        db,
                        codigo_proveedor=codigo_proveedor,
                        numero_material=numero_material,
                        precio=precio_valor,
                        currency_uom=currency_uom,
                        user_id=user_id,
                        _commit=False,
                    )
                    nuevos_creados += 1
                    
            except Exception as e:
                error_str = str(e)
                try:
                    await db.rollback()
                except Exception:
                    pass
                
                error_msg = f"Error al procesar precio {codigo_proveedor}/{numero_material}: {error_str}"
                errores.append(error_msg)
                continue
        
        # Un solo commit al final (evita MissingGreenlet por acceder objetos tras commit en el bucle)
        try:
            await db.commit()
        except Exception as e_commit:
            await db.rollback()
            errores.append(f"Error al confirmar cambios: {str(e_commit)}")
        return {
            "total_encontrados": total_encontrados,
            "nuevos_creados": nuevos_creados,
            "actualizados": actualizados,
            "errores": errores,
            "exitoso": len(errores) == 0
        }
        
    except Exception as e:
        await db.rollback()
        error_msg = f"Error general en sincronización: {str(e)}"
        errores.append(error_msg)
        return {
            "total_encontrados": 0,
            "nuevos_creados": 0,
            "actualizados": 0,
            "errores": errores,
            "exitoso": False
        }


async def actualizar_pais_origen_en_precios_materiales(
    db: AsyncSession,
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Actualiza el campo country_origin en precios_materiales usando datos de pais_origen_material.
    Busca todos los registros en precios_materiales donde country_origin esté vacío o sea null,
    y para cada uno busca en pais_origen_material usando codigo_proveedor y numero_material
    para obtener el pais_origen correspondiente.
    
    Returns:
        Dict con estadísticas de la actualización:
        - total_procesados: Total de registros procesados
        - actualizados: Cantidad de registros actualizados
        - no_encontrados: Cantidad de registros sin país de origen en la tabla pais_origen_material
        - errores: Lista de errores encontrados
    """
    errores = []
    actualizados = 0
    no_encontrados = 0
    
    try:
        # Primero, corregir la secuencia de precios_materiales_historial para evitar errores de clave duplicada
        try:
            result_max_id = await db.execute(
                select(func.max(PrecioMaterialHistorial.id))
            )
            max_id = result_max_id.scalar() or 0
            await db.execute(
                text(f"SELECT setval('precios_materiales_historial_id_seq', {max_id + 1}, false)")
            )
            await db.commit()
        except Exception as seq_error:
            # Si falla la corrección de secuencia, intentar continuar de todas formas
            try:
                await db.rollback()
            except Exception:
                pass
        
        # 1. Obtener todos los precios sin país de origen (NULL o cadena vacía)
        # Obtenemos solo los IDs para luego procesar uno por uno con una sesión limpia
        query_sin_pais = select(
            PrecioMaterial.id,
            PrecioMaterial.codigo_proveedor,
            PrecioMaterial.numero_material
        ).where(
            or_(
                PrecioMaterial.country_origin.is_(None),
                PrecioMaterial.country_origin == ''
            )
        )
        
        result = await db.execute(query_sin_pais)
        precios_sin_pais = result.all()
        
        total_procesados = len(precios_sin_pais)
        
        # 2. Para cada precio, buscar el país de origen en pais_origen_material
        for precio_id, codigo_proveedor, numero_material in precios_sin_pais:
            if not codigo_proveedor or not numero_material:
                continue
            
            try:
                # Buscar en pais_origen_material
                result_pais = await db.execute(
                    select(PaisOrigenMaterial.pais_origen).where(
                        PaisOrigenMaterial.codigo_proveedor == int(codigo_proveedor),
                        PaisOrigenMaterial.numero_material == str(numero_material)
                    )
                )
                pais_origen_valor = result_pais.scalar_one_or_none()
                
                if pais_origen_valor and pais_origen_valor != "Pendiente":
                    # Obtener el precio actual para guardar datos antes
                    result_precio = await db.execute(
                        select(PrecioMaterial).where(PrecioMaterial.id == precio_id)
                    )
                    precio = result_precio.scalar_one_or_none()
                    
                    if precio:
                        # Guardar datos antes del cambio
                        datos_antes = _precio_material_to_dict(precio)
                        
                        # Actualizar el país de origen directamente con UPDATE
                        await db.execute(
                            text("""
                                UPDATE precios_materiales 
                                SET country_origin = :pais_origen, updated_at = NOW()
                                WHERE id = :precio_id
                            """),
                            {"pais_origen": pais_origen_valor, "precio_id": precio_id}
                        )
                        
                        # Registrar en historial si se proporciona user_id
                        if user_id is not None:
                            # Construir datos_despues manualmente
                            datos_despues = datos_antes.copy()
                            datos_despues["country_origin"] = pais_origen_valor
                            
                            await db.execute(
                                text("""
                                    INSERT INTO precios_materiales_historial 
                                    (precio_material_id, codigo_proveedor, numero_material, operacion, user_id, datos_antes, datos_despues, campos_modificados)
                                    VALUES (:precio_id, :codigo_proveedor, :numero_material, 'UPDATE', :user_id, :datos_antes, :datos_despues, :campos_modificados)
                                """),
                                {
                                    "precio_id": precio_id,
                                    "codigo_proveedor": codigo_proveedor,
                                    "numero_material": numero_material,
                                    "user_id": user_id,
                                    "datos_antes": json.dumps(datos_antes),
                                    "datos_despues": json.dumps(datos_despues),
                                    "campos_modificados": json.dumps(["country_origin"])
                                }
                            )
                        
                        await db.commit()
                        actualizados += 1
                else:
                    no_encontrados += 1
                    
            except Exception as e:
                error_str = str(e)
                try:
                    await db.rollback()
                except Exception:
                    pass
                
                # Solo agregar error si no es un error de secuencia que ya manejamos
                if "duplicate key" not in error_str.lower():
                    error_msg = f"Error al actualizar país de origen para {codigo_proveedor}/{numero_material}: {error_str}"
                    errores.append(error_msg)
                continue
        
        return {
            "total_procesados": total_procesados,
            "actualizados": actualizados,
            "no_encontrados": no_encontrados,
            "errores": errores,
            "exitoso": len(errores) == 0 or actualizados > 0
        }
        
    except Exception as e:
        try:
            await db.rollback()
        except Exception:
            pass
        error_msg = f"Error general en actualización de países de origen: {str(e)}"
        errores.append(error_msg)
        return {
            "total_procesados": 0,
            "actualizados": 0,
            "no_encontrados": 0,
            "errores": errores,
            "exitoso": False
        }


async def delete_precio_material(
    db: AsyncSession,
    precio_id: int,
    user_id: Optional[int] = None
) -> bool:
    """Elimina un precio de material."""
    from sqlalchemy import delete
    
    precio_material = await get_precio_material_by_id(db, precio_id)
    if not precio_material:
        return False
    
    # Guardar datos antes de eliminar
    datos_antes = _precio_material_to_dict(precio_material)
    codigo_proveedor = precio_material.codigo_proveedor
    numero_material = precio_material.numero_material
    
    stmt = delete(PrecioMaterial).where(PrecioMaterial.id == precio_id)
    await db.execute(stmt)
    
    # Registrar en historial
    if user_id is not None:
        historial = PrecioMaterialHistorial(
            precio_material_id=precio_id,
            codigo_proveedor=codigo_proveedor,
            numero_material=numero_material,
            operacion=PrecioMaterialOperacion.DELETE,
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=None,
            campos_modificados=None
        )
        db.add(historial)
    
    await db.commit()
    return True


# ==================== CRUD para Historial de Precios Materiales ====================

async def list_precio_material_historial(
    db: AsyncSession,
    precio_material_id: Optional[int] = None,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    operacion: Optional[PrecioMaterialOperacion] = None,
    user_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0
) -> List[PrecioMaterialHistorial]:
    """Lista el historial de cambios en precios de materiales con filtros opcionales."""
    query = select(PrecioMaterialHistorial).options(selectinload(PrecioMaterialHistorial.user))
    
    if precio_material_id:
        query = query.where(PrecioMaterialHistorial.precio_material_id == precio_material_id)
    
    if codigo_proveedor:
        query = query.where(PrecioMaterialHistorial.codigo_proveedor == int(codigo_proveedor))
    
    if numero_material:
        query = query.where(PrecioMaterialHistorial.numero_material == numero_material)
    
    if operacion:
        query = query.where(PrecioMaterialHistorial.operacion == operacion)
    
    if user_id:
        query = query.where(PrecioMaterialHistorial.user_id == user_id)
    
    query = query.order_by(desc(PrecioMaterialHistorial.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_precio_material_historial_by_id(
    db: AsyncSession,
    historial_id: int
) -> Optional[PrecioMaterialHistorial]:
    """Obtiene un registro del historial por ID."""
    result = await db.execute(
        select(PrecioMaterialHistorial).where(PrecioMaterialHistorial.id == historial_id)
    )
    return result.scalar_one_or_none()


async def count_precio_material_historial(
    db: AsyncSession,
    precio_material_id: Optional[int] = None,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    operacion: Optional[PrecioMaterialOperacion] = None,
    user_id: Optional[int] = None
) -> int:
    """Cuenta el total de registros en el historial con filtros opcionales."""
    query = select(func.count(PrecioMaterialHistorial.id))
    
    if precio_material_id:
        query = query.where(PrecioMaterialHistorial.precio_material_id == precio_material_id)
    
    if codigo_proveedor:
        query = query.where(PrecioMaterialHistorial.codigo_proveedor == int(codigo_proveedor))
    
    if numero_material:
        query = query.where(PrecioMaterialHistorial.numero_material == numero_material)
    
    if operacion:
        query = query.where(PrecioMaterialHistorial.operacion == operacion)
    
    if user_id:
        query = query.where(PrecioMaterialHistorial.user_id == user_id)
    
    result = await db.execute(query)
    return result.scalar() or 0


# ==================== CRUD para Compra ====================

async def create_compra(
    db: AsyncSession,
    purchasing_document: Optional[int] = None,
    item: Optional[int] = None,
    material_doc_year: Optional[int] = None,
    material_document: Optional[int] = None,
    material_doc_item: Optional[int] = None,
    movement_type: Optional[str] = None,
    posting_date: Optional[datetime] = None,
    quantity: Optional[int] = None,
    order_unit: Optional[str] = None,
    quantity_in_opun: Optional[int] = None,
    order_price_unit: Optional[str] = None,
    amount_in_lc: Optional[Decimal] = None,
    local_currency: Optional[str] = None,
    amount: Optional[Decimal] = None,
    currency: Optional[str] = None,
    gr_ir_clearing_value_lc: Optional[Decimal] = None,
    invoice_value: Optional[Decimal] = None,
    numero_material: Optional[str] = None,
    plant: Optional[str] = None,
    descripcion_material: Optional[str] = None,
    nombre_proveedor: Optional[str] = None,
    codigo_proveedor: Optional[str] = None,
    price: Optional[Decimal] = None,
) -> Compra:
    """Crea un nuevo registro de compra. numero_material se guarda sin notación científica."""
    # Si viene número (o notación científica), se guarda normalizado; None solo cuando la entrada era vacía/None
    if numero_material:
        numero_material_final = _normalizar_numero_material(numero_material) or None
    else:
        numero_material_final = None
    db_compra = Compra(
        purchasing_document=purchasing_document,
        item=item,
        material_doc_year=material_doc_year,
        material_document=material_document,
        material_doc_item=material_doc_item,
        movement_type=movement_type,
        posting_date=posting_date,
        quantity=quantity,
        order_unit=order_unit,
        quantity_in_opun=quantity_in_opun,
        order_price_unit=order_price_unit,
        amount_in_lc=amount_in_lc,
        local_currency=local_currency,
        amount=amount,
        currency=currency,
        gr_ir_clearing_value_lc=gr_ir_clearing_value_lc,
        invoice_value=invoice_value,
        numero_material=numero_material_final,
        plant=plant,
        descripcion_material=descripcion_material,
        nombre_proveedor=nombre_proveedor,
        codigo_proveedor=codigo_proveedor,
        price=price,
    )
    db.add(db_compra)
    await db.commit()
    await db.refresh(db_compra)
    return db_compra


async def get_compra_by_material_document(
    db: AsyncSession,
    material_document: Optional[int],
    material_doc_item: Optional[int]
) -> Optional[Compra]:
    """Busca una compra por material_document y material_doc_item (identificadores únicos)."""
    if material_document is None or material_doc_item is None:
        return None
    
    query = select(Compra).where(
        Compra.material_document == material_document,
        Compra.material_doc_item == material_doc_item
    )
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def get_compra_by_purchasing_and_material(
    db: AsyncSession,
    purchasing_document: Optional[int],
    numero_material: Optional[str],
    item: Optional[int] = None
) -> Optional[Compra]:
    """Busca una compra por purchasing_document, item y numero_material (fallback)."""
    if purchasing_document is None or numero_material is None:
        return None
    
    query = select(Compra).where(
        Compra.purchasing_document == purchasing_document,
        Compra.numero_material == numero_material
    )
    
    # Si se proporciona item, también filtrar por item para mayor precisión
    if item is not None:
        query = query.where(Compra.item == item)
    
    result = await db.execute(query)
    # Si hay múltiples resultados, tomar el primero
    try:
        return result.scalar_one_or_none()
    except Exception:
        # Si hay múltiples resultados, tomar el primero
        return result.scalars().first()


# Columnas de Compra que se pueden actualizar (no id ni created_at)
_COMPRA_UPDATABLE_KEYS = (
    'purchasing_document', 'item', 'material_doc_year', 'material_document',
    'material_doc_item', 'movement_type', 'posting_date', 'quantity', 'order_unit',
    'quantity_in_opun', 'order_price_unit', 'amount_in_lc', 'local_currency',
    'amount', 'currency', 'gr_ir_clearing_value_lc', 'invoice_value',
    'numero_material', 'plant', 'descripcion_material', 'nombre_proveedor',
    'codigo_proveedor', 'price',
)


async def bulk_create_or_update_compras(
    db: AsyncSession,
    compras_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Inserta o actualiza registros de compras.
    Si el registro ya existe (por material_document+material_doc_item o purchasing_document+numero_material+item),
    se actualiza con los nuevos datos. Si no existe, se inserta.
    
    Prioridad de búsqueda para considerar duplicado:
    1. material_document + material_doc_item (más preciso)
    2. purchasing_document + item + numero_material (fallback)
    
    Args:
        db: Sesión de base de datos
        compras_data: Lista de diccionarios con los datos de las compras
        
    Returns:
        Diccionario con 'insertados', 'actualizados', 'duplicados' (por compatibilidad, siempre 0) y 'errores'
    """
    if not compras_data:
        return {"insertados": 0, "actualizados": 0, "duplicados": 0, "errores": []}
    
    # Ajustar secuencia de compras.id para que el próximo id sea MAX(id)+1 (evita UniqueViolationError)
    try:
        await db.execute(text(
            "SELECT setval(pg_get_serial_sequence('compras', 'id'), COALESCE((SELECT MAX(id) FROM compras), 0))"
        ))
    except Exception:
        pass  # Si falla (ej. tabla sin secuencia), seguir con el insert
    
    insertados = 0
    actualizados = 0
    errores = []
    
    # Columnas enteras/númericas de Compra: omitir inf/-inf/NaN para evitar "cannot convert float infinity to integer"
    _compra_int_keys = (
        'purchasing_document', 'item', 'material_doc_year', 'material_document',
        'material_doc_item', 'quantity', 'quantity_in_opun', 'codigo_proveedor',
    )
    _compra_numeric_keys = ('amount_in_lc', 'amount', 'gr_ir_clearing_value_lc', 'invoice_value', 'price')

    def _sanitize_compra_row(data: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(data)
        for k in _compra_int_keys:
            v = out.get(k)
            if v is None:
                continue
            try:
                if isinstance(v, float) and not math.isfinite(v):
                    out[k] = None
                elif isinstance(v, float) and (v != v):  # NaN
                    out[k] = None
            except (TypeError, ValueError):
                pass
        for k in _compra_numeric_keys:
            v = out.get(k)
            if v is None:
                continue
            try:
                if isinstance(v, float) and not math.isfinite(v):
                    out[k] = None
                elif isinstance(v, Decimal) and v in (Decimal('Infinity'), Decimal('-Infinity'), Decimal('NaN')):
                    out[k] = None
            except (TypeError, ValueError):
                pass
        return out

    for idx, compra_data in enumerate(compras_data, start=1):
        try:
            compra_data = _sanitize_compra_row(compra_data)
            # Normalizar numero_material para no guardar notación científica
            raw_num = compra_data.get('numero_material')
            if raw_num is not None:
                n = _normalizar_numero_material(raw_num)
                compra_data = {**compra_data, 'numero_material': (n if n else None)}
            material_doc = compra_data.get('material_document')
            material_doc_item = compra_data.get('material_doc_item')
            purchasing_doc = compra_data.get('purchasing_document')
            numero_mat = compra_data.get('numero_material')
            item = compra_data.get('item')
            
            compra_existente = None
            
            if material_doc is not None and material_doc_item is not None:
                compra_existente = await get_compra_by_material_document(
                    db, material_doc, material_doc_item
                )
            
            if compra_existente is None and purchasing_doc is not None and numero_mat is not None:
                compra_existente = await get_compra_by_purchasing_and_material(
                    db, purchasing_doc, numero_mat, item
                )
            
            if compra_existente:
                # Ya existe: actualizar con los nuevos datos del archivo
                datos_insercion = {k: v for k, v in compra_data.items()
                                  if k in _COMPRA_UPDATABLE_KEYS}
                for k, v in datos_insercion.items():
                    setattr(compra_existente, k, v)
                compra_existente.updated_at = datetime.now(timezone.utc)
                actualizados += 1
            else:
                # No pasar id, created_at, updated_at: que la BD los genere (evita violación de PK)
                datos_insercion = {k: v for k, v in compra_data.items()
                                  if k not in ('id', 'created_at', 'updated_at')}
                compra = Compra(**datos_insercion)
                db.add(compra)
                insertados += 1
        except Exception as e:
            error_str = str(e)
            errores.append({
                "fila": idx,
                "error": error_str,
                "material_document": compra_data.get('material_document'),
                "numero_material": compra_data.get('numero_material'),
            })
    
    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise
    
    return {"insertados": insertados, "actualizados": actualizados, "duplicados": 0, "errores": errores}


async def bulk_create_compras(
    db: AsyncSession,
    compras_data: List[Dict[str, Any]]
) -> int:
    """Inserta múltiples registros de compras de manera eficiente.
    
    Args:
        db: Sesión de base de datos
        compras_data: Lista de diccionarios con los datos de las compras
        
    Returns:
        Número de registros insertados
    """
    if not compras_data:
        return 0
    
    compras_objects = []
    for compra_data in compras_data:
        datos_insercion = {k: v for k, v in compra_data.items()
                          if k not in ('id', 'created_at', 'updated_at')}
        if datos_insercion.get('numero_material') is not None:
            n = _normalizar_numero_material(datos_insercion['numero_material'])
            datos_insercion['numero_material'] = n if n else None
        compra = Compra(**datos_insercion)
        compras_objects.append(compra)
    
    db.add_all(compras_objects)
    await db.commit()
    
    return len(compras_objects)


async def list_compras(
    db: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    search: Optional[str] = None,
    fecha_inicio: Optional[datetime] = None,
    fecha_fin: Optional[datetime] = None,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    purchasing_document: Optional[int] = None,
    material_document: Optional[int] = None
) -> List[Compra]:
    """Lista compras con paginación y filtros."""
    query = select(Compra)
    
    # Aplicar filtros
    conditions = []
    
    if search:
        search_pattern = f"%{search.lower()}%"
        conditions.append(
            or_(
                Compra.numero_material.ilike(search_pattern),
                Compra.descripcion_material.ilike(search_pattern),
                Compra.nombre_proveedor.ilike(search_pattern),
                Compra.purchasing_document.cast(String).ilike(search_pattern),
                Compra.material_document.cast(String).ilike(search_pattern),
                Compra.movement_type.ilike(search_pattern),
                Compra.currency.ilike(search_pattern),
                Compra.local_currency.ilike(search_pattern)
            )
        )
    
    if fecha_inicio:
        conditions.append(Compra.posting_date >= fecha_inicio)
    
    if fecha_fin:
        # Asegurar que incluya todo el día
        fecha_fin_con_hora = fecha_fin.replace(hour=23, minute=59, second=59)
        conditions.append(Compra.posting_date <= fecha_fin_con_hora)
    
    if codigo_proveedor:
        conditions.append(Compra.codigo_proveedor == int(codigo_proveedor))
    
    if numero_material:
        conditions.append(Compra.numero_material.ilike(f"%{numero_material}%"))
    
    if purchasing_document:
        conditions.append(Compra.purchasing_document == purchasing_document)
    
    if material_document:
        conditions.append(Compra.material_document == material_document)
    
    if conditions:
        query = query.where(*conditions)
    
    query = query.order_by(desc(Compra.posting_date), desc(Compra.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_compras(
    db: AsyncSession,
    search: Optional[str] = None,
    fecha_inicio: Optional[datetime] = None,
    fecha_fin: Optional[datetime] = None,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    purchasing_document: Optional[int] = None,
    material_document: Optional[int] = None
) -> int:
    """Cuenta el total de compras con filtros."""
    query = select(func.count(Compra.id))
    
    # Aplicar los mismos filtros que en list_compras
    conditions = []
    
    if search:
        search_pattern = f"%{search.lower()}%"
        conditions.append(
            or_(
                Compra.numero_material.ilike(search_pattern),
                Compra.descripcion_material.ilike(search_pattern),
                Compra.nombre_proveedor.ilike(search_pattern),
                Compra.purchasing_document.cast(String).ilike(search_pattern),
                Compra.material_document.cast(String).ilike(search_pattern),
                Compra.movement_type.ilike(search_pattern),
                Compra.currency.ilike(search_pattern),
                Compra.local_currency.ilike(search_pattern)
            )
        )
    
    if fecha_inicio:
        conditions.append(Compra.posting_date >= fecha_inicio)
    
    if fecha_fin:
        fecha_fin_con_hora = fecha_fin.replace(hour=23, minute=59, second=59)
        conditions.append(Compra.posting_date <= fecha_fin_con_hora)
    
    if codigo_proveedor:
        conditions.append(Compra.codigo_proveedor == int(codigo_proveedor))
    
    if numero_material:
        conditions.append(Compra.numero_material.ilike(f"%{numero_material}%"))
    
    if purchasing_document:
        conditions.append(Compra.purchasing_document == purchasing_document)
    
    if material_document:
        conditions.append(Compra.material_document == material_document)
    
    if conditions:
        query = query.where(*conditions)
    
    result = await db.execute(query)
    return result.scalar() or 0


# ==================== CRUD para PaisOrigenMaterial ====================

# Sentinels para "no enviado" en update (no modificar el campo)
_COMENTARIO_SENTINEL = object()
_TIPO_SENTINEL = object()


def _pais_origen_material_to_dict(pais_origen_material: PaisOrigenMaterial) -> Dict[str, Any]:
    """Convierte un objeto PaisOrigenMaterial a diccionario para el historial."""
    return {
        "id": pais_origen_material.id,
        "codigo_proveedor": pais_origen_material.codigo_proveedor,
        "numero_material": pais_origen_material.numero_material,
        "pais_origen": pais_origen_material.pais_origen,
        "porcentaje_compra": float(pais_origen_material.porcentaje_compra) if pais_origen_material.porcentaje_compra is not None else None,
        "comentario": pais_origen_material.comentario,
        "tipo": pais_origen_material.tipo,
        "created_at": pais_origen_material.created_at.isoformat() if pais_origen_material.created_at else None,
        "updated_at": pais_origen_material.updated_at.isoformat() if pais_origen_material.updated_at else None
    }


async def get_pais_origen_material_by_id(db: AsyncSession, pais_id: int) -> Optional[PaisOrigenMaterial]:
    """Obtiene un país de origen por ID."""
    result = await db.execute(select(PaisOrigenMaterial).where(PaisOrigenMaterial.id == pais_id))
    return result.scalar_one_or_none()


async def get_pais_origen_material_by_proveedor_material(
    db: AsyncSession,
    codigo_proveedor: str,
    numero_material: str
) -> Optional[PaisOrigenMaterial]:
    """Obtiene un país de origen por código de proveedor y número de material."""
    result = await db.execute(
        select(PaisOrigenMaterial).where(
            PaisOrigenMaterial.codigo_proveedor == int(codigo_proveedor),
            PaisOrigenMaterial.numero_material == str(numero_material)
        )
    )
    return result.scalar_one_or_none()


async def create_pais_origen_material(
    db: AsyncSession,
    codigo_proveedor: str,
    numero_material: str,
    pais_origen: str,
    porcentaje_compra: Optional[float] = None,
    comentario: Optional[str] = None,
    tipo: Optional[str] = None,
    user_id: Optional[int] = None
) -> PaisOrigenMaterial:
    """Crea un nuevo país de origen de material."""
    pais_origen_material = PaisOrigenMaterial(
        codigo_proveedor=codigo_proveedor,
        numero_material=numero_material,
        pais_origen=pais_origen,
        porcentaje_compra=porcentaje_compra,
        comentario=comentario,
        tipo=tipo,
    )
    db.add(pais_origen_material)
    await db.flush()  # Flush para obtener el ID sin hacer commit
    await db.refresh(pais_origen_material)
    
    # Registrar en historial
    if user_id is not None:
        historial = PaisOrigenMaterialHistorial(
            pais_origen_id=pais_origen_material.id,
            codigo_proveedor=codigo_proveedor,
            numero_material=numero_material,
            operacion=PaisOrigenMaterialOperacion.CREATE,
            user_id=user_id,
            datos_antes=None,
            datos_despues=_pais_origen_material_to_dict(pais_origen_material),
            campos_modificados=None
        )
        db.add(historial)
    
    await db.commit()
    return pais_origen_material


async def update_pais_origen_material(
    db: AsyncSession,
    pais_id: int,
    pais_origen: Optional[str] = None,
    porcentaje_compra: Optional[float] = None,
    comentario: Optional[str] = _COMENTARIO_SENTINEL,
    tipo: Optional[str] = _TIPO_SENTINEL,
    user_id: Optional[int] = None
) -> Optional[PaisOrigenMaterial]:
    """Actualiza un país de origen de material."""
    pais_origen_material = await get_pais_origen_material_by_id(db, pais_id)
    if not pais_origen_material:
        return None
    
    # Guardar datos antes del cambio
    datos_antes = _pais_origen_material_to_dict(pais_origen_material)
    campos_modificados = []
    
    if pais_origen is not None and pais_origen_material.pais_origen != pais_origen:
        pais_origen_material.pais_origen = pais_origen
        campos_modificados.append("pais_origen")
    
    if porcentaje_compra is not None and pais_origen_material.porcentaje_compra != porcentaje_compra:
        pais_origen_material.porcentaje_compra = porcentaje_compra
        campos_modificados.append("porcentaje_compra")
    
    # comentario: solo actualizar si se pasó el parámetro (permite borrar enviando None o "")
    if comentario is not _COMENTARIO_SENTINEL and pais_origen_material.comentario != comentario:
        pais_origen_material.comentario = comentario
        campos_modificados.append("comentario")
    
    if tipo is not _TIPO_SENTINEL and pais_origen_material.tipo != tipo:
        pais_origen_material.tipo = tipo
        campos_modificados.append("tipo")
    
    # Solo registrar en historial si hubo cambios
    if campos_modificados and user_id is not None:
        await db.flush()  # Para obtener los datos actualizados
        await db.refresh(pais_origen_material)
        datos_despues = _pais_origen_material_to_dict(pais_origen_material)
        
        historial = PaisOrigenMaterialHistorial(
            pais_origen_id=pais_origen_material.id,
            codigo_proveedor=pais_origen_material.codigo_proveedor,
            numero_material=pais_origen_material.numero_material,
            operacion=PaisOrigenMaterialOperacion.UPDATE,
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=datos_despues,
            campos_modificados=campos_modificados
        )
        db.add(historial)
    
    await db.commit()
    await db.refresh(pais_origen_material)
    return pais_origen_material


async def upsert_pais_origen_material(
    db: AsyncSession,
    codigo_proveedor: str,
    numero_material: str,
    pais_origen: str,
    porcentaje_compra: Optional[float] = None,
    comentario: Optional[str] = None,
    tipo: Optional[str] = None,
    user_id: Optional[int] = None
) -> PaisOrigenMaterial:
    """Crea o actualiza un país de origen de material (upsert)."""
    existing = await get_pais_origen_material_by_proveedor_material(db, codigo_proveedor, numero_material)
    
    if existing:
        # Actualizar existente
        return await update_pais_origen_material(
            db,
            pais_id=existing.id,
            pais_origen=pais_origen,
            porcentaje_compra=porcentaje_compra,
            comentario=comentario,
            tipo=tipo,
            user_id=user_id
        )
    else:
        # Crear nuevo
        return await create_pais_origen_material(
            db,
            codigo_proveedor=codigo_proveedor,
            numero_material=numero_material,
            pais_origen=pais_origen,
            porcentaje_compra=porcentaje_compra,
            comentario=comentario,
            tipo=tipo,
            user_id=user_id
        )


async def actualizar_porcentaje_compra_desde_filas(
    db: AsyncSession,
    filas: List[Dict[str, Any]],
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Actualiza porcentaje_compra en pais_origen_material a partir de una lista de filas.
    Cada fila debe tener: codigo_proveedor (int o str), numero_material (str), porcentaje_compra (float o None).
    numero_material se normaliza con _normalizar_numero_material antes de buscar.
    Retorna: actualizados, no_encontrados, errores.
    """
    resultado: Dict[str, Any] = {
        "actualizados": 0,
        "no_encontrados": 0,
        "errores": []
    }
    for fila in filas:
        try:
            codigo_proveedor = fila.get("codigo_proveedor")
            numero_material_raw = fila.get("numero_material")
            porcentaje_compra = fila.get("porcentaje_compra")
            if codigo_proveedor is None or numero_material_raw is None:
                continue
            if porcentaje_compra is not None:
                try:
                    porcentaje_compra = float(porcentaje_compra)
                except (TypeError, ValueError):
                    porcentaje_compra = None
            codigo_proveedor = int(codigo_proveedor)
            numero_material = _normalizar_numero_material(numero_material_raw)
            if not numero_material:
                continue
            pais = await get_pais_origen_material_by_proveedor_material(
                db, str(codigo_proveedor), numero_material
            )
            if not pais:
                resultado["no_encontrados"] += 1
                continue
            await update_pais_origen_material(
                db,
                pais_id=pais.id,
                pais_origen=None,
                porcentaje_compra=porcentaje_compra,
                user_id=user_id
            )
            resultado["actualizados"] += 1
        except Exception as e:
            resultado["errores"].append(f"{fila.get('codigo_proveedor')}/{fila.get('numero_material')}: {str(e)}")
    return resultado


async def actualizar_porcentaje_compra_ultimos_5_anios(
    db: AsyncSession,
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Calcula y actualiza porcentaje_compra en pais_origen_material usando las compras
    de los últimos 2 años calendario hacia el presente.

    El periodo se define desde el 1 de enero del año actual-2 hasta el momento actual
    (incluye el año actual en forma parcial).
    Para cada (codigo_proveedor, numero_material) el porcentaje es:
    100 * (suma de quantity_in_opun de ese proveedor/material en el periodo) / (suma total de quantity_in_opun del material en el periodo).
    Solo se actualizan registros que ya existen en pais_origen_material.
    Returns:
        actualizados: cantidad de registros actualizados
        sin_compras_en_periodo: cantidad de registros con 0% por no tener compras en el periodo
        errores: lista de mensajes de error
    """
    resultado: Dict[str, Any] = {
        "actualizados": 0,
        "sin_compras_en_periodo": 0,
        "errores": [],
    }
    try:
        now = datetime.now(timezone.utc)
        año_actual = now.year
        # Desde el 1-ene del año actual-2 hasta el momento actual (UTC)
        fecha_inicio = datetime(año_actual - 2, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        fecha_fin = now

        # 1. Suma de quantity_in_opun por (codigo_proveedor, numero_material) en el periodo
        q = (
            select(
                Compra.codigo_proveedor,
                Compra.numero_material,
                func.coalesce(func.sum(Compra.quantity_in_opun), 0).label("total_qty"),
            )
            .where(
                Compra.posting_date.isnot(None),
                Compra.posting_date >= fecha_inicio,
                Compra.posting_date < fecha_fin,
                Compra.codigo_proveedor.isnot(None),
                Compra.numero_material.isnot(None),
                Compra.numero_material != "",
            )
            .group_by(Compra.codigo_proveedor, Compra.numero_material)
        )
        res = await db.execute(q)
        filas = res.all()

        # 2. Normalizar numero_material y agregar por (codigo_proveedor, numero_material_normalizado)
        from collections import defaultdict
        cantidades: Dict[tuple, float] = defaultdict(lambda: 0.0)
        for row in filas:
            codigo = row.codigo_proveedor
            num_raw = row.numero_material
            total_qty = float(row.total_qty) if row.total_qty is not None else 0.0
            if codigo is None or not num_raw:
                continue
            num_norm = _normalizar_numero_material(num_raw)
            if not num_norm:
                continue
            key = (int(codigo), num_norm)
            cantidades[key] += total_qty

        # 3. Total por material (suma sobre todos los proveedores)
        total_por_material: Dict[str, float] = defaultdict(lambda: 0.0)
        for (codigo, num_mat), qty in cantidades.items():
            total_por_material[num_mat] += qty

        # 4. Porcentaje por (codigo_proveedor, numero_material): 100 * cantidad_proveedor / total_material
        porcentajes: Dict[tuple, float] = {}
        for (codigo, num_mat), qty in cantidades.items():
            total_mat = total_por_material.get(num_mat) or 0
            if total_mat > 0:
                pct = round(100.0 * qty / total_mat, 4)
                porcentajes[(codigo, num_mat)] = pct

        # 5. Obtener todos los registros de pais_origen_material y actualizar
        query_pom = select(PaisOrigenMaterial)
        res_pom = await db.execute(query_pom)
        todos = res_pom.scalars().all()

        for pom in todos:
            num_norm = _normalizar_numero_material(pom.numero_material) or (pom.numero_material or "").strip()
            key = (int(pom.codigo_proveedor), num_norm)
            if not key[1]:
                continue
            pct = porcentajes.get(key)
            if pct is not None:
                if pom.porcentaje_compra is None or float(pom.porcentaje_compra) != pct:
                    await update_pais_origen_material(
                        db,
                        pais_id=pom.id,
                        pais_origen=None,
                        porcentaje_compra=pct,
                        user_id=user_id,
                    )
                    resultado["actualizados"] += 1
            else:
                # No hay compras en el periodo para este (proveedor, material) -> dejar en 0%
                if pom.porcentaje_compra is None or float(pom.porcentaje_compra) != 0:
                    await update_pais_origen_material(
                        db,
                        pais_id=pom.id,
                        pais_origen=None,
                        porcentaje_compra=0.0,
                        user_id=user_id,
                    )
                    resultado["actualizados"] += 1
                resultado["sin_compras_en_periodo"] += 1

    except Exception as e:
        resultado["errores"].append(str(e))
    return resultado


async def list_paises_origen_material(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[PaisOrigenMaterial]:
    """Lista países de origen de materiales con filtros opcionales."""
    query = select(
        PaisOrigenMaterial,
        Proveedor.nombre.label("proveedor_nombre")
    ).outerjoin(
        Proveedor,
        cast(Proveedor.codigo_proveedor, String) == cast(PaisOrigenMaterial.codigo_proveedor, String)
    ).options(
        selectinload(PaisOrigenMaterial.material)
    )
    
    if codigo_proveedor:
        query = query.where(PaisOrigenMaterial.codigo_proveedor == int(codigo_proveedor))
    
    if numero_material:
        query = query.where(PaisOrigenMaterial.numero_material == numero_material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PaisOrigenMaterial.codigo_proveedor.cast(String).ilike(search_pattern),
                PaisOrigenMaterial.numero_material.ilike(search_pattern),
                PaisOrigenMaterial.pais_origen.ilike(search_pattern)
            )
        )
    
    query = query.order_by(desc(PaisOrigenMaterial.updated_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    paises = []
    for pais, proveedor_nombre in result.all():
        # Atributo temporal para la vista, evita resolver relación incompatible por tipo.
        pais.proveedor_nombre = proveedor_nombre
        paises.append(pais)
    return paises


async def count_paises_origen_material(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    search: Optional[str] = None
) -> int:
    """Cuenta el total de países de origen de materiales con filtros opcionales."""
    query = select(func.count(PaisOrigenMaterial.id))
    
    if codigo_proveedor:
        query = query.where(PaisOrigenMaterial.codigo_proveedor == int(codigo_proveedor))
    
    if numero_material:
        query = query.where(PaisOrigenMaterial.numero_material == numero_material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PaisOrigenMaterial.codigo_proveedor.cast(String).ilike(search_pattern),
                PaisOrigenMaterial.numero_material.ilike(search_pattern),
                PaisOrigenMaterial.pais_origen.ilike(search_pattern)
            )
        )
    
    result = await db.execute(query)
    return result.scalar_one()


async def delete_pais_origen_material(
    db: AsyncSession,
    pais_id: int,
    user_id: Optional[int] = None
) -> bool:
    """Elimina un país de origen de material."""
    from sqlalchemy import delete
    
    pais_origen_material = await get_pais_origen_material_by_id(db, pais_id)
    if not pais_origen_material:
        return False
    
    # Guardar datos antes de eliminar
    datos_antes = _pais_origen_material_to_dict(pais_origen_material)
    codigo_proveedor = pais_origen_material.codigo_proveedor
    numero_material = pais_origen_material.numero_material
    
    stmt = delete(PaisOrigenMaterial).where(PaisOrigenMaterial.id == pais_id)
    await db.execute(stmt)
    
    # Registrar en historial
    if user_id is not None:
        historial = PaisOrigenMaterialHistorial(
            pais_origen_id=pais_id,
            codigo_proveedor=codigo_proveedor,
            numero_material=numero_material,
            operacion=PaisOrigenMaterialOperacion.DELETE,
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=None,
            campos_modificados=None
        )
        db.add(historial)
    
    await db.commit()
    return True


# ==================== CRUD para Historial de Paises de Origen ====================

async def list_pais_origen_material_historial(
    db: AsyncSession,
    pais_origen_id: Optional[int] = None,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    operacion: Optional[PaisOrigenMaterialOperacion] = None,
    user_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0
) -> List[PaisOrigenMaterialHistorial]:
    """Lista el historial de cambios en países de origen con filtros opcionales."""
    query = select(PaisOrigenMaterialHistorial).options(selectinload(PaisOrigenMaterialHistorial.user))
    
    if pais_origen_id:
        query = query.where(PaisOrigenMaterialHistorial.pais_origen_id == pais_origen_id)
    
    if codigo_proveedor:
        query = query.where(PaisOrigenMaterialHistorial.codigo_proveedor == int(codigo_proveedor))
    
    if numero_material:
        query = query.where(PaisOrigenMaterialHistorial.numero_material == numero_material)
    
    if operacion:
        query = query.where(PaisOrigenMaterialHistorial.operacion == operacion)
    
    if user_id:
        query = query.where(PaisOrigenMaterialHistorial.user_id == user_id)
    
    query = query.order_by(desc(PaisOrigenMaterialHistorial.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_pais_origen_material_historial_by_id(
    db: AsyncSession,
    historial_id: int
) -> Optional[PaisOrigenMaterialHistorial]:
    """Obtiene un registro del historial por ID."""
    result = await db.execute(
        select(PaisOrigenMaterialHistorial).where(PaisOrigenMaterialHistorial.id == historial_id)
    )
    return result.scalar_one_or_none()


async def count_pais_origen_material_historial(
    db: AsyncSession,
    pais_origen_id: Optional[int] = None,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    operacion: Optional[PaisOrigenMaterialOperacion] = None,
    user_id: Optional[int] = None
) -> int:
    """Cuenta el total de registros en el historial con filtros opcionales."""
    query = select(func.count(PaisOrigenMaterialHistorial.id))
    
    if pais_origen_id:
        query = query.where(PaisOrigenMaterialHistorial.pais_origen_id == pais_origen_id)
    
    if codigo_proveedor:
        query = query.where(PaisOrigenMaterialHistorial.codigo_proveedor == int(codigo_proveedor))
    
    if numero_material:
        query = query.where(PaisOrigenMaterialHistorial.numero_material == numero_material)
    
    if operacion:
        query = query.where(PaisOrigenMaterialHistorial.operacion == operacion)
    
    if user_id:
        query = query.where(PaisOrigenMaterialHistorial.user_id == user_id)
    
    result = await db.execute(query)
    return result.scalar() or 0


# ============================================================================
# Funciones CRUD para ClienteGrupo
# ============================================================================

async def get_cliente_grupo_by_id(db: AsyncSession, id: int) -> Optional[ClienteGrupo]:
    """Obtiene un grupo de cliente por ID."""
    result = await db.execute(select(ClienteGrupo).where(ClienteGrupo.id == id))
    return result.scalar_one_or_none()


async def get_cliente_grupo_by_codigo(db: AsyncSession, codigo_cliente: int) -> Optional[ClienteGrupo]:
    """Obtiene un grupo de cliente por código de cliente.
    Si hay múltiples registros, retorna el primero encontrado."""
    result = await db.execute(select(ClienteGrupo).where(ClienteGrupo.codigo_cliente == codigo_cliente))
    # Usar first() en lugar de scalar_one_or_none() para evitar error cuando hay múltiples registros
    return result.scalars().first()


async def list_cliente_grupos(
    db: AsyncSession,
    search: Optional[str] = None,
    grupo: Optional[str] = None,
    grupo_viejo: Optional[str] = None,
    limit: int = 1000,
    offset: int = 0
) -> List[ClienteGrupo]:
    """Lista grupos de clientes con filtros opcionales."""
    query = select(ClienteGrupo)
    
    if search:
        # Buscar en código_cliente (convertir a string para búsqueda)
        try:
            codigo_search = int(search)
            query = query.where(ClienteGrupo.codigo_cliente == codigo_search)
        except ValueError:
            # Si no es un número, buscar en grupo y grupo_viejo
            search_pattern = f"%{search}%"
            query = query.where(
                or_(
                    ClienteGrupo.grupo.ilike(search_pattern),
                    ClienteGrupo.grupo_viejo.ilike(search_pattern)
                )
            )
    
    if grupo:
        query = query.where(ClienteGrupo.grupo == grupo)
    
    if grupo_viejo:
        query = query.where(ClienteGrupo.grupo_viejo == grupo_viejo)
    
    query = query.order_by(desc(ClienteGrupo.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_cliente_grupos(
    db: AsyncSession,
    search: Optional[str] = None,
    grupo: Optional[str] = None,
    grupo_viejo: Optional[str] = None
) -> int:
    """Cuenta el total de grupos de clientes con filtros opcionales."""
    query = select(func.count(ClienteGrupo.id))
    
    if search:
        try:
            codigo_search = int(search)
            query = query.where(ClienteGrupo.codigo_cliente == codigo_search)
        except ValueError:
            search_pattern = f"%{search}%"
            query = query.where(
                or_(
                    ClienteGrupo.grupo.ilike(search_pattern),
                    ClienteGrupo.grupo_viejo.ilike(search_pattern)
                )
            )
    
    if grupo:
        query = query.where(ClienteGrupo.grupo == grupo)
    
    if grupo_viejo:
        query = query.where(ClienteGrupo.grupo_viejo == grupo_viejo)
    
    result = await db.execute(query)
    return result.scalar() or 0


async def create_cliente_grupo(
    db: AsyncSession,
    codigo_cliente: int,
    grupo: Optional[str] = None,
    grupo_viejo: Optional[str] = None
) -> ClienteGrupo:
    """Crea un nuevo grupo de cliente."""
    cliente_grupo = ClienteGrupo(
        codigo_cliente=codigo_cliente,
        grupo=grupo,
        grupo_viejo=grupo_viejo
    )
    db.add(cliente_grupo)
    await db.commit()
    await db.refresh(cliente_grupo)
    return cliente_grupo


async def update_cliente_grupo(
    db: AsyncSession,
    id: int,
    grupo: Optional[str] = None,
    grupo_viejo: Optional[str] = None
) -> Optional[ClienteGrupo]:
    """Actualiza un grupo de cliente."""
    cliente_grupo = await get_cliente_grupo_by_id(db, id)
    if not cliente_grupo:
        return None
    
    if grupo is not None:
        cliente_grupo.grupo = grupo
    if grupo_viejo is not None:
        cliente_grupo.grupo_viejo = grupo_viejo
    
    await db.commit()
    await db.refresh(cliente_grupo)
    return cliente_grupo


async def delete_cliente_grupo(db: AsyncSession, id: int) -> bool:
    """Elimina un grupo de cliente."""
    cliente_grupo = await get_cliente_grupo_by_id(db, id)
    if not cliente_grupo:
        return False
    
    await db.delete(cliente_grupo)
    await db.commit()
    return True


# ==================== CRUD para Venta ====================

async def bulk_create_ventas(
    db: AsyncSession,
    ventas_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Inserta múltiples registros de ventas verificando duplicados.
    No inserta si ya existe un registro con la misma combinación de codigo_cliente, producto y periodo.
    Evita duplicados dentro del mismo lote y maneja concurrencia con un reintento en caso de conflicto.
    
    Args:
        db: Sesión de base de datos
        ventas_data: Lista de diccionarios con los datos de las ventas
        
    Returns:
        Diccionario con 'insertados', 'duplicados' y 'errores'
    """
    if not ventas_data:
        return {"insertados": 0, "duplicados": 0, "errores": []}

    # Sincronizar secuencia de id para evitar UniqueViolationError (id ya existente)
    try:
        await db.execute(
            text(
                "SELECT setval(pg_get_serial_sequence('ventas', 'id'), "
                "COALESCE((SELECT MAX(id) FROM ventas), 1))"
            )
        )
    except Exception:
        pass  # Si la tabla no tiene secuencia o falla, continuar

    # Desduplicar por (codigo_cliente, producto, periodo) dentro del mismo lote (mantener primera ocurrencia)
    vistos_en_lote = set()
    ventas_unicas = []
    for v in ventas_data:
        key = (v.get("codigo_cliente"), v.get("producto"), v.get("periodo"))
        if key in vistos_en_lote:
            continue
        vistos_en_lote.add(key)
        ventas_unicas.append(v)
    ventas_data = ventas_unicas

    max_reintentos = 2  # intento normal + 1 reintento por concurrencia
    for intento in range(max_reintentos):
        insertados = 0
        duplicados = 0
        errores = []

        for idx, venta_data in enumerate(ventas_data, start=1):
            try:
                codigo_cliente = venta_data.get('codigo_cliente')
                producto = venta_data.get('producto')
                periodo = venta_data.get('periodo')

                # Verificar si ya existe un registro con la misma combinación (en BD o en esta sesión)
                venta_existente = await get_venta_by_codigo_producto_periodo(
                    db, codigo_cliente, producto, periodo
                )

                if venta_existente:
                    # Ya existe, no insertar (contar como duplicado)
                    duplicados += 1
                else:
                    # No existe, crear nuevo registro (no pasar 'id' para que la BD asigne el valor)
                    venta_data_sin_id = {k: v for k, v in venta_data.items() if k != "id"}
                    venta = Venta(**venta_data_sin_id)
                    db.add(venta)
                    await db.flush()  # Hacer visible en esta sesión para la siguiente comprobación
                    insertados += 1
            except IntegrityError as e:
                # Error de integridad (ej. PK/unique): la sesión queda invalidada, no se puede seguir
                await db.rollback()
                error_str = str(e)
                error_lower = error_str.lower()
                if "ventas_pkey" in error_lower or ("duplicate" in error_lower and "key" in error_lower and "id" in error_lower):
                    raise Exception(
                        "Error al guardar: La secuencia de IDs de la tabla ventas está desincronizada (ya existe un registro con el mismo id). Se ha intentado corregir automáticamente; si el error persiste, ejecuta en la base de datos: SELECT setval(pg_get_serial_sequence('ventas', 'id'), (SELECT COALESCE(MAX(id), 1) FROM ventas));"
                    )
                raise Exception(f"Error al guardar los datos en la base de datos: {error_str}")
            except Exception as e:
                error_str = str(e)
                error_lower = error_str.lower()
                # Si la sesión quedó invalidada por un error previo (ej. durante flush), no continuar
                if "rolled back" in error_lower and "previous exception" in error_lower:
                    await db.rollback()
                    raise Exception(
                        "Error al guardar: Ocurrió un error durante la inserción y la sesión quedó invalidada. Detalle: " + error_str
                    )
                # Capturar otros errores individuales
                if "multiple rows" in error_lower or "multiple rows were found" in error_lower:
                    mensaje_error = f"Error en registro {idx}: Se encontraron múltiples registros duplicados en la base de datos con la misma combinación de código cliente, producto y período. Esto indica que ya existen duplicados en la base de datos. El registro no se insertará."
                elif "foreign key" in error_lower or "constraint" in error_lower:
                    mensaje_error = f"Error en registro {idx}: Violación de restricción de base de datos. Verifica que el grupo de cliente exista."
                elif "null" in error_lower and "not null" in error_lower:
                    mensaje_error = f"Error en registro {idx}: Campo requerido está vacío. Verifica que todos los campos obligatorios tengan valores."
                elif "value" in error_lower and "type" in error_lower:
                    mensaje_error = f"Error en registro {idx}: Tipo de dato incorrecto. Verifica el formato de los valores."
                else:
                    mensaje_error = f"Error en registro {idx}: {error_str}"

                errores.append({
                    "fila": idx,
                    "error": mensaje_error,
                    "error_tecnico": error_str,
                    "codigo_cliente": venta_data.get('codigo_cliente'),
                    "producto": venta_data.get('producto'),
                    "periodo": str(venta_data.get('periodo')) if venta_data.get('periodo') else None
                })

        try:
            await db.commit()
            return {
                "insertados": insertados,
                "duplicados": duplicados,
                "errores": errores
            }
        except IntegrityError as e:
            await db.rollback()
            error_lower = str(e).lower()
            if "duplicate" in error_lower or "unique" in error_lower:
                # Posible concurrencia: otro proceso insertó la misma clave. Reintentar una vez.
                if intento < max_reintentos - 1:
                    continue
                raise Exception(
                    "Error al guardar: Se intentó insertar registros duplicados. El sistema ya verifica duplicados, pero puede haber un problema de concurrencia. Intenta subir de nuevo; los duplicados se omitirán."
                )
            raise
        except Exception as e:
            await db.rollback()
            error_str = str(e)
            error_lower = error_str.lower()

            if "foreign key" in error_lower:
                raise Exception("Error al guardar: Violación de restricción de clave foránea. Verifica que los datos de referencia (grupo de cliente) existan en la base de datos.")
            if "connection" in error_lower or "timeout" in error_lower:
                raise Exception("Error al guardar: No se pudo conectar con la base de datos o se agotó el tiempo de espera. Por favor, intenta nuevamente.")
            raise Exception(f"Error al guardar los datos en la base de datos: {error_str}")


async def get_cliente_grupo_by_codigo_cliente(db: AsyncSession, codigo_cliente: int) -> Optional[ClienteGrupo]:
    """Obtiene un grupo de cliente por código de cliente.
    Si hay múltiples registros, retorna el primero encontrado."""
    result = await db.execute(
        select(ClienteGrupo).where(ClienteGrupo.codigo_cliente == codigo_cliente)
    )
    # Usar first() en lugar de scalar_one_or_none() para evitar error cuando hay múltiples registros
    return result.scalars().first()


async def get_venta_by_codigo_producto_periodo(
    db: AsyncSession,
    codigo_cliente: Optional[int],
    producto: Optional[str],
    periodo: Optional[date]
) -> Optional[Venta]:
    """Verifica si existe una venta con la misma combinación de codigo_cliente, producto y periodo.
    Si hay múltiples registros, retorna el primero encontrado (indica que ya existe un duplicado)."""
    if codigo_cliente is None or producto is None or periodo is None:
        return None
    
    query = select(Venta).where(
        Venta.codigo_cliente == codigo_cliente,
        Venta.producto == producto,
        Venta.periodo == periodo
    )
    result = await db.execute(query)
    # Usar first() en lugar de scalar_one_or_none() para evitar error cuando hay múltiples registros
    # Si hay múltiples, significa que ya existen duplicados en la BD, así que retornamos el primero
    return result.scalars().first()


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
    only_with_sales_km: bool = False
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
                Venta.planta.ilike(search_pattern)
            )
        )
    
    if cliente:
        query = query.where(Venta.cliente.ilike(f"%{cliente}%"))
    
    if codigo_cliente is not None:
        query = query.where(Venta.codigo_cliente == codigo_cliente)
    
    if periodo_inicio:
        query = query.where(Venta.periodo >= periodo_inicio.date() if isinstance(periodo_inicio, datetime) else periodo_inicio)
    
    if periodo_fin:
        query = query.where(Venta.periodo <= periodo_fin.date() if isinstance(periodo_fin, datetime) else periodo_fin)
    
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
    """
    Lista ventas filtradas por lista de números de parte.

    - `producto`: coincidencia exacta con algún valor (tras strip).
    - `producto_condensado`: si `producto_condensado_prefix_len` > 0, coincide con el prefijo de esa
      longitud de cada valor (conjunto único); si es None, coincide con el valor completo como `producto`.
    """
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
    only_with_sales_km: bool = False
) -> int:
    """Cuenta el total de ventas con filtros opcionales."""
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
                Venta.planta.ilike(search_pattern)
            )
        )
    
    if cliente:
        query = query.where(Venta.cliente.ilike(f"%{cliente}%"))
    
    if codigo_cliente is not None:
        query = query.where(Venta.codigo_cliente == codigo_cliente)
    
    if periodo_inicio:
        query = query.where(Venta.periodo >= periodo_inicio.date() if isinstance(periodo_inicio, datetime) else periodo_inicio)
    
    if periodo_fin:
        query = query.where(Venta.periodo <= periodo_fin.date() if isinstance(periodo_fin, datetime) else periodo_fin)
    
    if producto:
        query = query.where(Venta.producto.ilike(f"%{producto}%"))
    
    if planta:
        query = query.where(Venta.planta.ilike(f"%{planta}%"))
    
    result = await db.execute(query)
    return result.scalar() or 0


async def list_fracciones_arancelarias_ventas(
    db: AsyncSession,
    search: Optional[str] = None,
    solo_con_fraccion: bool = False,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Lista números de parte únicos que aparecen en registros de ventas (producto_condensado)
    con su fracción arancelaria desde la tabla partes. Sin duplicados por número de parte.
    Si solo_con_fraccion=True, solo incluye registros que tienen fracción no nula ni vacía.
    """
    # Subconsulta o query: distinct producto_condensado de ventas + fraccion de partes (LEFT JOIN)
    query = (
        select(Venta.producto_condensado, Parte.fraccion)
        .select_from(Venta)
        .outerjoin(Parte, Venta.producto_condensado == Parte.numero_parte)
        .where(
            Venta.producto_condensado.isnot(None),
            Venta.producto_condensado != "",
        )
        .distinct()
        .order_by(Venta.producto_condensado)
    )
    if solo_con_fraccion:
        query = query.where(
            Parte.fraccion.isnot(None),
            Parte.fraccion != "",
        )
    if search and search.strip():
        search_pattern = f"%{search.strip()}%"
        query = query.where(
            or_(
                Venta.producto_condensado.ilike(search_pattern),
                Parte.fraccion.ilike(search_pattern),
            )
        )
    if limit is not None:
        query = query.limit(limit)
    if offset is not None:
        query = query.offset(offset)
    result = await db.execute(query)
    rows = result.all()
    return [
        {
            "numero_parte": (r[0] and str(r[0]).strip()) or "",
            "fraccion": (r[1] and str(r[1]).strip()) or "—",
        }
        for r in rows
        if r[0]
    ]


async def count_fracciones_arancelarias_ventas(
    db: AsyncSession,
    search: Optional[str] = None,
    solo_con_fraccion: bool = False,
) -> int:
    """Cuenta números de parte únicos en ventas (para paginación)."""
    subq = (
        select(Venta.producto_condensado, Parte.fraccion)
        .select_from(Venta)
        .outerjoin(Parte, Venta.producto_condensado == Parte.numero_parte)
        .where(
            Venta.producto_condensado.isnot(None),
            Venta.producto_condensado != "",
        )
        .distinct()
    )
    if solo_con_fraccion:
        subq = subq.where(
            Parte.fraccion.isnot(None),
            Parte.fraccion != "",
        )
    if search and search.strip():
        search_pattern = f"%{search.strip()}%"
        subq = subq.where(
            or_(
                Venta.producto_condensado.ilike(search_pattern),
                Parte.fraccion.ilike(search_pattern),
            )
        )
    query = select(func.count()).select_from(subq.subquery())
    result = await db.execute(query)
    return result.scalar() or 0


async def list_partes_icr_por_cliente(
    db: AsyncSession,
    codigo_cliente: int,
) -> List[str]:
    """
    Devuelve los números de parte únicos (producto_condensado) que ha adquirido un cliente,
    considerando solo ventas donde precio_exmetal_km, precio_exmetal_m, precio_full_metal_km
    y precio_full_metal_m no son null ni 0.
    """
    query = (
        select(Venta.producto_condensado)
        .where(
            Venta.codigo_cliente == codigo_cliente,
            Venta.producto_condensado.isnot(None),
            Venta.producto_condensado != "",
            Venta.precio_exmetal_km.isnot(None),
            Venta.precio_exmetal_km != 0,
            Venta.precio_exmetal_m.isnot(None),
            Venta.precio_exmetal_m != 0,
            Venta.precio_full_metal_km.isnot(None),
            Venta.precio_full_metal_km != 0,
            Venta.precio_full_metal_m.isnot(None),
            Venta.precio_full_metal_m != 0,
        )
        .distinct()
        .order_by(Venta.producto_condensado)
    )
    result = await db.execute(query)
    rows = result.all()
    return [str(r[0]).strip() for r in rows if r[0] and str(r[0]).strip()]


# Filtro común: ventas con sales_km y precios ICR no vacíos ni 0 (para reportes y análisis).
_ventas_icr_filter = and_(
    Venta.codigo_cliente.isnot(None),
    Venta.producto_condensado.isnot(None),
    Venta.producto_condensado != "",
    Venta.sales_km.isnot(None),
    Venta.sales_km != 0,
    Venta.precio_exmetal_km.isnot(None),
    Venta.precio_exmetal_km != 0,
    Venta.precio_exmetal_m.isnot(None),
    Venta.precio_exmetal_m != 0,
    Venta.precio_full_metal_km.isnot(None),
    Venta.precio_full_metal_km != 0,
    Venta.precio_full_metal_m.isnot(None),
    Venta.precio_full_metal_m != 0,
)


async def list_todos_clientes_partes_con_ventas_icr(db: AsyncSession) -> List[Dict[str, Any]]:
    """
    Lista todos los (codigo_cliente, numero_parte) únicos a partir de ventas que tienen
    sales_km y columnas de precio (precio_exmetal_km, precio_exmetal_m, precio_full_metal_km,
    precio_full_metal_m) no vacías ni 0.
    Devuelve lista de dicts con codigo_cliente, cliente_nombre, part_number.
    """
    query = (
        select(Venta.codigo_cliente, Venta.producto_condensado)
        .where(_ventas_icr_filter)
        .distinct()
        .order_by(Venta.codigo_cliente, Venta.producto_condensado)
    )
    result = await db.execute(query)
    rows = result.all()
    if not rows:
        return []
    # Nombres de cliente desde tabla Cliente
    codigos = list({int(r[0]) for r in rows if r[0] is not None})
    nombres_query = select(Cliente.codigo_cliente, Cliente.nombre).where(Cliente.codigo_cliente.in_(codigos))
    nombres_result = await db.execute(nombres_query)
    nombres_map = {int(r[0]): (r[1] or "").strip() for r in nombres_result.all() if r[0] is not None}
    # Si no está en Cliente, opcionalmente desde Venta.cliente (una consulta por codigo faltante)
    codigos_sin_nombre = [c for c in codigos if not nombres_map.get(c)]
    if codigos_sin_nombre:
        fallback = await db.execute(
            select(Venta.codigo_cliente, Venta.cliente).where(
                Venta.codigo_cliente.in_(codigos_sin_nombre), Venta.cliente.isnot(None)
            )
        )
        for r in fallback.all():
            cod = int(r[0]) if r[0] is not None else None
            if cod is not None and cod not in nombres_map:
                nombres_map[cod] = (r[1] or "").strip()
    out = []
    for r in rows:
        codigo = r[0]
        pn = (r[1] and str(r[1]).strip()) or ""
        if codigo is None or not pn:
            continue
        out.append({
            "codigo_cliente": int(codigo),
            "cliente_nombre": nombres_map.get(int(codigo)) or str(codigo),
            "part_number": pn,
        })
    return out


async def list_clientes_con_ventas_icr(
    db: AsyncSession,
    search: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Lista clientes que tienen al menos una venta con datos ICR (precios exmetal/full_metal no nulos).
    Devuelve lista de dicts con codigo_cliente y nombre. Opcionalmente filtra por búsqueda en código o nombre.
    """
    subq = (
        select(Venta.codigo_cliente)
        .where(_ventas_icr_filter)
        .distinct()
    )
    result = await db.execute(subq)
    codigos = [int(r[0]) for r in result.all() if r[0] is not None]
    if not codigos:
        return []
    nombres_query = select(Cliente.codigo_cliente, Cliente.nombre).where(Cliente.codigo_cliente.in_(codigos))
    nombres_result = await db.execute(nombres_query)
    nombres_map = {int(r[0]): (r[1] or "").strip() for r in nombres_result.all() if r[0] is not None}
    codigos_sin_nombre = [c for c in codigos if c not in nombres_map]
    if codigos_sin_nombre:
        fallback = await db.execute(
            select(Venta.codigo_cliente, Venta.cliente).where(
                Venta.codigo_cliente.in_(codigos_sin_nombre),
                Venta.cliente.isnot(None),
            ).distinct()
        )
        for r in fallback.all():
            cod = int(r[0]) if r[0] is not None else None
            if cod is not None and cod not in nombres_map:
                nombres_map[cod] = (r[1] or "").strip()
    out = []
    search_clean = (search or "").strip().lower() if search else ""
    for cod in codigos:
        nombre = nombres_map.get(cod) or str(cod)
        if search_clean and search_clean not in nombre.lower() and search_clean not in str(cod):
            continue
        out.append({"codigo_cliente": cod, "nombre": nombre})
    out.sort(key=lambda x: (x["nombre"].lower(), x["codigo_cliente"]))
    return out[:limit]


def _sum_value_items(items: List[Dict[str, Any]]) -> float:
    """Suma la columna Value (qty * porcentaje_compra/100 * precio_compra) de una lista de ítems con proveedores."""
    total = 0.0
    if items is None:
        return total
    for item in items:
        for p in (item.get("proveedores") or []):
            qty = item.get("qty")
            pct = p.get("porcentaje_compra")
            precio = p.get("precio_compra")
            if qty is not None and pct is not None and precio is not None:
                total += float(qty) * float(pct) / 100 * float(precio)
    return round(total, 3)


def _icr_rules_force_zero(
    items_originating: List[Dict[str, Any]],
    items_non_originating: List[Dict[str, Any]],
) -> bool:
    """Devuelve True si alguna regla fuerza ICR 0%. Ver _icr_rules_force_zero_reason para el texto."""
    return _icr_rules_force_zero_reason(items_originating, items_non_originating) is not None


def _icr_rules_force_zero_reason(
    items_originating: List[Dict[str, Any]],
    items_non_originating: List[Dict[str, Any]],
) -> Optional[str]:
    """
    Reglas de negocio: devuelve la razón por la que ICR debe ser 0%, o None si no aplica.
    - Regla 1: CABLE en Non-Originating sin 310004003 en Originating.
    - Regla 2: CUERDA en Non-Originating sin 310004003 en Originating.
    - Regla 3: No hay ítems en Originating.
    """
    orig = items_originating or []
    non_orig = items_non_originating or []
    if not orig:
        return "Sin ítems Originating"
    has_310004003_in_orig = any(
        (item.get("numero_parte") or "").strip() == "310004003" for item in orig
    )

    def item_has_tipo(item: Dict[str, Any], tipo_val: str) -> bool:
        t = (tipo_val or "").strip().upper()
        return any(
            (p.get("tipo") or "").strip().upper() == t
            for p in (item.get("proveedores") or [])
        )

    has_cable_in_non = any(item_has_tipo(item, "CABLE") for item in non_orig)
    if has_cable_in_non and not has_310004003_in_orig:
        return "CABLE sin 310004003 en Originating"
    has_cuerda_in_non = any(item_has_tipo(item, "CUERDA") for item in non_orig)
    if has_cuerda_in_non and not has_310004003_in_orig:
        return "CUERDA sin 310004003 en Originating"
    return None


async def get_numero_partes_trading_good_true(db: AsyncSession) -> Set[str]:
    """Devuelve el conjunto de numero_parte que están en trading_goods con is_trading_good=True."""
    result = await db.execute(
        select(TradingGood.numero_parte).where(TradingGood.is_trading_good.is_(True))
    )
    return {str(r[0]).strip() for r in result.all() if r[0]}


async def get_icr_para_parte(
    db: AsyncSession,
    codigo_cliente: int,
    numero_parte: str,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Calcula el Regional Index (ICR) para un cliente y número de parte.
    Devuelve (icr, icr_zero_reason): icr es el porcentaje o None; icr_zero_reason solo cuando icr == 0.
    """
    numero_parte = str(numero_parte or "").strip()
    tg = await get_trading_good_by_numero_parte(db, numero_parte)
    if tg and tg.is_trading_good:
        return (0.0, "Trading good")
    bom_data = await get_bom_items_for_parte(db, numero_parte)
    if bom_data and bom_data.get("alguna_compra_antigua"):
        return (0.0, "Compra antigua (>2 años)")
    items_orig = (bom_data.get("items_originating") or []) if bom_data else []
    items_non_orig = (bom_data.get("items_non_originating") or []) if bom_data else []
    rules_reason = _icr_rules_force_zero_reason(items_orig, items_non_orig)
    if rules_reason is not None:
        return (0.0, rules_reason)
    total_originating_value = _sum_value_items(items_orig)
    total_non_originating_value = _sum_value_items(items_non_orig)
    fob_total_value = await get_ultimo_precio_venta_cliente_parte(db, int(codigo_cliente), str(numero_parte).strip())
    if fob_total_value is None:
        return (None, None)
    fob_total_value = round(fob_total_value, 3)
    markup_value = round(
        fob_total_value - total_originating_value - total_non_originating_value,
        3,
    )
    if markup_value < 0:
        return (0.0, "Markup negativo")
    if fob_total_value == 0:
        return (None, None)
    regional_index = (total_originating_value + markup_value) / fob_total_value * 100
    return (round(regional_index, 2), None)


async def get_analisis_icr_detalle(
    db: AsyncSession,
    codigo_cliente: int,
) -> Dict[str, Any]:
    """
    Devuelve el detalle para la vista Análisis ICR: nombre del cliente y lista de partes
    con part_number, description, tariff_schedule (fracción de Parte), origin, icr.
    Solo ventas con precios ICR no null ni 0.
    """
    # Nombre del cliente: primero desde tabla clientes, si no desde ventas
    cliente_row = await db.execute(
        select(Cliente.nombre).where(Cliente.codigo_cliente == codigo_cliente)
    )
    cliente_nombre = cliente_row.scalar_one_or_none()
    if cliente_nombre is None:
        venta_nombre = await db.execute(
            select(Venta.cliente)
            .where(Venta.codigo_cliente == codigo_cliente, Venta.cliente.isnot(None))
            .limit(1)
        )
        cliente_nombre = (venta_nombre.scalar_one_or_none() or "").strip() or str(codigo_cliente)

    # Solo ventas del año pasado y año actual (ej. 2025 y 2026)
    año_actual = datetime.now().year
    año_pasado = año_actual - 1
    inicio_periodo = date(año_pasado, 1, 1)
    fin_periodo = date(año_actual, 12, 31)

    base_filter = and_(
        Venta.codigo_cliente == codigo_cliente,
        Venta.producto_condensado.isnot(None),
        Venta.producto_condensado != "",
        Venta.periodo >= inicio_periodo,
        Venta.periodo <= fin_periodo,
        Venta.precio_exmetal_km.isnot(None),
        Venta.precio_exmetal_km != 0,
        Venta.precio_exmetal_m.isnot(None),
        Venta.precio_exmetal_m != 0,
        Venta.precio_full_metal_km.isnot(None),
        Venta.precio_full_metal_km != 0,
        Venta.precio_full_metal_m.isnot(None),
        Venta.precio_full_metal_m != 0,
    )
    ventas_query = (
        select(Venta.producto_condensado, Venta.descripcion_producto)
        .where(base_filter)
        .order_by(Venta.producto_condensado, desc(Venta.id))
    )
    ventas_result = await db.execute(ventas_query)
    ventas_rows = ventas_result.all()

    # Un parte por producto_condensado, primera descripción encontrada
    part_numbers_seen = set()
    partes_list = []
    for row in ventas_rows:
        pn = (row[0] and str(row[0]).strip()) or ""
        if not pn or pn == "DUMMY" or pn in part_numbers_seen:
            continue
        part_numbers_seen.add(pn)
        descr = (row[1] and str(row[1]).strip()) or "—"
        partes_list.append({"part_number": pn, "description": descr})

    # Ordenar por part_number
    partes_list.sort(key=lambda x: x["part_number"])

    # Obtener fracción (tariff) y descripción canónica desde tabla partes
    for p in partes_list:
        p["tariff_schedule"] = "—"
        p["origin"] = "—"
    if partes_list:
        numeros = [p["part_number"] for p in partes_list]
        partes_db = await db.execute(
            select(Parte.numero_parte, Parte.fraccion, Parte.descripcion).where(Parte.numero_parte.in_(numeros))
        )
        for r in partes_db.all():
            pn = str(r[0]).strip()
            fraccion = (r[1] and str(r[1]).strip()) or "—"
            descr_parte = (r[2] and str(r[2]).strip()) or ""
            for p in partes_list:
                if p["part_number"] == pn:
                    p["tariff_schedule"] = fraccion
                    if descr_parte:
                        p["description"] = descr_parte  # descripción canónica por número de parte
                    break

    # Calcular ICR (Regional index) por parte para mostrarlo en la lista; incluir razón cuando ICR = 0%
    for p in partes_list:
        icr_val, icr_reason = await get_icr_para_parte(db, codigo_cliente, p["part_number"])
        p["icr"] = icr_val
        p["icr_zero_reason"] = icr_reason if (icr_val is not None and float(icr_val) == 0) else None

    # Marcar partes que son trading good (ICR 0% y badge)
    trading_good_set = await get_numero_partes_trading_good_true(db)
    for p in partes_list:
        p["is_trading_good"] = (p.get("part_number") or "").strip() in trading_good_set

    # Partes que tienen al menos un ítem de BOM (revisión vigente): para mostrar/ocultar Select
    part_numbers_with_bom = set()
    if partes_list:
        numeros = [p["part_number"] for p in partes_list]
        q_bom = (
            select(Parte.numero_parte)
            .join(Bom, Bom.parte_id == Parte.id)
            .join(BomRevision, (BomRevision.bom_id == Bom.id) & (BomRevision.effective_to.is_(None)))
            .join(BomItem, BomItem.bom_revision_id == BomRevision.id)
            .where(Parte.numero_parte.in_(numeros))
            .distinct()
        )
        result_bom = await db.execute(q_bom)
        part_numbers_with_bom = {str(r[0]).strip() for r in result_bom.all() if r[0]}
    for p in partes_list:
        p["has_bom_items"] = (p.get("part_number") or "").strip() in part_numbers_with_bom

    # Completion Status = (número de partes con ICR > 60%) / total * 100
    total_partes = len(partes_list)
    partes_icr_over_60 = sum(1 for p in partes_list if p.get("icr") is not None and float(p["icr"]) > 60)
    completion_pct = round((partes_icr_over_60 / total_partes * 100), 2) if total_partes else 0

    return {
        "codigo_cliente": codigo_cliente,
        "cliente_nombre": cliente_nombre,
        "partes": partes_list,
        "total": total_partes,
        "completion_pct": completion_pct,
        "partes_icr_over_60": partes_icr_over_60,
    }


# ============================================================================
# CRUD para CargaProveedor
# ============================================================================

async def create_carga_proveedor(
    db: AsyncSession,
    codigo_proveedor: str,
    nombre: Optional[str] = None,
    apellido_paterno: Optional[str] = None,
    apellido_materno: Optional[str] = None,
    pais: Optional[str] = None,
    domicilio: Optional[str] = None,
    cliente_proveedor: Optional[str] = None,
    estatus: Optional[str] = None
) -> CargaProveedor:
    """Crea un nuevo registro de carga de proveedor."""
    carga_proveedor = CargaProveedor(
        codigo_proveedor=codigo_proveedor,
        nombre=nombre,
        apellido_paterno=apellido_paterno,
        apellido_materno=apellido_materno,
        pais=pais,
        domicilio=domicilio,
        cliente_proveedor=cliente_proveedor,
        estatus=estatus
    )
    db.add(carga_proveedor)
    await db.commit()
    await db.refresh(carga_proveedor)
    return carga_proveedor


async def get_carga_proveedor_by_id(db: AsyncSession, carga_id: int) -> Optional[CargaProveedor]:
    """Obtiene un registro de carga de proveedor por ID."""
    result = await db.execute(
        select(CargaProveedor)
        .where(CargaProveedor.id == carga_id)
    )
    return result.scalar_one_or_none()


async def get_carga_proveedor_by_codigo(db: AsyncSession, codigo_proveedor: str) -> Optional[CargaProveedor]:
    """Obtiene un registro de carga de proveedor por código de proveedor."""
    result = await db.execute(
        select(CargaProveedor)
        .where(CargaProveedor.codigo_proveedor == int(codigo_proveedor))
        .order_by(desc(CargaProveedor.created_at))
        .limit(1)
    )
    return result.scalar_one_or_none()


async def list_carga_proveedores(
    db: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    codigo_proveedor: Optional[str] = None,
    cliente_proveedor: Optional[str] = None,
    estatus: Optional[str] = None
) -> List[CargaProveedor]:
    """Lista registros de carga de proveedores con filtros opcionales."""
    query = select(CargaProveedor)
    
    if codigo_proveedor:
        query = query.where(CargaProveedor.codigo_proveedor.cast(String).ilike(f"%{codigo_proveedor}%"))
    
    if cliente_proveedor:
        query = query.where(CargaProveedor.cliente_proveedor.ilike(f"%{cliente_proveedor}%"))
    
    if estatus:
        query = query.where(CargaProveedor.estatus == estatus)
    
    query = query.order_by(desc(CargaProveedor.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_carga_proveedores(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    cliente_proveedor: Optional[str] = None,
    estatus: Optional[str] = None
) -> int:
    """Cuenta el total de registros de carga de proveedores con filtros opcionales."""
    query = select(func.count(CargaProveedor.id))
    
    if codigo_proveedor:
        query = query.where(CargaProveedor.codigo_proveedor.cast(String).ilike(f"%{codigo_proveedor}%"))
    
    if cliente_proveedor:
        query = query.where(CargaProveedor.cliente_proveedor.ilike(f"%{cliente_proveedor}%"))
    
    if estatus:
        query = query.where(CargaProveedor.estatus == estatus)
    
    result = await db.execute(query)
    return result.scalar() or 0


async def update_carga_proveedor(
    db: AsyncSession,
    carga_id: int,
    codigo_proveedor: Optional[str] = None,
    nombre: Optional[str] = None,
    apellido_paterno: Optional[str] = None,
    apellido_materno: Optional[str] = None,
    pais: Optional[str] = None,
    domicilio: Optional[str] = None,
    cliente_proveedor: Optional[str] = None,
    estatus: Optional[str] = None
) -> Optional[CargaProveedor]:
    """Actualiza un registro de carga de proveedor."""
    carga_proveedor = await get_carga_proveedor_by_id(db, carga_id)
    if not carga_proveedor:
        return None
    
    if codigo_proveedor is not None:
        carga_proveedor.codigo_proveedor = codigo_proveedor
    if nombre is not None:
        carga_proveedor.nombre = nombre
    if apellido_paterno is not None:
        carga_proveedor.apellido_paterno = apellido_paterno
    if apellido_materno is not None:
        carga_proveedor.apellido_materno = apellido_materno
    if pais is not None:
        carga_proveedor.pais = pais
    if domicilio is not None:
        carga_proveedor.domicilio = domicilio
    if cliente_proveedor is not None:
        carga_proveedor.cliente_proveedor = cliente_proveedor
    if estatus is not None:
        carga_proveedor.estatus = estatus
    
    await db.commit()
    await db.refresh(carga_proveedor)
    return carga_proveedor


async def delete_carga_proveedor(db: AsyncSession, carga_id: int) -> bool:
    """Elimina un registro de carga de proveedor."""
    carga_proveedor = await get_carga_proveedor_by_id(db, carga_id)
    if not carga_proveedor:
        return False
    
    await db.delete(carga_proveedor)
    await db.commit()
    return True


async def obtener_fecha_ultimo_cambio_estatus_proveedor(db: AsyncSession, codigo_proveedor: str, estatus: str) -> Optional[date]:
    """
    Obtiene la fecha del último registro en el historial donde el proveedor
    cambió a un estatus específico. Retorna None si no hay registro.
    """
    query = select(CargaProveedorHistorial).where(
        CargaProveedorHistorial.codigo_proveedor == int(codigo_proveedor),
        CargaProveedorHistorial.estatus_nuevo == estatus
    ).order_by(desc(CargaProveedorHistorial.created_at)).limit(1)
    
    result = await db.execute(query)
    historial = result.scalar_one_or_none()
    
    if historial and historial.created_at:
        return historial.created_at.date()
    return None


async def actualizar_estatus_carga_proveedores_por_compras(
    db: AsyncSession
) -> Dict[str, Any]:
    """
    Actualiza los estatus de carga_proveedores basándose en compras de los últimos 6 meses.
    Solo considera proveedores con país diferente a "MX" (México).
    
    Reglas con verificación de MES CALENDARIO:
    - Solo se consideran proveedores con país != "MX"
    - Alta se mantiene en el mismo mes, cambia a Sin modificacion en mes diferente
    - Sin modificacion se mantiene en el mismo mes
    - Baja que vuelve a tener compras → Alta (reactivación)
    - Baja dos meses seguidos → Eliminado
    
    Returns:
        Dict con estadísticas de la actualización
    """
    errores = []
    proveedores_marcados_baja = 0
    proveedores_sin_modificacion = 0
    proveedores_nuevos = 0
    proveedores_omitidos_mx = 0
    proveedores_eliminados = 0
    proveedores_sin_cambios = 0
    
    # Fecha de hoy para comparación de mes calendario
    hoy = date.today()
    
    try:
        # Fecha límite para compras (últimos 6 meses)
        fecha_limite_compras = datetime.now(timezone.utc) - timedelta(days=180)
        
        # 1. Obtener proveedores extranjeros (país != "MX") desde la tabla proveedores
        query_proveedores_extranjeros = select(Proveedor.codigo_proveedor).where(
            or_(
                Proveedor.pais != "MX",
                Proveedor.pais.is_(None)  # Incluir proveedores sin país definido
            )
        )
        result_proveedores_extranjeros = await db.execute(query_proveedores_extranjeros)
        proveedores_extranjeros = set(row[0] for row in result_proveedores_extranjeros.fetchall() if row[0])
        
        # 2. Obtener todos los código_proveedor con compras en los últimos 6 meses
        # Solo considerar proveedores extranjeros (país != "MX")
        query_proveedores_con_compras = select(Compra.codigo_proveedor).where(
            Compra.codigo_proveedor.isnot(None),
            Compra.posting_date.isnot(None),
            Compra.posting_date >= fecha_limite_compras,
            Compra.codigo_proveedor.in_(proveedores_extranjeros)  # Solo proveedores extranjeros
        ).distinct()
        result_proveedores_con_compras = await db.execute(query_proveedores_con_compras)
        proveedores_con_compras_recientes = set(row[0] for row in result_proveedores_con_compras.fetchall() if row[0])
        
        # 3. Obtener todos los proveedores actuales en carga_proveedores
        query_carga_proveedores = select(CargaProveedor)
        result_carga_proveedores = await db.execute(query_carga_proveedores)
        carga_proveedores_existentes = result_carga_proveedores.scalars().all()
        
        codigos_en_carga = set(cp.codigo_proveedor for cp in carga_proveedores_existentes)
        
        # Lista para almacenar proveedores a eliminar
        proveedores_a_eliminar = []
        
        # 4. Actualizar estatus de proveedores existentes (solo extranjeros)
        for carga_prov in carga_proveedores_existentes:
            try:
                # Verificar si el proveedor es extranjero (país != "MX")
                if carga_prov.pais and carga_prov.pais.upper() == "MX":
                    proveedores_omitidos_mx += 1
                    continue  # Omitir proveedores mexicanos
                
                estatus_anterior = carga_prov.estatus
                
                if carga_prov.codigo_proveedor in proveedores_con_compras_recientes:
                    # Tiene compras en los últimos 6 meses
                    
                    if estatus_anterior == "Alta":
                        # Es Alta, verificar si fue en el mismo mes
                        fecha_ultimo_alta = await obtener_fecha_ultimo_cambio_estatus_proveedor(db, carga_prov.codigo_proveedor, "Alta")
                        
                        if fecha_ultimo_alta and es_mismo_mes(fecha_ultimo_alta, hoy):
                            # Mismo mes del Alta, mantener como Alta
                            proveedores_sin_cambios += 1
                            continue
                        else:
                            # Mes diferente, cambiar a Sin modificacion
                            carga_prov.estatus = "Sin modificacion"
                            historial = CargaProveedorHistorial(
                                carga_proveedor_id=carga_prov.id,
                                codigo_proveedor=carga_prov.codigo_proveedor,
                                operacion=CargaProveedorOperacion.UPDATE,
                                estatus_anterior=estatus_anterior,
                                estatus_nuevo="Sin modificacion",
                                motivo="Actualización automática: proveedor con compras, cambio de Alta a Sin modificacion (mes diferente)"
                            )
                            db.add(historial)
                            proveedores_sin_modificacion += 1
                    
                    elif estatus_anterior == "Sin modificacion":
                        # Ya tiene Sin modificacion, verificar si ya se registró este mes
                        fecha_ultimo_cambio = await obtener_fecha_ultimo_cambio_estatus_proveedor(db, carga_prov.codigo_proveedor, "Sin modificacion")
                        
                        if fecha_ultimo_cambio and es_mismo_mes(fecha_ultimo_cambio, hoy):
                            # Ya se registró este mes, no hacer nada
                            proveedores_sin_cambios += 1
                            continue
                        else:
                            # Mes diferente, registrar nuevo Sin modificacion
                            historial = CargaProveedorHistorial(
                                carga_proveedor_id=carga_prov.id,
                                codigo_proveedor=carga_prov.codigo_proveedor,
                                operacion=CargaProveedorOperacion.UPDATE,
                                estatus_anterior=estatus_anterior,
                                estatus_nuevo="Sin modificacion",
                                motivo="Actualización automática: proveedor con compras en últimos 6 meses"
                            )
                            db.add(historial)
                            proveedores_sin_modificacion += 1
                    
                    elif estatus_anterior == "Baja":
                        # Estaba en Baja y ahora tiene compras → Alta (reactivación)
                        carga_prov.estatus = "Alta"
                        historial = CargaProveedorHistorial(
                            carga_proveedor_id=carga_prov.id,
                            codigo_proveedor=carga_prov.codigo_proveedor,
                            operacion=CargaProveedorOperacion.UPDATE,
                            estatus_anterior=estatus_anterior,
                            estatus_nuevo="Alta",
                            motivo="Actualización automática: proveedor reactivado con compras detectadas"
                        )
                        db.add(historial)
                        proveedores_nuevos += 1
                    
                    else:
                        # Otro estatus, cambiar a Alta
                        carga_prov.estatus = "Alta"
                        historial = CargaProveedorHistorial(
                            carga_proveedor_id=carga_prov.id,
                            codigo_proveedor=carga_prov.codigo_proveedor,
                            operacion=CargaProveedorOperacion.UPDATE,
                            estatus_anterior=estatus_anterior,
                            estatus_nuevo="Alta",
                            motivo="Actualización automática: proveedor reactivado con compras detectadas"
                        )
                        db.add(historial)
                        proveedores_nuevos += 1
                
                else:
                    # No tiene compras en los últimos 6 meses
                    
                    if estatus_anterior == "Baja":
                        # Ya estaba en Baja, verificar si fue en un mes diferente
                        fecha_ultima_baja = await obtener_fecha_ultimo_cambio_estatus_proveedor(db, carga_prov.codigo_proveedor, "Baja")
                        
                        if fecha_ultima_baja:
                            if es_mismo_mes(fecha_ultima_baja, hoy):
                                # Mismo mes, no hacer nada
                                proveedores_sin_cambios += 1
                                continue
                            else:
                                # Baja en mes diferente → Eliminar
                                proveedores_a_eliminar.append((carga_prov, fecha_ultima_baja))
                        else:
                            # No hay historial de baja, registrar como nueva baja
                            historial = CargaProveedorHistorial(
                                carga_proveedor_id=carga_prov.id,
                                codigo_proveedor=carga_prov.codigo_proveedor,
                                operacion=CargaProveedorOperacion.UPDATE,
                                estatus_anterior=estatus_anterior,
                                estatus_nuevo="Baja",
                                motivo="Actualización automática: proveedor sin compras en últimos 6 meses"
                            )
                            db.add(historial)
                            proveedores_marcados_baja += 1
                    else:
                        # Primera vez sin compras → Baja
                        carga_prov.estatus = "Baja"
                        historial = CargaProveedorHistorial(
                            carga_proveedor_id=carga_prov.id,
                            codigo_proveedor=carga_prov.codigo_proveedor,
                            operacion=CargaProveedorOperacion.UPDATE,
                            estatus_anterior=estatus_anterior,
                            estatus_nuevo="Baja",
                            motivo="Actualización automática: proveedor sin compras en últimos 6 meses"
                        )
                        db.add(historial)
                        proveedores_marcados_baja += 1
                        
            except Exception as e:
                errores.append(f"Error actualizando proveedor {carga_prov.codigo_proveedor}: {str(e)}")
        
        # 5. Eliminar proveedores que estuvieron en Baja dos meses seguidos
        for carga_prov, fecha_baja_anterior in proveedores_a_eliminar:
            try:
                # Registrar en historial antes de eliminar
                historial = CargaProveedorHistorial(
                    carga_proveedor_id=carga_prov.id,
                    codigo_proveedor=carga_prov.codigo_proveedor,
                    operacion=CargaProveedorOperacion.DELETE,
                    estatus_anterior="Baja",
                    estatus_nuevo=None,
                    datos_antes={
                        "id": carga_prov.id,
                        "codigo_proveedor": carga_prov.codigo_proveedor,
                        "nombre": carga_prov.nombre,
                        "pais": carga_prov.pais,
                        "domicilio": carga_prov.domicilio,
                        "cliente_proveedor": carga_prov.cliente_proveedor,
                        "estatus": carga_prov.estatus
                    },
                    motivo=f"Eliminación automática: proveedor con Baja dos meses seguidos (Baja anterior: {fecha_baja_anterior.strftime('%m/%Y')})"
                )
                db.add(historial)
                
                # Eliminar el proveedor
                await db.delete(carga_prov)
                proveedores_eliminados += 1
            except Exception as e:
                errores.append(f"Error eliminando proveedor {carga_prov.codigo_proveedor}: {str(e)}")
        
        # 6. Agregar proveedores nuevos (están en compras recientes pero no en carga_proveedores)
        # Solo proveedores extranjeros
        proveedores_nuevos_codigos = proveedores_con_compras_recientes - codigos_en_carga
        
        for codigo_proveedor in proveedores_nuevos_codigos:
            try:
                # VERIFICACIÓN DOBLE: Consultar directamente si ya existe en carga_proveedores
                query_existe = select(func.count(CargaProveedor.id)).where(
                    CargaProveedor.codigo_proveedor == codigo_proveedor
                )
                result_existe = await db.execute(query_existe)
                cantidad_existente = result_existe.scalar() or 0
                
                if cantidad_existente > 0:
                    # Ya existe, no agregar duplicado
                    continue
                
                # Obtener info del proveedor desde la tabla proveedores
                query_proveedor = select(Proveedor).where(Proveedor.codigo_proveedor == codigo_proveedor)
                result_proveedor = await db.execute(query_proveedor)
                proveedor_info = result_proveedor.scalar_one_or_none()
                
                # Verificar que no sea mexicano
                if proveedor_info and proveedor_info.pais and proveedor_info.pais.upper() == "MX":
                    proveedores_omitidos_mx += 1
                    continue  # Omitir proveedores mexicanos
                
                # Si no está en proveedores, obtener info de compras
                if proveedor_info:
                    nombre = proveedor_info.nombre
                    pais = proveedor_info.pais
                    domicilio = proveedor_info.domicilio
                else:
                    # Obtener nombre de la tabla compras
                    query_nombre = select(Compra.nombre_proveedor).where(
                        Compra.codigo_proveedor == codigo_proveedor,
                        Compra.nombre_proveedor.isnot(None)
                    ).limit(1)
                    result_nombre = await db.execute(query_nombre)
                    nombre_row = result_nombre.fetchone()
                    nombre = nombre_row[0] if nombre_row else None
                    pais = None
                    domicilio = None
                
                # Crear nuevo registro en carga_proveedores
                nuevo_carga_proveedor = CargaProveedor(
                    codigo_proveedor=codigo_proveedor,
                    nombre=nombre,
                    pais=pais,
                    domicilio=domicilio,
                    cliente_proveedor="Proveedor",
                    estatus="Alta"
                )
                db.add(nuevo_carga_proveedor)
                await db.flush()  # Para obtener el ID del nuevo registro
                
                # Registrar en historial
                historial = CargaProveedorHistorial(
                    carga_proveedor_id=nuevo_carga_proveedor.id,
                    codigo_proveedor=codigo_proveedor,
                    operacion=CargaProveedorOperacion.CREATE,
                    estatus_anterior=None,
                    estatus_nuevo="Alta",
                    datos_antes=None,
                    datos_despues={
                        "id": nuevo_carga_proveedor.id,
                        "codigo_proveedor": codigo_proveedor,
                        "nombre": nombre,
                        "pais": pais,
                        "domicilio": domicilio,
                        "cliente_proveedor": "Proveedor",
                        "estatus": "Alta"
                    },
                    motivo="Proveedor nuevo con compras en los últimos 6 meses"
                )
                db.add(historial)
                proveedores_nuevos += 1
            except Exception as e:
                errores.append(f"Error creando proveedor nuevo {codigo_proveedor}: {str(e)}")
        
        # Guardar cambios
        await db.commit()
        
        return {
            "exitoso": True,
            "proveedores_marcados_baja": proveedores_marcados_baja,
            "proveedores_sin_modificacion": proveedores_sin_modificacion,
            "proveedores_nuevos": proveedores_nuevos,
            "proveedores_omitidos_mx": proveedores_omitidos_mx,
            "proveedores_eliminados": proveedores_eliminados,
            "proveedores_sin_cambios": proveedores_sin_cambios,
            "total_procesados": proveedores_marcados_baja + proveedores_sin_modificacion + proveedores_nuevos + proveedores_eliminados,
            "errores": errores if errores else None
        }
        
    except Exception as e:
        await db.rollback()
        return {
            "exitoso": False,
            "error": str(e),
            "proveedores_marcados_baja": proveedores_marcados_baja,
            "proveedores_sin_modificacion": proveedores_sin_modificacion,
            "proveedores_nuevos": proveedores_nuevos,
            "proveedores_omitidos_mx": proveedores_omitidos_mx,
            "proveedores_eliminados": proveedores_eliminados,
            "proveedores_sin_cambios": proveedores_sin_cambios
        }


async def obtener_fecha_ultimo_cambio_estatus_proveedor_nacional(db: AsyncSession, codigo_proveedor: str, estatus: str) -> Optional[date]:
    """Obtiene la fecha del último registro en el historial nacional donde el proveedor cambió al estatus dado."""
    query = select(CargaProveedoresNacionalHistorial).where(
        CargaProveedoresNacionalHistorial.codigo_proveedor == int(codigo_proveedor),
        CargaProveedoresNacionalHistorial.estatus_nuevo == estatus
    ).order_by(desc(CargaProveedoresNacionalHistorial.created_at)).limit(1)
    result = await db.execute(query)
    historial = result.scalar_one_or_none()
    if historial and historial.created_at:
        return historial.created_at.date()
    return None


async def actualizar_estatus_carga_proveedores_nacional_por_compras(db: AsyncSession) -> Dict[str, Any]:
    """
    Actualiza los estatus de carga_proveedores_nacional basándose en compras de los últimos 6 meses.
    Solo considera proveedores con país = "MX" (México).
    Mismas reglas que actualizar_estatus_carga_proveedores_por_compras pero filtro inverso (solo MX).
    """
    proveedores_marcados_baja = 0
    proveedores_sin_modificacion = 0
    proveedores_nuevos = 0
    proveedores_eliminados = 0
    proveedores_sin_cambios = 0
    hoy = date.today()
    errores = []
    proveedores_a_eliminar = []

    try:
        fecha_limite_compras = datetime.now(timezone.utc) - timedelta(days=180)

        # 1. Solo proveedores MX
        query_proveedores_mx = select(Proveedor.codigo_proveedor).where(
            Proveedor.pais.isnot(None),
            func.upper(Proveedor.pais) == "MX"
        )
        result_mx = await db.execute(query_proveedores_mx)
        proveedores_mx = set(row[0] for row in result_mx.fetchall() if row[0])

        # 2. De esos MX, cuáles tienen compras en últimos 6 meses
        query_con_compras = select(Compra.codigo_proveedor).where(
            Compra.codigo_proveedor.isnot(None),
            Compra.posting_date.isnot(None),
            Compra.posting_date >= fecha_limite_compras,
            Compra.codigo_proveedor.in_(proveedores_mx)
        ).distinct()
        result_compras = await db.execute(query_con_compras)
        proveedores_con_compras_recientes = set(row[0] for row in result_compras.fetchall() if row[0])

        # 3. Registros actuales en carga_proveedores_nacional
        query_carga = select(CargaProveedoresNacional)
        result_carga = await db.execute(query_carga)
        carga_existentes = result_carga.scalars().all()
        codigos_en_carga = set(cp.codigo_proveedor for cp in carga_existentes if cp.codigo_proveedor)

        for carga_prov in carga_existentes:
            try:
                if not carga_prov.pais or carga_prov.pais.upper() != "MX":
                    continue
                estatus_anterior = carga_prov.estatus

                if carga_prov.codigo_proveedor in proveedores_con_compras_recientes:
                    if estatus_anterior == "Alta":
                        fecha_ultimo_alta = await obtener_fecha_ultimo_cambio_estatus_proveedor_nacional(db, carga_prov.codigo_proveedor, "Alta")
                        if fecha_ultimo_alta and es_mismo_mes(fecha_ultimo_alta, hoy):
                            proveedores_sin_cambios += 1
                            continue
                        carga_prov.estatus = "Sin modificacion"
                        carga_prov.operacion = "UPDATE"
                        db.add(CargaProveedoresNacionalHistorial(
                            carga_proveedores_nacional_id=carga_prov.id,
                            codigo_proveedor=carga_prov.codigo_proveedor,
                            operacion="UPDATE",
                            estatus_anterior=estatus_anterior,
                            estatus_nuevo="Sin modificacion",
                            motivo="Actualización automática: proveedor con compras, cambio de Alta a Sin modificacion (mes diferente)"
                        ))
                        proveedores_sin_modificacion += 1
                    elif estatus_anterior == "Sin modificacion":
                        fecha_ultimo = await obtener_fecha_ultimo_cambio_estatus_proveedor_nacional(db, carga_prov.codigo_proveedor, "Sin modificacion")
                        if fecha_ultimo and es_mismo_mes(fecha_ultimo, hoy):
                            proveedores_sin_cambios += 1
                            continue
                        db.add(CargaProveedoresNacionalHistorial(
                            carga_proveedores_nacional_id=carga_prov.id,
                            codigo_proveedor=carga_prov.codigo_proveedor,
                            operacion="UPDATE",
                            estatus_anterior=estatus_anterior,
                            estatus_nuevo="Sin modificacion",
                            motivo="Actualización automática: proveedor con compras en últimos 6 meses"
                        ))
                        proveedores_sin_modificacion += 1
                    elif estatus_anterior == "Baja":
                        carga_prov.estatus = "Alta"
                        carga_prov.operacion = "UPDATE"
                        db.add(CargaProveedoresNacionalHistorial(
                            carga_proveedores_nacional_id=carga_prov.id,
                            codigo_proveedor=carga_prov.codigo_proveedor,
                            operacion="UPDATE",
                            estatus_anterior=estatus_anterior,
                            estatus_nuevo="Alta",
                            motivo="Actualización automática: proveedor reactivado con compras detectadas"
                        ))
                        proveedores_nuevos += 1
                    else:
                        carga_prov.estatus = "Alta"
                        carga_prov.operacion = "UPDATE"
                        db.add(CargaProveedoresNacionalHistorial(
                            carga_proveedores_nacional_id=carga_prov.id,
                            codigo_proveedor=carga_prov.codigo_proveedor,
                            operacion="UPDATE",
                            estatus_anterior=estatus_anterior,
                            estatus_nuevo="Alta",
                            motivo="Actualización automática: proveedor reactivado con compras detectadas"
                        ))
                        proveedores_nuevos += 1
                else:
                    if estatus_anterior == "Baja":
                        fecha_ultima_baja = await obtener_fecha_ultimo_cambio_estatus_proveedor_nacional(db, carga_prov.codigo_proveedor, "Baja")
                        if fecha_ultima_baja:
                            if es_mismo_mes(fecha_ultima_baja, hoy):
                                proveedores_sin_cambios += 1
                                continue
                            proveedores_a_eliminar.append((carga_prov, fecha_ultima_baja))
                        else:
                            db.add(CargaProveedoresNacionalHistorial(
                                carga_proveedores_nacional_id=carga_prov.id,
                                codigo_proveedor=carga_prov.codigo_proveedor,
                                operacion="UPDATE",
                                estatus_anterior=estatus_anterior,
                                estatus_nuevo="Baja",
                                motivo="Actualización automática: proveedor sin compras en últimos 6 meses"
                            ))
                            proveedores_marcados_baja += 1
                    else:
                        carga_prov.estatus = "Baja"
                        carga_prov.operacion = "UPDATE"
                        db.add(CargaProveedoresNacionalHistorial(
                            carga_proveedores_nacional_id=carga_prov.id,
                            codigo_proveedor=carga_prov.codigo_proveedor,
                            operacion="UPDATE",
                            estatus_anterior=estatus_anterior,
                            estatus_nuevo="Baja",
                            motivo="Actualización automática: proveedor sin compras en últimos 6 meses"
                        ))
                        proveedores_marcados_baja += 1
            except Exception as e:
                errores.append(f"Error actualizando proveedor nacional {getattr(carga_prov, 'codigo_proveedor', '?')}: {str(e)}")

        for carga_prov, fecha_baja_anterior in proveedores_a_eliminar:
            try:
                db.add(CargaProveedoresNacionalHistorial(
                    carga_proveedores_nacional_id=carga_prov.id,
                    codigo_proveedor=carga_prov.codigo_proveedor,
                    operacion="DELETE",
                    estatus_anterior="Baja",
                    estatus_nuevo=None,
                    motivo=f"Eliminación automática: proveedor con Baja dos meses seguidos (Baja anterior: {fecha_baja_anterior.strftime('%m/%Y')})"
                ))
                await db.delete(carga_prov)
                proveedores_eliminados += 1
            except Exception as e:
                errores.append(f"Error eliminando proveedor nacional {getattr(carga_prov, 'codigo_proveedor', '?')}: {str(e)}")

        # Nuevos: en compras recientes MX y no en carga_proveedores_nacional
        proveedores_nuevos_codigos = proveedores_con_compras_recientes - codigos_en_carga
        for codigo_proveedor in proveedores_nuevos_codigos:
            try:
                result_existe = await db.execute(select(func.count(CargaProveedoresNacional.id)).where(CargaProveedoresNacional.codigo_proveedor == codigo_proveedor))
                if (result_existe.scalar() or 0) > 0:
                    continue
                result_prov = await db.execute(select(Proveedor).where(Proveedor.codigo_proveedor == codigo_proveedor))
                proveedor_info = result_prov.scalar_one_or_none()
                if proveedor_info and proveedor_info.pais and proveedor_info.pais.upper() != "MX":
                    continue
                nombre = domicilio = rfc = None
                pais = "MX"
                if proveedor_info:
                    nombre = proveedor_info.nombre
                    domicilio = proveedor_info.domicilio
                else:
                    row_nombre = await db.execute(select(Compra.nombre_proveedor).where(Compra.codigo_proveedor == codigo_proveedor, Compra.nombre_proveedor.isnot(None)).limit(1))
                    nombre = (row_nombre.fetchone() or (None,))[0]
                nuevo = CargaProveedoresNacional(
                    codigo_proveedor=codigo_proveedor,
                    nombre=nombre,
                    pais=pais,
                    domicilio=domicilio,
                    rfc=rfc,
                    cliente_proveedor="Proveedor",
                    estatus="Alta",
                    operacion="CREATE"
                )
                db.add(nuevo)
                await db.flush()
                db.add(CargaProveedoresNacionalHistorial(
                    carga_proveedores_nacional_id=nuevo.id,
                    codigo_proveedor=codigo_proveedor,
                    operacion="CREATE",
                    estatus_anterior=None,
                    estatus_nuevo="Alta",
                    motivo="Proveedor nuevo (MX) con compras en los últimos 6 meses"
                ))
                proveedores_nuevos += 1
            except Exception as e:
                errores.append(f"Error creando proveedor nacional {codigo_proveedor}: {str(e)}")

        # Siempre registrar una entrada de ejecución en el historial (para ya_actualizado y trazabilidad)
        motivo_ejecucion = f"Ejecución completada. Nuevos: {proveedores_nuevos}, Sin modificación: {proveedores_sin_modificacion}, Baja: {proveedores_marcados_baja}, Eliminados: {proveedores_eliminados}, Sin cambios: {proveedores_sin_cambios}"
        db.add(CargaProveedoresNacionalHistorial(
            carga_proveedores_nacional_id=None,
            codigo_proveedor=None,
            operacion="EJECUCION",
            estatus_anterior=None,
            estatus_nuevo=None,
            motivo=motivo_ejecucion
        ))

        await db.commit()
        return {
            "exitoso": True,
            "proveedores_marcados_baja": proveedores_marcados_baja,
            "proveedores_sin_modificacion": proveedores_sin_modificacion,
            "proveedores_nuevos": proveedores_nuevos,
            "proveedores_eliminados": proveedores_eliminados,
            "proveedores_sin_cambios": proveedores_sin_cambios,
            "total_procesados": proveedores_marcados_baja + proveedores_sin_modificacion + proveedores_nuevos + proveedores_eliminados,
            "errores": errores if errores else None
        }
    except Exception as e:
        await db.rollback()
        return {
            "exitoso": False,
            "error": str(e),
            "proveedores_marcados_baja": proveedores_marcados_baja,
            "proveedores_sin_modificacion": proveedores_sin_modificacion,
            "proveedores_nuevos": proveedores_nuevos,
            "proveedores_eliminados": proveedores_eliminados,
            "proveedores_sin_cambios": proveedores_sin_cambios,
        }


# ============================================================================
# CRUD para CargaProveedorHistorial
# ============================================================================

async def list_carga_proveedor_historial(
    db: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    codigo_proveedor: Optional[str] = None,
    operacion: Optional[str] = None
) -> List[CargaProveedorHistorial]:
    """Lista el historial de cambios en carga_proveedores con filtros opcionales."""
    query = select(CargaProveedorHistorial)

    if codigo_proveedor:
        query = query.where(CargaProveedorHistorial.codigo_proveedor.cast(String).ilike(f"%{codigo_proveedor}%"))

    if operacion:
        query = query.where(CargaProveedorHistorial.operacion == operacion)

    query = query.order_by(desc(CargaProveedorHistorial.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_carga_proveedor_historial(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    operacion: Optional[str] = None
) -> int:
    """Cuenta el total de registros en el historial con filtros opcionales."""
    query = select(func.count(CargaProveedorHistorial.id))

    if codigo_proveedor:
        query = query.where(CargaProveedorHistorial.codigo_proveedor.cast(String).ilike(f"%{codigo_proveedor}%"))

    if operacion:
        query = query.where(CargaProveedorHistorial.operacion == operacion)
    
    result = await db.execute(query)
    return result.scalar() or 0


async def count_carga_proveedor_historial_este_mes(db: AsyncSession) -> int:
    """Cuenta registros del historial de carga proveedores creados en el mes actual."""
    ahora = datetime.now(timezone.utc)
    inicio_mes = ahora.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    query = select(func.count(CargaProveedorHistorial.id)).where(
        CargaProveedorHistorial.created_at >= inicio_mes
    )
    result = await db.execute(query)
    return result.scalar() or 0


async def get_historial_por_proveedor(
    db: AsyncSession,
    codigo_proveedor: str,
    limit: int = 50
) -> List[CargaProveedorHistorial]:
    """Obtiene el historial de un proveedor específico."""
    query = select(CargaProveedorHistorial).where(
        CargaProveedorHistorial.codigo_proveedor == int(codigo_proveedor)
    ).order_by(desc(CargaProveedorHistorial.created_at)).limit(limit)
    
    result = await db.execute(query)
    return list(result.scalars().all())


# ============================================================================
# CRUD para CargaProveedoresNacional
# ============================================================================

async def list_carga_proveedores_nacional(
    db: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    codigo_proveedor: Optional[str] = None,
    estatus: Optional[str] = None,
    operacion: Optional[str] = None
) -> List[CargaProveedoresNacional]:
    """Lista registros de carga de proveedores nacionales con filtros opcionales."""
    query = select(CargaProveedoresNacional)
    if codigo_proveedor:
        query = query.where(CargaProveedoresNacional.codigo_proveedor.cast(String).ilike(f"%{codigo_proveedor}%"))
    if estatus:
        query = query.where(CargaProveedoresNacional.estatus == estatus)
    if operacion:
        query = query.where(CargaProveedoresNacional.operacion == operacion)
    query = query.order_by(desc(CargaProveedoresNacional.created_at)).limit(limit).offset(offset)
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_carga_proveedores_nacional(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    estatus: Optional[str] = None,
    operacion: Optional[str] = None
) -> int:
    """Cuenta el total de registros de carga de proveedores nacionales con filtros opcionales."""
    query = select(func.count(CargaProveedoresNacional.id))
    if codigo_proveedor:
        query = query.where(CargaProveedoresNacional.codigo_proveedor.cast(String).ilike(f"%{codigo_proveedor}%"))
    if estatus:
        query = query.where(CargaProveedoresNacional.estatus == estatus)
    if operacion:
        query = query.where(CargaProveedoresNacional.operacion == operacion)
    result = await db.execute(query)
    return result.scalar() or 0


async def list_carga_proveedores_nacional_historial(
    db: AsyncSession,
    limit: int = 50,
    offset: int = 0,
    codigo_proveedor: Optional[str] = None,
    operacion: Optional[str] = None
) -> List[CargaProveedoresNacionalHistorial]:
    """Lista el historial de cambios en carga_proveedores_nacional."""
    query = select(CargaProveedoresNacionalHistorial)
    if codigo_proveedor:
        query = query.where(CargaProveedoresNacionalHistorial.codigo_proveedor.cast(String).ilike(f"%{codigo_proveedor}%"))
    if operacion:
        query = query.where(CargaProveedoresNacionalHistorial.operacion == operacion)
    query = query.order_by(desc(CargaProveedoresNacionalHistorial.created_at)).limit(limit).offset(offset)
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_carga_proveedores_nacional_historial(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    operacion: Optional[str] = None
) -> int:
    """Cuenta el total de registros en el historial de carga proveedores nacionales."""
    query = select(func.count(CargaProveedoresNacionalHistorial.id))
    if codigo_proveedor:
        query = query.where(CargaProveedoresNacionalHistorial.codigo_proveedor.cast(String).ilike(f"%{codigo_proveedor}%"))
    if operacion:
        query = query.where(CargaProveedoresNacionalHistorial.operacion == operacion)
    result = await db.execute(query)
    return result.scalar() or 0


async def count_carga_proveedores_nacional_historial_este_mes(db: AsyncSession) -> int:
    """Cuenta registros del historial de carga proveedores nacionales creados en el mes actual."""
    ahora = datetime.now(timezone.utc)
    inicio_mes = ahora.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    query = select(func.count(CargaProveedoresNacionalHistorial.id)).where(
        CargaProveedoresNacionalHistorial.created_at >= inicio_mes
    )
    result = await db.execute(query)
    return result.scalar() or 0


# ============================================================================
# CRUD para CargaCliente
# ============================================================================

async def create_carga_cliente(
    db: AsyncSession,
    codigo_cliente: int,
    nombre: Optional[str] = None,
    pais: Optional[str] = None,
    domicilio: Optional[str] = None,
    cliente_proveedor: Optional[str] = None,
    estatus: Optional[str] = None
) -> CargaCliente:
    """Crea un nuevo registro de carga de cliente."""
    carga_cliente = CargaCliente(
        codigo_cliente=codigo_cliente,
        nombre=nombre,
        pais=pais,
        domicilio=domicilio,
        cliente_proveedor=cliente_proveedor,
        estatus=estatus
    )
    db.add(carga_cliente)
    await db.commit()
    await db.refresh(carga_cliente)
    return carga_cliente


async def get_carga_cliente_by_id(db: AsyncSession, carga_id: int) -> Optional[CargaCliente]:
    """Obtiene un registro de carga de cliente por ID."""
    result = await db.execute(
        select(CargaCliente).where(CargaCliente.id == carga_id)
    )
    return result.scalar_one_or_none()


async def get_carga_cliente_by_codigo(db: AsyncSession, codigo_cliente: int) -> Optional[CargaCliente]:
    """Obtiene un registro de carga de cliente por código de cliente."""
    result = await db.execute(
        select(CargaCliente)
        .where(CargaCliente.codigo_cliente == codigo_cliente)
        .order_by(desc(CargaCliente.created_at))
        .limit(1)
    )
    return result.scalar_one_or_none()


async def list_carga_clientes(
    db: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    codigo_cliente: Optional[int] = None,
    cliente_proveedor: Optional[str] = None,
    estatus: Optional[str] = None,
    search: Optional[str] = None
) -> List[CargaCliente]:
    """Lista registros de carga de clientes con filtros opcionales. search filtra por código o nombre."""
    query = select(CargaCliente)

    if search and search.strip():
        search = search.strip()
        if search.isdigit():
            query = query.where(
                or_(
                    CargaCliente.codigo_cliente == int(search),
                    CargaCliente.nombre.ilike(f"%{search}%")
                )
            )
        else:
            query = query.where(CargaCliente.nombre.ilike(f"%{search}%"))
    elif codigo_cliente is not None:
        query = query.where(CargaCliente.codigo_cliente == codigo_cliente)

    if cliente_proveedor:
        query = query.where(CargaCliente.cliente_proveedor.ilike(f"%{cliente_proveedor}%"))

    if estatus:
        query = query.where(CargaCliente.estatus == estatus)

    query = query.order_by(desc(CargaCliente.created_at)).limit(limit).offset(offset)

    result = await db.execute(query)
    return list(result.scalars().all())


async def count_carga_clientes(
    db: AsyncSession,
    codigo_cliente: Optional[int] = None,
    cliente_proveedor: Optional[str] = None,
    estatus: Optional[str] = None,
    search: Optional[str] = None
) -> int:
    """Cuenta el total de registros de carga de clientes con filtros opcionales. search filtra por código o nombre."""
    query = select(func.count(CargaCliente.id))

    if search and search.strip():
        search = search.strip()
        if search.isdigit():
            query = query.where(
                or_(
                    CargaCliente.codigo_cliente == int(search),
                    CargaCliente.nombre.ilike(f"%{search}%")
                )
            )
        else:
            query = query.where(CargaCliente.nombre.ilike(f"%{search}%"))
    elif codigo_cliente is not None:
        query = query.where(CargaCliente.codigo_cliente == codigo_cliente)

    if cliente_proveedor:
        query = query.where(CargaCliente.cliente_proveedor.ilike(f"%{cliente_proveedor}%"))

    if estatus:
        query = query.where(CargaCliente.estatus == estatus)

    result = await db.execute(query)
    return result.scalar() or 0


async def actualizar_domicilio_pais_carga_clientes_desde_clientes(db: AsyncSession) -> Dict[str, int]:
    """
    Completa domicilio y pais en carga_clientes usando la tabla clientes.

    Solo actualiza registros de carga_clientes que tengan domicilio o pais vacios/nulos
    y cuando en clientes exista al menos uno de esos datos con valor.
    """
    pendientes_result = await db.execute(
        select(func.count(CargaCliente.id)).where(
            or_(
                CargaCliente.domicilio.is_(None),
                func.btrim(CargaCliente.domicilio) == "",
                CargaCliente.pais.is_(None),
                func.btrim(CargaCliente.pais) == "",
            )
        )
    )
    total_pendientes = int(pendientes_result.scalar() or 0)

    update_stmt = text(
        """
        UPDATE carga_clientes cc
        SET
            domicilio = CASE
                WHEN (cc.domicilio IS NULL OR btrim(cc.domicilio) = '')
                     AND c.domicilio IS NOT NULL
                     AND btrim(c.domicilio) <> ''
                THEN c.domicilio
                ELSE cc.domicilio
            END,
            pais = CASE
                WHEN (cc.pais IS NULL OR btrim(cc.pais) = '')
                     AND c.pais IS NOT NULL
                     AND btrim(c.pais) <> ''
                THEN c.pais
                ELSE cc.pais
            END,
            updated_at = NOW()
        FROM clientes c
        WHERE cc.codigo_cliente = c.codigo_cliente
          AND (
                (
                    (cc.domicilio IS NULL OR btrim(cc.domicilio) = '')
                    AND c.domicilio IS NOT NULL
                    AND btrim(c.domicilio) <> ''
                )
                OR
                (
                    (cc.pais IS NULL OR btrim(cc.pais) = '')
                    AND c.pais IS NOT NULL
                    AND btrim(c.pais) <> ''
                )
              )
        """
    )
    update_result = await db.execute(update_stmt)
    actualizados = int(update_result.rowcount or 0)
    await db.commit()

    return {
        "total_pendientes": total_pendientes,
        "actualizados": actualizados,
        "sin_actualizar": max(total_pendientes - actualizados, 0),
    }


async def update_carga_cliente(
    db: AsyncSession,
    carga_id: int,
    codigo_cliente: Optional[int] = None,
    nombre: Optional[str] = None,
    pais: Optional[str] = None,
    domicilio: Optional[str] = None,
    cliente_proveedor: Optional[str] = None,
    estatus: Optional[str] = None
) -> Optional[CargaCliente]:
    """Actualiza un registro de carga de cliente."""
    carga_cliente = await get_carga_cliente_by_id(db, carga_id)
    if not carga_cliente:
        return None
    
    if codigo_cliente is not None:
        carga_cliente.codigo_cliente = codigo_cliente
    if nombre is not None:
        carga_cliente.nombre = nombre
    if pais is not None:
        carga_cliente.pais = pais
    if domicilio is not None:
        carga_cliente.domicilio = domicilio
    if cliente_proveedor is not None:
        carga_cliente.cliente_proveedor = cliente_proveedor
    if estatus is not None:
        carga_cliente.estatus = estatus
    
    await db.commit()
    await db.refresh(carga_cliente)
    return carga_cliente


async def delete_carga_cliente(db: AsyncSession, carga_id: int) -> bool:
    """Elimina un registro de carga de cliente."""
    carga_cliente = await get_carga_cliente_by_id(db, carga_id)
    if not carga_cliente:
        return False
    
    await db.delete(carga_cliente)
    await db.commit()
    return True


# ============================================================================
# CRUD para CargaClienteHistorial
# ============================================================================

async def list_carga_cliente_historial(
    db: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    codigo_cliente: Optional[int] = None,
    operacion: Optional[str] = None
) -> List[CargaClienteHistorial]:
    """Lista el historial de cambios en carga_clientes con filtros opcionales."""
    query = select(CargaClienteHistorial)
    
    if codigo_cliente:
        query = query.where(CargaClienteHistorial.codigo_cliente == codigo_cliente)
    
    if operacion:
        query = query.where(CargaClienteHistorial.operacion == operacion)
    
    query = query.order_by(desc(CargaClienteHistorial.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_carga_cliente_historial(
    db: AsyncSession,
    codigo_cliente: Optional[int] = None,
    operacion: Optional[str] = None
) -> int:
    """Cuenta el total de registros en el historial con filtros opcionales."""
    query = select(func.count(CargaClienteHistorial.id))
    
    if codigo_cliente:
        query = query.where(CargaClienteHistorial.codigo_cliente == codigo_cliente)
    
    if operacion:
        query = query.where(CargaClienteHistorial.operacion == operacion)
    
    result = await db.execute(query)
    return result.scalar() or 0


async def count_carga_cliente_historial_este_mes(db: AsyncSession) -> int:
    """Cuenta registros del historial de carga clientes creados en el mes actual."""
    ahora = datetime.now(timezone.utc)
    inicio_mes = ahora.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    query = select(func.count(CargaClienteHistorial.id)).where(
        CargaClienteHistorial.created_at >= inicio_mes
    )
    result = await db.execute(query)
    return result.scalar() or 0


async def get_historial_por_cliente(
    db: AsyncSession,
    codigo_cliente: int,
    limit: int = 50
) -> List[CargaClienteHistorial]:
    """Obtiene el historial de un cliente específico."""
    query = select(CargaClienteHistorial).where(
        CargaClienteHistorial.codigo_cliente == codigo_cliente
    ).order_by(desc(CargaClienteHistorial.created_at)).limit(limit)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def create_carga_cliente_historial(
    db: AsyncSession,
    carga_cliente_id: Optional[int] = None,
    codigo_cliente: Optional[int] = None,
    operacion: CargaClienteOperacion = CargaClienteOperacion.CREATE,
    estatus_anterior: Optional[str] = None,
    estatus_nuevo: Optional[str] = None,
    datos_antes: Optional[Dict[str, Any]] = None,
    datos_despues: Optional[Dict[str, Any]] = None,
    motivo: Optional[str] = None
) -> CargaClienteHistorial:
    """Crea un nuevo registro en el historial de carga de clientes."""
    historial = CargaClienteHistorial(
        carga_cliente_id=carga_cliente_id,
        codigo_cliente=codigo_cliente,
        operacion=operacion,
        estatus_anterior=estatus_anterior,
        estatus_nuevo=estatus_nuevo,
        datos_antes=datos_antes,
        datos_despues=datos_despues,
        motivo=motivo
    )
    db.add(historial)
    await db.commit()
    await db.refresh(historial)
    return historial


async def get_clientes_con_ventas_ultimos_6_meses(
    db: AsyncSession
) -> List[Dict[str, Any]]:
    """
    Obtiene los clientes únicos con ventas en los últimos 6 meses
    donde region_asc != 'MX  Mexiko' (exportaciones/internacionales).
    Retorna lista de diccionarios con codigo_cliente y datos del cliente.
    """
    from app.db.models import Venta
    from dateutil.relativedelta import relativedelta
    
    # Calcular fecha de hace 6 meses
    fecha_limite = date.today() - relativedelta(months=6)
    
    # Query para obtener clientes únicos con ventas internacionales en últimos 6 meses
    query = select(
        Venta.codigo_cliente,
        Venta.cliente,
        Venta.ship_to_party
    ).where(
        Venta.region_asc != "MX  Mexiko",
        Venta.region_asc.isnot(None),
        Venta.codigo_cliente.isnot(None),
        Venta.periodo >= fecha_limite
    ).distinct(Venta.codigo_cliente)
    
    result = await db.execute(query)
    rows = result.all()
    
    clientes = []
    for row in rows:
        clientes.append({
            "codigo_cliente": row.codigo_cliente,
            "nombre": row.cliente,
            "ship_to_party": row.ship_to_party
        })
    
    return clientes


async def get_todos_codigos_clientes_carga(db: AsyncSession) -> Dict[int, CargaCliente]:
    """
    Obtiene todos los clientes de carga_clientes como diccionario
    con codigo_cliente como clave.
    """
    query = select(CargaCliente)
    result = await db.execute(query)
    clientes = result.scalars().all()
    
    return {c.codigo_cliente: c for c in clientes}


async def ensure_sequence_is_synced(
    db: AsyncSession,
    *,
    table_name: str,
    column_attr
) -> None:
    """
    Garantiza que la secuencia autoincremental asociada a una tabla no se
    quede detrás del valor máximo existente en la columna dada.
    Esto evita errores de clave duplicada cuando la tabla se pobló mediante
    cargas manuales que no actualizaron la secuencia.
    """
    seq_result = await db.execute(
        text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
        {"table_name": table_name, "column_name": column_attr.key}
    )
    seq_name = seq_result.scalar()
    if not seq_name:
        return
    
    # Obtener el estado actual de la secuencia
    seq_state_result = await db.execute(
        text(f"SELECT last_value, is_called FROM {seq_name}")
    )
    seq_state = seq_state_result.first()
    if not seq_state:
        return
    
    max_id_result = await db.execute(select(func.max(column_attr)))
    max_id = max_id_result.scalar() or 0
    if max_id == 0:
        return
    
    current_value = seq_state.last_value if seq_state.is_called else 0
    if max_id > current_value:
        await db.execute(
            text("SELECT setval(:seq_name, :max_id, true)"),
            {"seq_name": seq_name, "max_id": max_id}
        )


async def obtener_fecha_ultimo_cambio_estatus(db: AsyncSession, codigo_cliente: int, estatus: str) -> Optional[date]:
    """
    Obtiene la fecha del último registro en el historial donde el cliente
    cambió a un estatus específico. Retorna None si no hay registro.
    """
    query = select(CargaClienteHistorial).where(
        CargaClienteHistorial.codigo_cliente == codigo_cliente,
        CargaClienteHistorial.estatus_nuevo == estatus
    ).order_by(desc(CargaClienteHistorial.created_at)).limit(1)
    
    result = await db.execute(query)
    historial = result.scalar_one_or_none()
    
    if historial and historial.created_at:
        return historial.created_at.date()
    return None


def es_mismo_mes(fecha1: date, fecha2: date) -> bool:
    """Verifica si dos fechas están en el mismo mes calendario."""
    return fecha1.month == fecha2.month and fecha1.year == fecha2.year


async def actualizar_carga_clientes_desde_ventas(db: AsyncSession) -> Dict[str, Any]:
    """
    Actualiza la tabla carga_clientes basándose en las ventas:
    - Si el cliente existe pero NO tiene ventas en últimos 6 meses → Baja
    - Si el cliente existe y SÍ tiene ventas en últimos 6 meses → Sin modificacion
    - Si el cliente NO existe y tiene ventas → Alta (crear nuevo)
    - Si el cliente ya estaba en Baja en un MES DIFERENTE y sigue sin ventas → Eliminado
    
    IMPORTANTE: Los cambios de estatus solo se registran si ocurren en un mes calendario
    diferente al último cambio. Si se actualiza varias veces en el mismo mes con el mismo
    resultado, NO se registra duplicado en el historial.
    
    Retorna un resumen de los cambios realizados.
    """
    from app.db.models import Venta
    from dateutil.relativedelta import relativedelta
    
    resumen = {
        "altas": 0,
        "bajas": 0,
        "eliminados": 0,
        "sin_modificacion": 0,
        "sin_cambios": 0,  # Clientes que ya tenían el estatus correcto este mes
        "errores": 0,
        "detalles": []
    }
    
    # Obtener fecha actual para comparación
    hoy = date.today()
    
    # 1. Obtener clientes con ventas internacionales en últimos 6 meses
    clientes_con_ventas = await get_clientes_con_ventas_ultimos_6_meses(db)
    codigos_con_ventas = {c["codigo_cliente"] for c in clientes_con_ventas}
    
    # 2. Obtener todos los clientes actuales en carga_clientes
    clientes_actuales = await get_todos_codigos_clientes_carga(db)
    
    # Lista para almacenar clientes a eliminar (no podemos modificar el dict mientras iteramos)
    clientes_a_eliminar = []
    secuencia_carga_clientes_sincronizada = False
    
    # 3. Procesar clientes existentes
    for codigo_cliente, cliente_carga in clientes_actuales.items():
        estatus_anterior = cliente_carga.estatus
        
        if codigo_cliente in codigos_con_ventas:
            # Cliente tiene ventas en últimos 6 meses
            
            if estatus_anterior == "Alta":
                # Es Alta, verificar si el Alta fue en el mismo mes
                fecha_ultimo_alta = await obtener_fecha_ultimo_cambio_estatus(db, codigo_cliente, "Alta")
                
                if fecha_ultimo_alta and es_mismo_mes(fecha_ultimo_alta, hoy):
                    # Mismo mes del Alta, mantener como Alta
                    resumen["sin_cambios"] += 1
                    continue
                else:
                    # Mes diferente, cambiar a Sin modificacion
                    nuevo_estatus = "Sin modificacion"
                    cliente_carga.estatus = nuevo_estatus
                    
                    historial = CargaClienteHistorial(
                        carga_cliente_id=cliente_carga.id,
                        codigo_cliente=codigo_cliente,
                        operacion=CargaClienteOperacion.UPDATE,
                        estatus_anterior=estatus_anterior,
                        estatus_nuevo=nuevo_estatus,
                        motivo="Actualización automática: cliente con ventas, cambio de Alta a Sin modificacion (mes diferente)"
                    )
                    db.add(historial)
                    
                    resumen["sin_modificacion"] += 1
                    resumen["detalles"].append({
                        "codigo_cliente": codigo_cliente,
                        "accion": "Sin modificacion",
                        "estatus_anterior": estatus_anterior
                    })
            
            elif estatus_anterior == "Sin modificacion":
                # Ya tiene Sin modificacion, verificar si ya se registró este mes
                fecha_ultimo_cambio = await obtener_fecha_ultimo_cambio_estatus(db, codigo_cliente, "Sin modificacion")
                
                if fecha_ultimo_cambio and es_mismo_mes(fecha_ultimo_cambio, hoy):
                    # Ya se registró este mes, no hacer nada
                    resumen["sin_cambios"] += 1
                    continue
                else:
                    # Mes diferente, registrar nuevo Sin modificacion
                    nuevo_estatus = "Sin modificacion"
                    
                    historial = CargaClienteHistorial(
                        carga_cliente_id=cliente_carga.id,
                        codigo_cliente=codigo_cliente,
                        operacion=CargaClienteOperacion.UPDATE,
                        estatus_anterior=estatus_anterior,
                        estatus_nuevo=nuevo_estatus,
                        motivo="Actualización automática: cliente con ventas en últimos 6 meses"
                    )
                    db.add(historial)
                    
                    resumen["sin_modificacion"] += 1
                    resumen["detalles"].append({
                        "codigo_cliente": codigo_cliente,
                        "accion": "Sin modificacion",
                        "estatus_anterior": estatus_anterior
                    })
            
            else:
                # Otro estatus (ej: Baja), cambiar a Alta (reactivación del cliente)
                nuevo_estatus = "Alta"
                cliente_carga.estatus = nuevo_estatus
                
                historial = CargaClienteHistorial(
                    carga_cliente_id=cliente_carga.id,
                    codigo_cliente=codigo_cliente,
                    operacion=CargaClienteOperacion.UPDATE,
                    estatus_anterior=estatus_anterior,
                    estatus_nuevo=nuevo_estatus,
                    motivo="Actualización automática: cliente reactivado con ventas detectadas"
                )
                db.add(historial)
                
                resumen["altas"] += 1
                resumen["detalles"].append({
                    "codigo_cliente": codigo_cliente,
                    "accion": "Alta",
                    "estatus_anterior": estatus_anterior
                })
        else:
            # Cliente NO tiene ventas en últimos 6 meses → Baja
            nuevo_estatus = "Baja"
            
            if estatus_anterior == "Baja":
                # Ya estaba en Baja → Verificar si la baja fue en un mes diferente
                fecha_ultima_baja = await obtener_fecha_ultimo_cambio_estatus(db, codigo_cliente, "Baja")
                
                if fecha_ultima_baja:
                    if es_mismo_mes(fecha_ultima_baja, hoy):
                        # Mismo mes, no hacer nada
                        resumen["sin_cambios"] += 1
                        continue
                    else:
                        # Baja en mes diferente → Eliminar
                        clientes_a_eliminar.append((codigo_cliente, cliente_carga, fecha_ultima_baja))
                else:
                    # No hay historial de baja, registrar como nueva baja
                    historial = CargaClienteHistorial(
                        carga_cliente_id=cliente_carga.id,
                        codigo_cliente=codigo_cliente,
                        operacion=CargaClienteOperacion.UPDATE,
                        estatus_anterior=estatus_anterior,
                        estatus_nuevo=nuevo_estatus,
                        motivo="Actualización automática: cliente sin ventas en últimos 6 meses"
                    )
                    db.add(historial)
                    
                    resumen["bajas"] += 1
                    resumen["detalles"].append({
                        "codigo_cliente": codigo_cliente,
                        "accion": "Baja",
                        "estatus_anterior": estatus_anterior
                    })
            else:
                # Cambio de otro estatus a Baja
                cliente_carga.estatus = nuevo_estatus
                
                # Registrar en historial
                historial = CargaClienteHistorial(
                    carga_cliente_id=cliente_carga.id,
                    codigo_cliente=codigo_cliente,
                    operacion=CargaClienteOperacion.UPDATE,
                    estatus_anterior=estatus_anterior,
                    estatus_nuevo=nuevo_estatus,
                    motivo="Actualización automática: cliente sin ventas en últimos 6 meses"
                )
                db.add(historial)
                
                resumen["bajas"] += 1
                resumen["detalles"].append({
                    "codigo_cliente": codigo_cliente,
                    "accion": "Baja",
                    "estatus_anterior": estatus_anterior
                })
    
    # 4. Eliminar clientes que estuvieron en Baja dos meses seguidos
    for codigo_cliente, cliente_carga, fecha_baja_anterior in clientes_a_eliminar:
        # Registrar en historial antes de eliminar
        historial = CargaClienteHistorial(
            carga_cliente_id=cliente_carga.id,
            codigo_cliente=codigo_cliente,
            operacion=CargaClienteOperacion.DELETE,
            estatus_anterior="Baja",
            estatus_nuevo=None,
            datos_antes={
                "id": cliente_carga.id,
                "codigo_cliente": cliente_carga.codigo_cliente,
                "nombre": cliente_carga.nombre,
                "pais": cliente_carga.pais,
                "domicilio": cliente_carga.domicilio,
                "cliente_proveedor": cliente_carga.cliente_proveedor,
                "estatus": cliente_carga.estatus
            },
            motivo=f"Eliminación automática: cliente con Baja dos meses seguidos (Baja anterior: {fecha_baja_anterior.strftime('%m/%Y')})"
        )
        db.add(historial)
        
        # Eliminar el cliente
        await db.delete(cliente_carga)
        
        resumen["eliminados"] += 1
        resumen["detalles"].append({
            "codigo_cliente": codigo_cliente,
            "accion": "Eliminado",
            "estatus_anterior": "Baja",
            "motivo": f"Baja en {fecha_baja_anterior.strftime('%m/%Y')} y {hoy.strftime('%m/%Y')}"
        })
    
    # 4. Crear nuevos clientes (los que tienen ventas pero no están en carga_clientes)
    for cliente_venta in clientes_con_ventas:
        codigo = cliente_venta["codigo_cliente"]
        if codigo not in clientes_actuales:
            if not secuencia_carga_clientes_sincronizada:
                await ensure_sequence_is_synced(
                    db,
                    table_name="carga_clientes",
                    column_attr=CargaCliente.id
                )
                secuencia_carga_clientes_sincronizada = True
            # Crear nuevo cliente con estatus Alta
            nuevo_cliente = CargaCliente(
                codigo_cliente=codigo,
                nombre=cliente_venta.get("nombre"),
                cliente_proveedor="Cliente",
                estatus="Alta"
            )
            db.add(nuevo_cliente)
            await db.flush()  # Para obtener el ID
            clientes_actuales[codigo] = nuevo_cliente
            
            # Registrar en historial
            historial = CargaClienteHistorial(
                carga_cliente_id=nuevo_cliente.id,
                codigo_cliente=codigo,
                operacion=CargaClienteOperacion.CREATE,
                estatus_anterior=None,
                estatus_nuevo="Alta",
                motivo="Actualización automática: nuevo cliente con ventas detectado"
            )
            db.add(historial)
            
            resumen["altas"] += 1
            resumen["detalles"].append({
                "codigo_cliente": codigo,
                "accion": "Alta",
                "nombre": cliente_venta.get("nombre")
            })
    
    # 5. Siempre registrar una entrada de ejecución en el historial (para ya_actualizado y trazabilidad)
    motivo_ejecucion = (
        f"Ejecución completada. Altas: {resumen['altas']}, Bajas: {resumen['bajas']}, "
        f"Sin modificación: {resumen['sin_modificacion']}, Eliminados: {resumen['eliminados']}, "
        f"Sin cambios: {resumen['sin_cambios']}"
    )
    db.add(CargaClienteHistorial(
        carga_cliente_id=None,
        codigo_cliente=None,
        operacion=CargaClienteOperacion.EJECUCION,
        estatus_anterior=None,
        estatus_nuevo=None,
        motivo=motivo_ejecucion
    ))
    
    # 6. Commit de todos los cambios
    await db.commit()
    
    return resumen


# ============================================================================
# CRUD para MasterUnificadoVirtuales
# ============================================================================

def master_unificado_virtual_to_dict(master: Optional[MasterUnificadoVirtuales]) -> Dict[str, Any]:
    """Convierte un registro de virtuales a diccionario para historial."""
    if not master:
        return {}
    return {
        "id": master.id,
        "numero": master.numero,
        "solicitud_previo": master.solicitud_previo,
        "agente": master.agente,
        "pedimento": master.pedimento,
        "aduana": master.aduana,
        "patente": master.patente,
        "destino": master.destino,
        "cliente_space": master.cliente_space,
        "impo_expo": master.impo_expo,
        "proveedor_cliente": master.proveedor_cliente,
        "mes": master.mes,
        "complemento": master.complemento,
        "tipo_immex": master.tipo_immex,
        "factura": master.factura,
        "fecha_pago": master.fecha_pago.isoformat() if master.fecha_pago else None,
        "informacion": master.informacion,
        "estatus": master.estatus,
        "op_regular": master.op_regular,
        "tipo": master.tipo,
        "carretes": master.carretes,
        "servicio_cliente": master.servicio_cliente,
        "plazo": master.plazo,
        "firma": master.firma,
        "incoterm": master.incoterm,
        "tipo_exportacion": master.tipo_exportacion,
        "escenario": master.escenario,
        "materialidad": master.materialidad,
        "created_at": master.created_at.isoformat() if master.created_at else None,
        "updated_at": master.updated_at.isoformat() if master.updated_at else None,
    }


def _add_master_unificado_virtual_historial_entry(
    db: AsyncSession,
    *,
    numero: Optional[int],
    operacion: MasterUnificadoVirtualOperacion,
    user_id: int,
    datos_antes: Optional[Dict[str, Any]] = None,
    datos_despues: Optional[Dict[str, Any]] = None,
    campos_modificados: Optional[List[str]] = None,
    comentario: Optional[str] = None,
) -> MasterUnificadoVirtualHistorial:
    """
    Agrega una entrada al historial sin hacer commit.
    Útil cuando se están registrando varios movimientos en la misma transacción.
    """
    historial = MasterUnificadoVirtualHistorial(
        numero=numero,
        operacion=operacion,
        user_id=user_id,
        datos_antes=datos_antes,
        datos_despues=datos_despues,
        campos_modificados=campos_modificados,
        comentario=comentario,
    )
    db.add(historial)
    return historial


async def create_master_unificado_virtual_historial(
    db: AsyncSession,
    *,
    numero: Optional[int],
    operacion: MasterUnificadoVirtualOperacion,
    user_id: int,
    datos_antes: Optional[Dict[str, Any]] = None,
    datos_despues: Optional[Dict[str, Any]] = None,
    campos_modificados: Optional[List[str]] = None,
    comentario: Optional[str] = None,
) -> MasterUnificadoVirtualHistorial:
    """Registra una entrada en el historial de virtuales y hace commit."""
    historial = _add_master_unificado_virtual_historial_entry(
        db,
        numero=numero,
        operacion=operacion,
        user_id=user_id,
        datos_antes=datos_antes,
        datos_despues=datos_despues,
        campos_modificados=campos_modificados,
        comentario=comentario,
    )
    await db.commit()
    await db.refresh(historial)
    return historial


async def create_master_unificado_virtuales(
    db: AsyncSession,
    solicitud_previo: Optional[bool] = None,
    agente: Optional[str] = None,
    pedimento: Optional[int] = None,
    aduana: Optional[int] = None,
    patente: Optional[int] = None,
    destino: Optional[int] = None,
    cliente_space: Optional[str] = None,
    impo_expo: Optional[str] = None,
    proveedor_cliente: Optional[str] = None,
    mes: Optional[str] = None,
    complemento: Optional[str] = None,
    tipo_immex: Optional[str] = None,
    factura: Optional[str] = None,
    fecha_pago: Optional[date] = None,
    informacion: Optional[str] = None,
    estatus: Optional[str] = None,
    op_regular: Optional[bool] = None,
    tipo: Optional[str] = None,
    numero: Optional[int] = None,
    carretes: Optional[bool] = None,
    servicio_cliente: Optional[str] = None,
    plazo: Optional[str] = None,
    firma: Optional[str] = None,
    incoterm: Optional[str] = None,
    tipo_exportacion: Optional[str] = None,
    escenario: Optional[str] = None,
    materialidad: Optional[bool] = None,
    user_id: Optional[int] = None,
) -> MasterUnificadoVirtuales:
    """Crea un nuevo registro de master unificado virtuales."""
    master = MasterUnificadoVirtuales(
        solicitud_previo=solicitud_previo,
        agente=agente,
        pedimento=pedimento,
        aduana=aduana,
        patente=patente,
        destino=destino,
        cliente_space=cliente_space,
        impo_expo=impo_expo,
        proveedor_cliente=proveedor_cliente,
        mes=mes,
        complemento=complemento,
        tipo_immex=tipo_immex,
        factura=factura,
        fecha_pago=fecha_pago,
        informacion=informacion,
        estatus=estatus,
        op_regular=op_regular,
        tipo=tipo,
        numero=numero,
        carretes=carretes,
        servicio_cliente=servicio_cliente,
        plazo=plazo,
        firma=firma,
        incoterm=incoterm,
        tipo_exportacion=tipo_exportacion,
        escenario=escenario,
        materialidad=materialidad,
    )
    db.add(master)
    await db.flush()
    if master.numero is None and master.id:
        master.numero = master.id
        await db.flush()
    if user_id:
        _add_master_unificado_virtual_historial_entry(
            db,
            numero=master.numero,
            operacion=MasterUnificadoVirtualOperacion.CREATE,
            user_id=user_id,
            datos_antes=None,
            datos_despues=master_unificado_virtual_to_dict(master),
            comentario="Creación manual",
        )
    await db.commit()
    await db.refresh(master)
    return master


async def get_master_unificado_virtuales_by_id(db: AsyncSession, master_id: int) -> Optional[MasterUnificadoVirtuales]:
    """Obtiene un registro de master unificado virtuales por ID."""
    result = await db.execute(
        select(MasterUnificadoVirtuales).where(MasterUnificadoVirtuales.id == master_id)
    )
    return result.scalar_one_or_none()


async def get_master_unificado_virtuales_by_pedimento(db: AsyncSession, pedimento: int) -> Optional[MasterUnificadoVirtuales]:
    """Obtiene un registro de master unificado virtuales por pedimento."""
    result = await db.execute(
        select(MasterUnificadoVirtuales)
        .where(MasterUnificadoVirtuales.pedimento == pedimento)
        .order_by(desc(MasterUnificadoVirtuales.created_at))
        .limit(1)
    )
    return result.scalar_one_or_none()


async def get_master_unificado_virtuales_by_numero(db: AsyncSession, numero: int) -> Optional[MasterUnificadoVirtuales]:
    """Obtiene un registro de master unificado virtuales por número."""
    result = await db.execute(
        select(MasterUnificadoVirtuales)
        .where(MasterUnificadoVirtuales.numero == numero)
        .order_by(desc(MasterUnificadoVirtuales.created_at))
        .limit(1)
    )
    return result.scalar_one_or_none()


async def get_master_unificado_virtuales_by_numero_impoexpo(
    db: AsyncSession,
    numero: int,
    impo_expo: str
) -> Optional[MasterUnificadoVirtuales]:
    """Obtiene el registro más reciente de master unificado virtuales por número e impo_expo."""
    result = await db.execute(
        select(MasterUnificadoVirtuales)
        .where(MasterUnificadoVirtuales.numero == numero)
        .where(MasterUnificadoVirtuales.impo_expo == impo_expo)
        .order_by(desc(MasterUnificadoVirtuales.created_at))
        .limit(1)
    )
    return result.scalar_one_or_none()


async def get_master_unificado_virtuales_by_numero_impoexpo_mes(
    db: AsyncSession,
    numero: int,
    impo_expo: str,
    mes: str,
) -> Optional[MasterUnificadoVirtuales]:
    """Obtiene el registro de master unificado virtuales por número, impo_expo y mes (el más reciente si hay varios)."""
    if not mes or not str(mes).strip():
        return None
    result = await db.execute(
        select(MasterUnificadoVirtuales)
        .where(MasterUnificadoVirtuales.numero == numero)
        .where(MasterUnificadoVirtuales.impo_expo == impo_expo)
        .where(MasterUnificadoVirtuales.mes.ilike(str(mes).strip()))
        .order_by(desc(MasterUnificadoVirtuales.created_at))
        .limit(1)
    )
    return result.scalar_one_or_none()


async def list_master_unificado_virtuales(
    db: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    pedimento: Optional[int] = None,
    numero: Optional[int] = None,
    aduana: Optional[int] = None,
    patente: Optional[int] = None,
    estatus: Optional[str] = None,
    proveedor_cliente: Optional[str] = None,
    mes: Optional[str] = None,
    año: Optional[int] = None
) -> List[MasterUnificadoVirtuales]:
    """Lista registros de master unificado virtuales con filtros opcionales."""
    query = select(MasterUnificadoVirtuales)
    
    if pedimento is not None:
        query = query.where(MasterUnificadoVirtuales.pedimento == pedimento)
    
    if numero is not None:
        query = query.where(MasterUnificadoVirtuales.numero == numero)
    
    if aduana is not None:
        query = query.where(MasterUnificadoVirtuales.aduana == aduana)
    
    if patente is not None:
        query = query.where(MasterUnificadoVirtuales.patente == patente)
    
    if estatus:
        query = query.where(MasterUnificadoVirtuales.estatus == estatus)
    
    if proveedor_cliente:
        query = query.where(MasterUnificadoVirtuales.proveedor_cliente.ilike(f"%{proveedor_cliente}%"))
    
    if mes:
        query = query.where(MasterUnificadoVirtuales.mes.ilike(mes))
    
    if año is not None:
        query = query.where(func.extract("year", MasterUnificadoVirtuales.created_at) == año)
    
    query = query.order_by(desc(MasterUnificadoVirtuales.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_master_unificado_virtuales(
    db: AsyncSession,
    pedimento: Optional[int] = None,
    numero: Optional[int] = None,
    aduana: Optional[int] = None,
    patente: Optional[int] = None,
    estatus: Optional[str] = None,
    proveedor_cliente: Optional[str] = None,
    mes: Optional[str] = None
) -> int:
    """Cuenta el total de registros de master unificado virtuales con filtros opcionales."""
    query = select(func.count(MasterUnificadoVirtuales.id))
    
    if pedimento is not None:
        query = query.where(MasterUnificadoVirtuales.pedimento == pedimento)
    
    if numero is not None:
        query = query.where(MasterUnificadoVirtuales.numero == numero)
    
    if aduana is not None:
        query = query.where(MasterUnificadoVirtuales.aduana == aduana)
    
    if patente is not None:
        query = query.where(MasterUnificadoVirtuales.patente == patente)
    
    if estatus:
        query = query.where(MasterUnificadoVirtuales.estatus == estatus)
    
    if proveedor_cliente:
        query = query.where(MasterUnificadoVirtuales.proveedor_cliente.ilike(f"%{proveedor_cliente}%"))
    
    if mes:
        query = query.where(MasterUnificadoVirtuales.mes.ilike(mes))
    
    result = await db.execute(query)
    return result.scalar() or 0


async def update_master_unificado_virtuales_campos_desde_excel(
    db: AsyncSession,
    numero: int,
    impo_expo: str,
    mes: str,
    user_id: int,
    *,
    patente: Optional[int] = None,
    aduana: Optional[int] = None,
    complemento: Optional[str] = None,
    firma: Optional[str] = None,
    pedimento: Optional[int] = None,
) -> Optional[MasterUnificadoVirtuales]:
    """
    Busca el registro por numero, impo_expo y mes; actualiza patente, aduana, complemento, firma y pedimento;
    registra el cambio en el historial. No hace commit (lo debe hacer el llamador).
    """
    master = await get_master_unificado_virtuales_by_numero_impoexpo_mes(db, numero, impo_expo, mes)
    if not master:
        return None
    datos_antes = master_unificado_virtual_to_dict(master)
    if patente is not None:
        master.patente = patente
    if aduana is not None:
        master.aduana = aduana
    if complemento is not None:
        master.complemento = complemento
    if firma is not None:
        master.firma = firma
    if pedimento is not None:
        master.pedimento = pedimento
    datos_despues = master_unificado_virtual_to_dict(master)
    campos_modificados = [k for k in datos_antes if datos_antes.get(k) != datos_despues.get(k)]
    _add_master_unificado_virtual_historial_entry(
        db,
        numero=master.numero,
        operacion=MasterUnificadoVirtualOperacion.UPDATE,
        user_id=user_id,
        datos_antes=datos_antes,
        datos_despues=datos_despues,
        campos_modificados=campos_modificados if campos_modificados else None,
        comentario="Actualización desde Excel (patente, aduana, complemento, firma, pedimento)",
    )
    return master


async def update_master_unificado_virtuales(
    db: AsyncSession,
    numero: int,
    solicitud_previo: Optional[bool] = None,
    agente: Optional[str] = None,
    pedimento: Optional[int] = None,
    aduana: Optional[int] = None,
    patente: Optional[int] = None,
    destino: Optional[int] = None,
    cliente_space: Optional[str] = None,
    impo_expo: Optional[str] = None,
    proveedor_cliente: Optional[str] = None,
    mes: Optional[str] = None,
    complemento: Optional[str] = None,
    tipo_immex: Optional[str] = None,
    factura: Optional[str] = None,
    fecha_pago: Optional[date] = None,
    informacion: Optional[str] = None,
    estatus: Optional[str] = None,
    op_regular: Optional[bool] = None,
    tipo: Optional[str] = None,
    numero_nuevo: Optional[int] = None,
    carretes: Optional[bool] = None,
    servicio_cliente: Optional[str] = None,
    plazo: Optional[str] = None,
    firma: Optional[str] = None,
    incoterm: Optional[str] = None,
    tipo_exportacion: Optional[str] = None,
    escenario: Optional[str] = None,
    materialidad: Optional[bool] = None,
    user_id: Optional[int] = None,
    master_id: Optional[int] = None,
) -> Optional[MasterUnificadoVirtuales]:
    """
    Actualiza un registro de master unificado virtuales.
    Si se pasa master_id, se usa para identificar el registro (recomendado en la UI).
    Si no, se busca por numero (el más reciente por created_at).
    """
    if master_id is not None:
        master = await get_master_unificado_virtuales_by_id(db, master_id)
    else:
        master = await get_master_unificado_virtuales_by_numero(db, numero)
    if not master:
        return None

    datos_antes = master_unificado_virtual_to_dict(master)
    
    if solicitud_previo is not None:
        master.solicitud_previo = solicitud_previo
    if agente is not None:
        master.agente = agente
    if pedimento is not None:
        master.pedimento = pedimento
    if aduana is not None:
        master.aduana = aduana
    if patente is not None:
        master.patente = patente
    if destino is not None:
        master.destino = destino
    if cliente_space is not None:
        master.cliente_space = cliente_space
    if impo_expo is not None:
        master.impo_expo = impo_expo
    if proveedor_cliente is not None:
        master.proveedor_cliente = proveedor_cliente
    if mes is not None:
        master.mes = mes
    if complemento is not None:
        master.complemento = complemento
    if tipo_immex is not None:
        master.tipo_immex = tipo_immex
    if factura is not None:
        master.factura = factura
    if fecha_pago is not None:
        master.fecha_pago = fecha_pago
    if informacion is not None:
        master.informacion = informacion
    if estatus is not None:
        master.estatus = estatus
    if op_regular is not None:
        master.op_regular = op_regular
    if tipo is not None:
        master.tipo = tipo
    if numero_nuevo is not None:
        master.numero = numero_nuevo
    if carretes is not None:
        master.carretes = carretes
    if servicio_cliente is not None:
        master.servicio_cliente = servicio_cliente
    if plazo is not None:
        master.plazo = plazo
    if firma is not None:
        master.firma = firma
    if incoterm is not None:
        master.incoterm = incoterm
    if tipo_exportacion is not None:
        master.tipo_exportacion = tipo_exportacion
    if escenario is not None:
        master.escenario = escenario
    if materialidad is not None:
        master.materialidad = materialidad

    if user_id:
        datos_despues = master_unificado_virtual_to_dict(master)
        campos_modificados = [
            k for k in datos_antes
            if datos_antes.get(k) != datos_despues.get(k)
        ]
        _add_master_unificado_virtual_historial_entry(
            db,
            numero=master.numero,
            operacion=MasterUnificadoVirtualOperacion.UPDATE,
            user_id=user_id,
            datos_antes=datos_antes,
            datos_despues=datos_despues,
            campos_modificados=campos_modificados if campos_modificados else None,
            comentario="Actualización manual",
        )
    
    await db.commit()
    await db.refresh(master)
    return master


async def list_master_unificado_virtuales_historial(
    db: AsyncSession,
    numero: Optional[int] = None,
    operacion: Optional[MasterUnificadoVirtualOperacion] = None,
    user_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0
) -> List[MasterUnificadoVirtualHistorial]:
    """Lista el historial de movimientos del master virtuales con filtros opcionales."""
    query = (
        select(MasterUnificadoVirtualHistorial)
        .options(selectinload(MasterUnificadoVirtualHistorial.user))
    )
    if numero is not None:
        query = query.where(MasterUnificadoVirtualHistorial.numero == numero)
    if operacion is not None:
        query = query.where(MasterUnificadoVirtualHistorial.operacion == operacion)
    if user_id is not None:
        query = query.where(MasterUnificadoVirtualHistorial.user_id == user_id)
    query = query.order_by(desc(MasterUnificadoVirtualHistorial.created_at)).limit(limit).offset(offset)
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_master_unificado_virtuales_historial(
    db: AsyncSession,
    numero: Optional[int] = None,
    operacion: Optional[MasterUnificadoVirtualOperacion] = None,
    user_id: Optional[int] = None
) -> int:
    """Cuenta registros del historial con filtros opcionales."""
    query = select(func.count(MasterUnificadoVirtualHistorial.id))
    if numero is not None:
        query = query.where(MasterUnificadoVirtualHistorial.numero == numero)
    if operacion is not None:
        query = query.where(MasterUnificadoVirtualHistorial.operacion == operacion)
    if user_id is not None:
        query = query.where(MasterUnificadoVirtualHistorial.user_id == user_id)
    result = await db.execute(query)
    return result.scalar() or 0


async def delete_master_unificado_virtuales(db: AsyncSession, numero: int) -> bool:
    """Elimina un registro de master unificado virtuales. Identificador: numero."""
    master = await get_master_unificado_virtuales_by_numero(db, numero)
    if not master:
        return False
    
    await db.delete(master)
    await db.commit()
    return True


# ============================================================================
# Actualización de Master Virtuales desde Compras y Ventas
# ============================================================================

def _es_tipo_proveedor(tipo: Optional[str]) -> bool:
    """Indica si el registro es tipo proveedor (revisar compras)."""
    if not tipo:
        return True
    return "proveedor" in str(tipo).lower()


def _es_tipo_cliente(tipo: Optional[str]) -> bool:
    """Indica si el registro es tipo cliente (revisar ventas)."""
    if not tipo:
        return False
    return "cliente" in str(tipo).lower()


async def get_tipo_exportacion_distintos_master_virtuales(db: AsyncSession) -> List[str]:
    """Obtiene los valores distintos de tipo_exportacion en master_unificado_virtuales (no vacíos), ordenados."""
    query = (
        select(MasterUnificadoVirtuales.tipo_exportacion)
        .distinct()
    )
    result = await db.execute(query)
    return sorted([(r[0] or "").strip() for r in result.all() if (r[0] or "").strip()])


async def get_proveedores_distintos_master_virtuales(db: AsyncSession) -> List[tuple]:
    """
    Obtiene (numero, proveedor_cliente) distintos donde tipo indica proveedor.
    numero se compara con codigo_proveedor en compras; proveedor_cliente es el nombre.
    """
    query = (
        select(MasterUnificadoVirtuales.numero, MasterUnificadoVirtuales.proveedor_cliente)
        .where(MasterUnificadoVirtuales.numero.isnot(None))
        .where(
            or_(
                MasterUnificadoVirtuales.tipo.is_(None),
                MasterUnificadoVirtuales.tipo.ilike("%proveedor%")
            )
        )
        .distinct()
    )
    result = await db.execute(query)
    return [
        (r[0], (str(r[1]).strip() if r[1] else None))
        for r in result.all()
        if r[0] is not None
    ]


async def get_clientes_distintos_master_virtuales(db: AsyncSession) -> List[tuple]:
    """
    Obtiene (numero, proveedor_cliente) distintos donde tipo indica cliente.
    numero se compara con codigo_cliente en ventas; proveedor_cliente es el nombre.
    """
    query = (
        select(MasterUnificadoVirtuales.numero, MasterUnificadoVirtuales.proveedor_cliente)
        .where(MasterUnificadoVirtuales.numero.isnot(None))
        .where(MasterUnificadoVirtuales.tipo.ilike("%cliente%"))
        .distinct()
    )
    result = await db.execute(query)
    return [
        (r[0], (str(r[1]).strip() if r[1] else None))
        for r in result.all()
        if r[0] is not None
    ]


async def get_registros_master_por_mes(
    db: AsyncSession,
    mes_str: str
) -> List[MasterUnificadoVirtuales]:
    """
    Obtiene todos los registros de master_unificado_virtuales del mes indicado.
    Usado para revisar los registros del mes anterior y propagarlos al mes actual.
    """
    query = (
        select(MasterUnificadoVirtuales)
        .where(MasterUnificadoVirtuales.mes == mes_str)
        .where(MasterUnificadoVirtuales.numero.isnot(None))
    )
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_ultimos_registros_por_numero_tipo_impoexpo(
    db: AsyncSession
) -> List[MasterUnificadoVirtuales]:
    """
    Cuando no hay registros del mes anterior, obtiene el último registro (por created_at)
    por cada combinación (numero, tipo, impo_expo). Así se puede propagar al mes actual
    sin marcar todo como "Nuevo".
    """
    query = (
        select(MasterUnificadoVirtuales)
        .where(MasterUnificadoVirtuales.numero.isnot(None))
        .order_by(desc(MasterUnificadoVirtuales.created_at))
    )
    result = await db.execute(query)
    todos = list(result.scalars().all())
    vistos = set()
    ultimos = []
    for r in todos:
        clave = (r.numero, (r.tipo or "").strip(), (r.impo_expo or "").strip())
        if clave in vistos:
            continue
        vistos.add(clave)
        ultimos.append(r)
    return ultimos


async def get_proveedores_con_compras_en_mes(
    db: AsyncSession,
    año: int,
    mes: int
) -> set:
    """
    Obtiene el conjunto de codigo_proveedor que tienen al menos una compra
    en el mes/año indicado. Proveedores son siempre IMPO (solo mexicanos, pais = MX).
    Se compara con master_virtuales.numero (tipo Proveedor).
    """
    from datetime import date
    inicio = date(año, mes, 1)
    if mes == 12:
        fin = date(año + 1, 1, 1)
    else:
        fin = date(año, mes + 1, 1)
    inicio_dt = datetime(inicio.year, inicio.month, inicio.day, 0, 0, 0, tzinfo=timezone.utc)
    fin_dt = datetime(fin.year, fin.month, fin.day, 0, 0, 0, tzinfo=timezone.utc)
    query = (
        select(Compra.codigo_proveedor)
        .select_from(Compra)
        .join(Proveedor, Compra.codigo_proveedor == Proveedor.codigo_proveedor)
        .where(
            Compra.posting_date.isnot(None),
            Compra.posting_date >= inicio_dt,
            Compra.posting_date < fin_dt,
            Compra.codigo_proveedor.isnot(None),
            Proveedor.pais == "MX"
        )
        .distinct()
    )
    result = await db.execute(query)
    return {str(row[0]).strip() for row in result.all() if row[0]}


async def get_clientes_con_ventas_en_mes(
    db: AsyncSession,
    año: int,
    mes: int
) -> Dict[str, List[str]]:
    """
    Obtiene un diccionario de codigo_cliente -> lista de tipos IMPO/EXPO que tienen ventas
    en el mes/año indicado (Venta.periodo). 
    
    Obtiene TODOS los clientes con ventas en el mes, sin filtrar por región.
    
    Reglas para determinar IMPO/EXPO:
    - Si region_asc == 'MX  Mexiko' → IMPO
    - Si region_asc != 'MX  Mexiko' → EXPO
    
    Se compara con master_virtuales.numero (tipo Cliente).
    
    Retorna: Dict[str, List[str]] donde la clave es codigo_cliente y el valor es lista de ["IMPO", "EXPO"]
    """
    from datetime import date
    periodo_mes = date(año, mes, 1)
    
    # Obtener TODOS los clientes con ventas en el mes, con su región
    query = (
        select(Venta.codigo_cliente, Venta.region_asc)
        .where(
            Venta.periodo == periodo_mes,
            Venta.codigo_cliente.isnot(None)
        )
        .distinct()
    )
    result = await db.execute(query)
    
    # Agrupar por cliente y determinar tipos IMPO/EXPO
    resultado = {}
    for row in result.all():
        codigo_cliente = str(row[0]).strip() if row[0] is not None else None
        region_asc = row[1]
        
        if not codigo_cliente:
            continue
        
        if codigo_cliente not in resultado:
            resultado[codigo_cliente] = []
        
        # Determinar tipo según región (NULL o vacío se considera EXPO para no perder registros)
        region_normalized = (region_asc or "").strip()
        if region_normalized == "MX  Mexiko":
            if "IMPO" not in resultado[codigo_cliente]:
                resultado[codigo_cliente].append("IMPO")
        else:
            # Cualquier otro valor (incl. NULL, "") → EXPO
            if "EXPO" not in resultado[codigo_cliente]:
                resultado[codigo_cliente].append("EXPO")
    
    return resultado


async def get_proveedores_nuevos_desde_compras(
    db: AsyncSession,
    año: int,
    mes: int,
    numeros_proveedores_existentes: set
) -> List[tuple]:
    """
    Obtiene (codigo_proveedor, nombre_proveedor, impo_expo) de compras en el mes que
    NO existen en master_virtuales (comparando codigo_proveedor con numero).
    Proveedores son siempre IMPO (solo mexicanos, pais = MX).
    numeros_proveedores_existentes: set de numero (str o int) de master tipo Proveedor.
    
    Retorna: List[tuple] donde cada tupla es (codigo, nombre, "IMPO")
    """
    from datetime import date
    inicio = date(año, mes, 1)
    if mes == 12:
        fin = date(año + 1, 1, 1)
    else:
        fin = date(año, mes + 1, 1)
    inicio_dt = datetime(inicio.year, inicio.month, inicio.day, 0, 0, 0, tzinfo=timezone.utc)
    fin_dt = datetime(fin.year, fin.month, fin.day, 0, 0, 0, tzinfo=timezone.utc)
    query = (
        select(Compra.codigo_proveedor, Compra.nombre_proveedor)
        .select_from(Compra)
        .join(Proveedor, Compra.codigo_proveedor == Proveedor.codigo_proveedor)
        .where(
            Compra.posting_date.isnot(None),
            Compra.posting_date >= inicio_dt,
            Compra.posting_date < fin_dt,
            Compra.codigo_proveedor.isnot(None),
            Proveedor.pais == "MX"
        )
        .distinct()
    )
    result = await db.execute(query)
    numeros_set = {str(n).strip() for n in numeros_proveedores_existentes if n is not None}
    nuevos = []
    vistos = set()
    for row in result.all():
        cod = str(row[0]).strip() if row[0] else ""
        nom = str(row[1]).strip() if row[1] else cod
        if not cod or cod in vistos or cod in numeros_set:
            continue
        vistos.add(cod)
        nuevos.append((cod, nom, "IMPO"))
    return nuevos


async def get_clientes_nuevos_desde_ventas(
    db: AsyncSession,
    año: int,
    mes: int,
    numeros_clientes_existentes: set
) -> List[tuple]:
    """
    Obtiene (codigo_cliente, nombre_cliente, impo_expo) de ventas en el mes que
    NO existen en master_virtuales como tipo cliente (comparando codigo_cliente con numero).
    
    Obtiene TODOS los clientes con ventas en el mes, sin filtrar por región.
    
    numeros_clientes_existentes: set de numero (str o int) de master tipo Cliente.
    
    Reglas para determinar IMPO/EXPO:
    - Si region_asc == 'MX  Mexiko' → IMPO
    - Si region_asc != 'MX  Mexiko' → EXPO
    
    Retorna: List[tuple] donde cada tupla es (codigo, nombre, impo_expo)
    """
    from datetime import date
    periodo_mes = date(año, mes, 1)
    
    # Obtener TODOS los clientes con ventas en el mes, con su región
    query = (
        select(Venta.codigo_cliente, Venta.cliente, Venta.region_asc)
        .where(
            Venta.periodo == periodo_mes,
            Venta.codigo_cliente.isnot(None)
        )
        .distinct()
    )
    result = await db.execute(query)
    
    numeros_set = {str(n).strip() for n in numeros_clientes_existentes if n is not None}
    nuevos = []
    vistos = set()
    
    # Procesar todas las ventas y agrupar por cliente + tipo IMPO/EXPO
    for row in result.all():
        cod = str(row[0]).strip() if row[0] is not None else ""
        nom = str(row[1]).strip() if row[1] else cod
        region_asc = row[2]
        
        if not cod or cod in numeros_set:
            continue
        
        # Determinar tipo según región (NULL o vacío → EXPO para no perder registros)
        region_normalized = (region_asc or "").strip()
        if region_normalized == "MX  Mexiko":
            tipo_impo_expo = "IMPO"
        else:
            tipo_impo_expo = "EXPO"
        
        clave = (cod, tipo_impo_expo)
        if clave in vistos:
            continue
        vistos.add(clave)
        nuevos.append((cod, nom or cod, tipo_impo_expo))
    
    return nuevos


async def get_ultimo_virtual_por_numero(
    db: AsyncSession,
    numero: int,
    es_proveedor: bool = True,
    impo_expo: Optional[str] = None
) -> Optional[MasterUnificadoVirtuales]:
    """
    Obtiene el último registro de master_virtuales para un numero (codigo_proveedor
    o codigo_cliente) del tipo indicado. Si se especifica impo_expo, filtra por ese valor.
    Ordenado por created_at descendente.
    """
    query = (
        select(MasterUnificadoVirtuales)
        .where(MasterUnificadoVirtuales.numero == numero)
    )
    if es_proveedor:
        query = query.where(
            or_(
                MasterUnificadoVirtuales.tipo.is_(None),
                MasterUnificadoVirtuales.tipo.ilike("%proveedor%")
            )
        )
    else:
        query = query.where(
            MasterUnificadoVirtuales.tipo.ilike("%cliente%")
        )
    
    # Filtrar por impo_expo si se especifica
    if impo_expo:
        query = query.where(MasterUnificadoVirtuales.impo_expo == impo_expo)
    
    query = query.order_by(desc(MasterUnificadoVirtuales.created_at)).limit(1)
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def existe_virtual_numero_mes(
    db: AsyncSession,
    numero: int,
    mes_captura: str,
    tipo: Optional[str] = None,
    impo_expo: Optional[str] = None
) -> bool:
    """
    Verifica si ya existe un registro en master_virtuales para ese numero,
    mes de captura e impo_expo. Si se indica tipo, filtra por tipo. 
    Evita duplicados en el mismo periodo considerando también impo_expo.
    """
    conditions = [
        MasterUnificadoVirtuales.numero == numero,
        MasterUnificadoVirtuales.mes == mes_captura
    ]
    if tipo:
        if "cliente" in str(tipo).lower():
            conditions.append(MasterUnificadoVirtuales.tipo.ilike("%cliente%"))
        elif "proveedor" in str(tipo).lower():
            conditions.append(
                or_(
                    MasterUnificadoVirtuales.tipo.is_(None),
                    MasterUnificadoVirtuales.tipo.ilike("%proveedor%")
                )
            )
    
    # Incluir impo_expo en la verificación para evitar duplicados
    if impo_expo:
        conditions.append(MasterUnificadoVirtuales.impo_expo == impo_expo)
    
    query = select(func.count(MasterUnificadoVirtuales.id)).where(*conditions)
    result = await db.execute(query)
    return (result.scalar() or 0) > 0


async def get_siguiente_numero_virtual(db: AsyncSession) -> int:
    """Obtiene el siguiente número disponible para un nuevo registro (max + 1)."""
    query = select(func.coalesce(func.max(MasterUnificadoVirtuales.numero), 0))
    result = await db.execute(query)
    max_num = result.scalar() or 0
    return int(max_num) + 1


async def get_ultimas_compras_proveedor(
    db: AsyncSession,
    codigo_proveedor: str,
    limit: int = 5
) -> List[Compra]:
    """Últimas compras del proveedor (codigo_proveedor), ordenadas por posting_date descendente."""
    codigo = str(codigo_proveedor).strip() if codigo_proveedor else ""
    if not codigo:
        return []
    query = (
        select(Compra)
        .where(Compra.codigo_proveedor == codigo)
        .where(Compra.posting_date.isnot(None))
        .order_by(desc(Compra.posting_date))
        .limit(limit)
    )
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_ultimas_ventas_cliente(
    db: AsyncSession,
    codigo_cliente: Optional[int],
    limit: int = 5
) -> List[Venta]:
    """Últimas ventas del cliente (codigo_cliente), ordenadas por periodo/created_at descendente."""
    if codigo_cliente is None:
        return []
    try:
        cod_int = int(codigo_cliente)
    except (TypeError, ValueError):
        return []
    query = (
        select(Venta)
        .where(Venta.codigo_cliente == cod_int)
        .order_by(desc(Venta.periodo), desc(Venta.created_at))
        .limit(limit)
    )
    result = await db.execute(query)
    return list(result.scalars().all())


async def actualizar_master_virtuales_desde_compras(
    db: AsyncSession,
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Actualiza el master de virtuales según especificación:
    
    1. Revisar los registros del MES ANTERIOR almacenados en la tabla principal.
    2. Para cada registro (tipo IMPO o EXPO):
       - Validar si existieron movimientos en el mes evaluado:
         * Cliente → Ventas
         * Proveedor → Compras (solo país = MX)
       - Si hay movimientos: crear registro del MES ACTUAL con estatus "En Captura".
       - Si no hay movimientos: crear registro del MES ACTUAL con estatus "Sin Operación".
       - Si el cliente/proveedor tiene IMPO y EXPO, se crean dos registros (uno por tipo).
    3. Clientes o Proveedores nuevos (no existían en el mes anterior):
       - Crear registro con información básica y estatus "Nuevo".
       - El campo Mes corresponde al mes en que se ejecuta el proceso.
    """
    import calendar
    MESES_ES = ("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")
    ahora = datetime.now(timezone.utc)
    año_captura = ahora.year
    mes_captura = ahora.month
    mes_captura_str = MESES_ES[mes_captura - 1]

    # Periodo de análisis: mes anterior (ej. en febrero → enero 1-31)
    if mes_captura == 1:
        año_analisis = año_captura - 1
        mes_analisis = 12
    else:
        año_analisis = año_captura
        mes_analisis = mes_captura - 1

    _, ultimo_dia = calendar.monthrange(año_analisis, mes_analisis)
    nombre_mes_analisis = MESES_ES[mes_analisis - 1]
    periodo_rango = f"1 - {ultimo_dia} {nombre_mes_analisis} {año_analisis}"

    resumen = {
        "periodo_analisis": f"{nombre_mes_analisis} {año_analisis}",
        "periodo_rango": periodo_rango,
        "mes_captura": mes_captura_str,
        "existentes_en_captura": 0,
        "existentes_sin_operacion": 0,
        "nuevos": 0,
        "nuevos_proveedores": 0,
        "nuevos_clientes": 0,
        "omitidos_duplicados": 0,
        "errores": [],
    }

    try:
        # 0. Sincronizar secuencia de id (evita UniqueViolationError si la secuencia quedó desincronizada)
        await db.execute(text(
            "SELECT setval(pg_get_serial_sequence('master_unificado_virtuales', 'id'), "
            "COALESCE((SELECT MAX(id) FROM master_unificado_virtuales), 1))"
        ))

        # 1. Movimientos en el mes evaluado (mes anterior)
        proveedores_con_compras = await get_proveedores_con_compras_en_mes(
            db, año_analisis, mes_analisis
        )
        clientes_con_ventas = await get_clientes_con_ventas_en_mes(
            db, año_analisis, mes_analisis
        )

        # 2. Registros del MES ANTERIOR en la tabla principal (origen para propagar al mes actual)
        registros_mes_anterior = await get_registros_master_por_mes(db, nombre_mes_analisis)
        # Si no hay registros del mes anterior (primera vez o mes sin datos), usar el último
        # registro por (numero, tipo, impo_expo) para no marcar todo como "Nuevo"
        if not registros_mes_anterior:
            registros_mes_anterior = await get_ultimos_registros_por_numero_tipo_impoexpo(db)

        # 3. Por cada registro del mes anterior: validar movimientos y crear registro del MES ACTUAL
        for r in registros_mes_anterior:
            if r.numero is None:
                continue

            if await existe_virtual_numero_mes(
                db, int(r.numero), mes_captura_str,
                tipo=r.tipo, impo_expo=r.impo_expo or None
            ):
                resumen["omitidos_duplicados"] += 1
                continue

            # Validar movimientos: Cliente → Ventas; Proveedor → Compras (solo MX)
            es_proveedor = r.tipo and "proveedor" in str(r.tipo).lower()
            if es_proveedor:
                tiene_movimientos = str(r.numero) in proveedores_con_compras
            else:
                tipos_del_cliente = clientes_con_ventas.get(str(r.numero), [])
                tiene_movimientos = (r.impo_expo or "") in tipos_del_cliente

            estatus = "En Captura" if tiene_movimientos else "Sin Operacion"

            nuevo = MasterUnificadoVirtuales(
                solicitud_previo=r.solicitud_previo,
                agente=r.agente,
                pedimento=None,
                aduana=None,
                patente=None,
                firma=None,
                destino=r.destino,
                cliente_space=r.cliente_space,
                impo_expo=r.impo_expo,
                proveedor_cliente=r.proveedor_cliente,
                mes=mes_captura_str,
                complemento=r.complemento,
                tipo_immex=r.tipo_immex,
                factura=r.factura,
                fecha_pago=r.fecha_pago,
                informacion=(
                    f"Periodo analizado: {resumen['periodo_analisis']}. "
                    + (r.informacion or "")
                ).strip() or None,
                estatus=estatus,
                op_regular=r.op_regular,
                tipo=r.tipo,
                numero=r.numero,
                carretes=r.carretes,
                servicio_cliente=r.servicio_cliente,
                plazo=r.plazo,
                incoterm=r.incoterm,
                tipo_exportacion=r.tipo_exportacion,
                escenario=r.escenario,
                materialidad=r.materialidad,
            )
            db.add(nuevo)
            await db.flush()
            if nuevo.id and nuevo.numero is None:
                nuevo.numero = nuevo.id
            if user_id:
                _add_master_unificado_virtual_historial_entry(
                    db,
                    numero=nuevo.numero,
                    operacion=MasterUnificadoVirtualOperacion.CREATE,
                    user_id=user_id,
                    datos_antes=None,
                    datos_despues=master_unificado_virtual_to_dict(nuevo),
                    comentario=f"Actualización automática. Estatus: {nuevo.estatus}",
                )
            if tiene_movimientos:
                resumen["existentes_en_captura"] += 1
            else:
                resumen["existentes_sin_operacion"] += 1

        # 4. Numeros ya registrados en la tabla (cualquier mes). Nuevos = solo los que NO están y tienen movimientos.
        prov_distintos = await get_proveedores_distintos_master_virtuales(db)
        cli_distintos = await get_clientes_distintos_master_virtuales(db)
        proveedores_en_tabla = {str(p[0]) for p in prov_distintos if p[0] is not None}
        clientes_en_tabla = {str(c[0]) for c in cli_distintos if c[0] is not None}

        # 5. Proveedores nuevos: no están en la tabla y tienen compras en el mes evaluado (MX)
        proveedores_nuevos = await get_proveedores_nuevos_desde_compras(
            db, año_analisis, mes_analisis, proveedores_en_tabla
        )

        for codigo, nombre, impo_expo_tipo in proveedores_nuevos:
            try:
                numero_asignar = int(codigo) if codigo else None
            except (ValueError, TypeError):
                numero_asignar = None
            if numero_asignar is not None and await existe_virtual_numero_mes(db, numero_asignar, mes_captura_str, tipo="Proveedor", impo_expo=impo_expo_tipo):
                resumen["omitidos_duplicados"] += 1
                continue
            if numero_asignar is None:
                numero_asignar = await get_siguiente_numero_virtual(db)

            nuevo = MasterUnificadoVirtuales(
                solicitud_previo=None,
                agente=None,
                pedimento=None,
                aduana=None,
                patente=None,
                firma=None,
                destino=None,
                cliente_space=None,
                impo_expo=impo_expo_tipo,
                proveedor_cliente=nombre or codigo,
                mes=mes_captura_str,
                complemento=None,
                tipo_immex=None,
                factura=None,
                fecha_pago=None,
                informacion=(
                    f"Periodo analizado: {resumen['periodo_analisis']}"
                    + (f". Nombre: {nombre}" if nombre else "")
                ),
                estatus="Nuevo",
                op_regular=None,
                tipo="Proveedor",
                numero=numero_asignar,
                carretes=None,
                servicio_cliente=None,
                plazo=None,
                incoterm=None,
                tipo_exportacion=None,
            )
            db.add(nuevo)
            await db.flush()
            if nuevo.id and nuevo.numero is None:
                nuevo.numero = nuevo.id
            if user_id:
                _add_master_unificado_virtual_historial_entry(
                    db,
                    numero=nuevo.numero,
                    operacion=MasterUnificadoVirtualOperacion.CREATE,
                    user_id=user_id,
                    datos_antes=None,
                    datos_despues=master_unificado_virtual_to_dict(nuevo),
                    comentario="Proveedor nuevo desde compras",
                )
            resumen["nuevos"] += 1
            resumen["nuevos_proveedores"] += 1

        # 6. Clientes nuevos: no están en la tabla y tienen ventas en el mes evaluado
        clientes_nuevos = await get_clientes_nuevos_desde_ventas(
            db, año_analisis, mes_analisis, clientes_en_tabla
        )

        for codigo, nombre, impo_expo_tipo in clientes_nuevos:
            if not codigo:
                continue
            try:
                numero_asignar = int(codigo)
            except (ValueError, TypeError):
                numero_asignar = await get_siguiente_numero_virtual(db)
            if await existe_virtual_numero_mes(db, numero_asignar, mes_captura_str, tipo="Cliente", impo_expo=impo_expo_tipo):
                resumen["omitidos_duplicados"] += 1
                continue

            nuevo = MasterUnificadoVirtuales(
                solicitud_previo=None,
                agente=None,
                pedimento=None,
                aduana=None,
                patente=None,
                firma=None,
                destino=None,
                cliente_space=None,
                impo_expo=impo_expo_tipo,
                proveedor_cliente=nombre or codigo,
                mes=mes_captura_str,
                complemento=None,
                tipo_immex=None,
                factura=None,
                fecha_pago=None,
                informacion=(
                    f"Periodo analizado: {resumen['periodo_analisis']}"
                    + (f". Nombre: {nombre}" if nombre else "")
                ),
                estatus="Nuevo",
                op_regular=None,
                tipo="Cliente",
                numero=numero_asignar,
                carretes=None,
                servicio_cliente=None,
                plazo=None,
                incoterm=None,
                tipo_exportacion=None,
            )
            db.add(nuevo)
            await db.flush()
            if nuevo.id and nuevo.numero is None:
                nuevo.numero = nuevo.id
            if user_id:
                _add_master_unificado_virtual_historial_entry(
                    db,
                    numero=nuevo.numero,
                    operacion=MasterUnificadoVirtualOperacion.CREATE,
                    user_id=user_id,
                    datos_antes=None,
                    datos_despues=master_unificado_virtual_to_dict(nuevo),
                    comentario="Cliente nuevo desde ventas",
                )
            resumen["nuevos"] += 1
            resumen["nuevos_clientes"] += 1

        await db.commit()
        return resumen

    except Exception as e:
        await db.rollback()
        resumen["errores"].append(str(e))
        raise


# ==================== CRUD para Clientes ====================

async def list_clientes(
    db: AsyncSession,
    pais: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Cliente]:
    """Lista clientes con filtros opcionales."""
    query = select(Cliente)
    
    if pais:
        query = query.where(Cliente.pais == pais)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Cliente.nombre.ilike(search_pattern),
                Cliente.codigo_cliente.cast(String).ilike(search_pattern),
                Cliente.domicilio.ilike(search_pattern)
            )
        )
    
    query = query.order_by(Cliente.nombre).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_clientes(
    db: AsyncSession,
    pais: Optional[str] = None,
    search: Optional[str] = None
) -> int:
    """Cuenta el total de clientes con filtros opcionales."""
    query = select(func.count(Cliente.codigo_cliente))
    
    if pais:
        query = query.where(Cliente.pais == pais)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Cliente.nombre.ilike(search_pattern),
                Cliente.codigo_cliente.cast(String).ilike(search_pattern),
                Cliente.domicilio.ilike(search_pattern)
            )
        )
    
    result = await db.execute(query)
    return result.scalar() or 0


async def get_cliente_by_codigo(db: AsyncSession, codigo_cliente: int) -> Optional[Cliente]:
    """Obtiene un cliente por su código."""
    result = await db.execute(
        select(Cliente).where(Cliente.codigo_cliente == codigo_cliente)
    )
    return result.scalar_one_or_none()


async def get_paises_clientes(db: AsyncSession) -> List[str]:
    """Obtiene la lista de países únicos de los clientes."""
    result = await db.execute(
        select(Cliente.pais)
        .where(Cliente.pais.isnot(None))
        .distinct()
        .order_by(Cliente.pais)
    )
    return [row[0] for row in result.all() if row[0]]
