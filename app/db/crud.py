"""Operaciones CRUD para usuarios, ejecuciones y BOM."""
import json
from sqlalchemy import select, desc, func, or_, String, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.postgresql import insert
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal
from app.db.models import User, ExecutionHistory, SalesExecutionHistory, ExecutionStatus, Part, BomFlat, PartRole, Proveedor, Material, PrecioMaterial, Compra, PaisOrigenMaterial, ProveedorHistorial, ProveedorOperacion, MaterialHistorial, MaterialOperacion, PaisOrigenMaterialHistorial, PaisOrigenMaterialOperacion, PrecioMaterialHistorial, PrecioMaterialOperacion, ClienteGrupo, Venta, CargaProveedor, CargaCliente, MasterUnificadoVirtuales, CargaProveedorHistorial, CargaProveedorOperacion, CargaClienteHistorial, CargaClienteOperacion
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


# ==================== CRUD para SalesExecutionHistory ====================

async def create_sales_execution(
    db: AsyncSession,
    user_id: int,
    fecha_inicio_periodo: datetime,
    fecha_fin_periodo: datetime,
    sistema_sap: Optional[str] = None,
    transaccion: Optional[str] = None,
    maquina: Optional[str] = None
) -> SalesExecutionHistory:
    """Crea un nuevo registro de ejecución de ventas."""
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
    await db.commit()
    await db.refresh(execution)
    return execution


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
    stack_trace: Optional[str] = None
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

async def get_proveedor_by_codigo_proveedor(db: AsyncSession, codigo_proveedor: str) -> Optional[Proveedor]:
    """Obtiene un proveedor por código de proveedor."""
    result = await db.execute(select(Proveedor).where(Proveedor.codigo_proveedor == codigo_proveedor))
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
    
    stmt = delete(Proveedor).where(Proveedor.codigo_proveedor == codigo_proveedor)
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
                Proveedor.codigo_proveedor.ilike(search_pattern),
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
                Proveedor.codigo_proveedor.ilike(search_pattern),
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
        query = query.where(ProveedorHistorial.codigo_proveedor == codigo_proveedor)
    
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
        query = query.where(ProveedorHistorial.codigo_proveedor == codigo_proveedor)
    
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
            Compra.codigo_proveedor.isnot(None),
            Compra.codigo_proveedor != ''
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
            if not codigo_proveedor or codigo_proveedor.strip() == '':
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
        
        # 2. Obtener todos los números de materiales existentes para comparación rápida
        query_existentes = select(Material.numero_material)
        result_existentes = await db.execute(query_existentes)
        numeros_existentes = {row[0] for row in result_existentes.all() if row[0]}
        
        # 3. Para cada numero_material, verificar si existe en materiales
        for numero_material, descripcion_material in materiales_en_compras:
            if not numero_material or numero_material.strip() == '':
                continue
            
            # Verificar si el material ya existe (usando el set para mejor rendimiento)
            if numero_material not in numeros_existentes:
                # Verificar nuevamente en la BD antes de crear (por si acaso)
                material_existente = await get_material_by_numero(db, numero_material)
                if material_existente:
                    numeros_existentes.add(numero_material)
                    continue
                
                # Crear el material con la descripción de compras
                try:
                    # Intentar crear el material
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


async def sincronizar_paises_origen_desde_compras(
    db: AsyncSession,
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Sincroniza países de origen desde la tabla compras.
    Busca combinaciones únicas de codigo_proveedor y numero_material en compras
    que no estén registradas en pais_origen_material y las crea con pais_origen = "Pendiente".
    
    Returns:
        Dict con estadísticas de la sincronización:
        - total_encontrados: Total de combinaciones únicas en compras
        - nuevos_creados: Cantidad de registros nuevos creados
        - errores: Lista de errores encontrados
    """
    errores = []
    nuevos_creados = 0
    
    try:
        # 1. Obtener todas las combinaciones únicas de proveedor/material en compras
        query_pares = select(
            Compra.codigo_proveedor,
            Compra.numero_material
        ).where(
            Compra.codigo_proveedor.isnot(None),
            Compra.codigo_proveedor != '',
            Compra.numero_material.isnot(None),
            Compra.numero_material != ''
        ).group_by(Compra.codigo_proveedor, Compra.numero_material)
        
        result = await db.execute(query_pares)
        pares_en_compras = result.all()
        
        total_encontrados = len(pares_en_compras)
        
        # 2. Obtener todas las combinaciones existentes para comparación rápida
        query_existentes = select(
            PaisOrigenMaterial.codigo_proveedor,
            PaisOrigenMaterial.numero_material
        )
        result_existentes = await db.execute(query_existentes)
        existentes = {
            (str(row[0]).strip(), str(row[1]).strip())
            for row in result_existentes.all()
            if row[0] and row[1]
        }
        
        # 3. Para cada par, verificar si existe y crear si no
        for codigo_proveedor, numero_material in pares_en_compras:
            if not codigo_proveedor or not numero_material:
                continue
            
            codigo_proveedor = str(codigo_proveedor).strip()
            numero_material = str(numero_material).strip()
            
            if not codigo_proveedor or not numero_material:
                continue
            
            if (codigo_proveedor, numero_material) in existentes:
                continue
            
            # Verificar nuevamente en la BD antes de crear (por si acaso)
            existente = await get_pais_origen_material_by_proveedor_material(
                db,
                codigo_proveedor,
                numero_material
            )
            if existente:
                existentes.add((codigo_proveedor, numero_material))
                continue
            
            try:
                pais_origen_material = PaisOrigenMaterial(
                    codigo_proveedor=codigo_proveedor,
                    numero_material=numero_material,
                    pais_origen="Pendiente"
                )
                db.add(pais_origen_material)
                await db.flush()
                await db.refresh(pais_origen_material)
                
                # Registrar en historial si se proporciona user_id
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
                nuevos_creados += 1
                existentes.add((codigo_proveedor, numero_material))
            except Exception as e:
                error_str = str(e)
                try:
                    await db.rollback()
                except Exception:
                    pass
                
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
                            pais_origen_material = PaisOrigenMaterial(
                                codigo_proveedor=codigo_proveedor,
                                numero_material=numero_material,
                                pais_origen="Pendiente"
                            )
                            db.add(pais_origen_material)
                            await db.flush()
                            await db.refresh(pais_origen_material)
                            
                            # Registrar en historial si se proporciona user_id
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
                    numero_material
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
            PrecioMaterial.codigo_proveedor == codigo_proveedor,
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
    user_id: Optional[int] = None
) -> PrecioMaterial:
    """Crea un nuevo precio de material."""
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
    user_id: Optional[int] = None
) -> Optional[PrecioMaterial]:
    """Actualiza un precio de material."""
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
    query = select(PrecioMaterial).options(
        selectinload(PrecioMaterial.proveedor),
        selectinload(PrecioMaterial.material)
    )
    
    if codigo_proveedor:
        query = query.where(PrecioMaterial.codigo_proveedor == codigo_proveedor)
    
    if numero_material:
        query = query.where(PrecioMaterial.numero_material == numero_material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PrecioMaterial.codigo_proveedor.ilike(search_pattern),
                PrecioMaterial.numero_material.ilike(search_pattern),
                PrecioMaterial.currency_uom.ilike(search_pattern),
                PrecioMaterial.country_origin.ilike(search_pattern)
            )
        )
    
    query = query.order_by(desc(PrecioMaterial.updated_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_precios_materiales(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    search: Optional[str] = None
) -> int:
    """Cuenta el total de precios de materiales con filtros opcionales."""
    query = select(func.count(PrecioMaterial.id))
    
    if codigo_proveedor:
        query = query.where(PrecioMaterial.codigo_proveedor == codigo_proveedor)
    
    if numero_material:
        query = query.where(PrecioMaterial.numero_material == numero_material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PrecioMaterial.codigo_proveedor.ilike(search_pattern),
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
        # 1. Obtener todas las combinaciones únicas de proveedor/material con precio válido
        query_pares = select(
            Compra.codigo_proveedor,
            Compra.numero_material
        ).where(
            Compra.codigo_proveedor.isnot(None),
            Compra.codigo_proveedor != '',
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
            numero_material = str(numero_material).strip()
            
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
                
                if precio_existente:
                    # Actualizar solo si el precio es diferente (precio de la última compra)
                    if precio_existente.precio != precio_valor:
                        await update_precio_material(
                            db,
                            precio_id=precio_existente.id,
                            precio=precio_valor,
                            currency_uom=currency_uom,
                            user_id=user_id
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
                        user_id=user_id
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
                        PaisOrigenMaterial.codigo_proveedor == codigo_proveedor,
                        PaisOrigenMaterial.numero_material == numero_material
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
        query = query.where(PrecioMaterialHistorial.codigo_proveedor == codigo_proveedor)
    
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
        query = query.where(PrecioMaterialHistorial.codigo_proveedor == codigo_proveedor)
    
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
    """Crea un nuevo registro de compra."""
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
        numero_material=numero_material,
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


async def bulk_create_or_update_compras(
    db: AsyncSession,
    compras_data: List[Dict[str, Any]]
) -> Dict[str, int]:
    """Inserta o actualiza múltiples registros de compras.
    Si ya existe un registro, se actualiza. Si no existe, se crea uno nuevo.
    
    Prioridad de búsqueda:
    1. material_document + material_doc_item (más preciso)
    2. purchasing_document + item + numero_material (fallback)
    
    Args:
        db: Sesión de base de datos
        compras_data: Lista de diccionarios con los datos de las compras
        
    Returns:
        Diccionario con 'insertados' y 'actualizados'
    """
    if not compras_data:
        return {"insertados": 0, "actualizados": 0}
    
    insertados = 0
    actualizados = 0
    
    for compra_data in compras_data:
        material_doc = compra_data.get('material_document')
        material_doc_item = compra_data.get('material_doc_item')
        purchasing_doc = compra_data.get('purchasing_document')
        numero_mat = compra_data.get('numero_material')
        item = compra_data.get('item')
        
        compra_existente = None
        
        # Primero intentar buscar por material_document y material_doc_item (más preciso)
        if material_doc is not None and material_doc_item is not None:
            compra_existente = await get_compra_by_material_document(
                db, material_doc, material_doc_item
            )
        
        # Si no se encontró, intentar con purchasing_document, item y numero_material
        if compra_existente is None and purchasing_doc is not None and numero_mat is not None:
            compra_existente = await get_compra_by_purchasing_and_material(
                db, purchasing_doc, numero_mat, item
            )
        
        if compra_existente:
            # Actualizar registro existente
            for key, value in compra_data.items():
                if key not in ['id', 'created_at']:  # No actualizar id ni created_at
                    setattr(compra_existente, key, value)
            actualizados += 1
        else:
            # Crear nuevo registro
            compra = Compra(**compra_data)
            db.add(compra)
            insertados += 1
    
    await db.commit()
    
    return {"insertados": insertados, "actualizados": actualizados}


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
        compra = Compra(**compra_data)
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
        conditions.append(Compra.codigo_proveedor == codigo_proveedor)
    
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
        conditions.append(Compra.codigo_proveedor == codigo_proveedor)
    
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

def _pais_origen_material_to_dict(pais_origen_material: PaisOrigenMaterial) -> Dict[str, Any]:
    """Convierte un objeto PaisOrigenMaterial a diccionario para el historial."""
    return {
        "id": pais_origen_material.id,
        "codigo_proveedor": pais_origen_material.codigo_proveedor,
        "numero_material": pais_origen_material.numero_material,
        "pais_origen": pais_origen_material.pais_origen,
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
            PaisOrigenMaterial.codigo_proveedor == codigo_proveedor,
            PaisOrigenMaterial.numero_material == numero_material
        )
    )
    return result.scalar_one_or_none()


async def create_pais_origen_material(
    db: AsyncSession,
    codigo_proveedor: str,
    numero_material: str,
    pais_origen: str,
    user_id: Optional[int] = None
) -> PaisOrigenMaterial:
    """Crea un nuevo país de origen de material."""
    pais_origen_material = PaisOrigenMaterial(
        codigo_proveedor=codigo_proveedor,
        numero_material=numero_material,
        pais_origen=pais_origen
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
            user_id=user_id
        )
    else:
        # Crear nuevo
        return await create_pais_origen_material(
            db,
            codigo_proveedor=codigo_proveedor,
            numero_material=numero_material,
            pais_origen=pais_origen,
            user_id=user_id
        )


async def list_paises_origen_material(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[PaisOrigenMaterial]:
    """Lista países de origen de materiales con filtros opcionales."""
    query = select(PaisOrigenMaterial).options(
        selectinload(PaisOrigenMaterial.proveedor),
        selectinload(PaisOrigenMaterial.material)
    )
    
    if codigo_proveedor:
        query = query.where(PaisOrigenMaterial.codigo_proveedor == codigo_proveedor)
    
    if numero_material:
        query = query.where(PaisOrigenMaterial.numero_material == numero_material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PaisOrigenMaterial.codigo_proveedor.ilike(search_pattern),
                PaisOrigenMaterial.numero_material.ilike(search_pattern),
                PaisOrigenMaterial.pais_origen.ilike(search_pattern)
            )
        )
    
    query = query.order_by(desc(PaisOrigenMaterial.updated_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_paises_origen_material(
    db: AsyncSession,
    codigo_proveedor: Optional[str] = None,
    numero_material: Optional[str] = None,
    search: Optional[str] = None
) -> int:
    """Cuenta el total de países de origen de materiales con filtros opcionales."""
    query = select(func.count(PaisOrigenMaterial.id))
    
    if codigo_proveedor:
        query = query.where(PaisOrigenMaterial.codigo_proveedor == codigo_proveedor)
    
    if numero_material:
        query = query.where(PaisOrigenMaterial.numero_material == numero_material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PaisOrigenMaterial.codigo_proveedor.ilike(search_pattern),
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
        query = query.where(PaisOrigenMaterialHistorial.codigo_proveedor == codigo_proveedor)
    
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
        query = query.where(PaisOrigenMaterialHistorial.codigo_proveedor == codigo_proveedor)
    
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
    
    Args:
        db: Sesión de base de datos
        ventas_data: Lista de diccionarios con los datos de las ventas
        
    Returns:
        Diccionario con 'insertados', 'duplicados' y 'errores'
    """
    if not ventas_data:
        return {"insertados": 0, "duplicados": 0, "errores": []}
    
    insertados = 0
    duplicados = 0
    errores = []
    
    for idx, venta_data in enumerate(ventas_data, start=1):
        try:
            codigo_cliente = venta_data.get('codigo_cliente')
            producto = venta_data.get('producto')
            periodo = venta_data.get('periodo')
            
            # Verificar si ya existe un registro con la misma combinación
            venta_existente = await get_venta_by_codigo_producto_periodo(
                db, codigo_cliente, producto, periodo
            )
            
            if venta_existente:
                # Ya existe, no insertar (contar como duplicado)
                duplicados += 1
            else:
                # No existe, crear nuevo registro
                venta = Venta(**venta_data)
                db.add(venta)
                insertados += 1
        except Exception as e:
            # Capturar errores individuales sin detener el proceso completo
            error_str = str(e)
            error_lower = error_str.lower()
            
            # Mensaje de error más descriptivo en español
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
    except Exception as e:
        # Si hay error al hacer commit, revertir cambios
        await db.rollback()
        error_str = str(e)
        error_lower = error_str.lower()
        
        # Mensaje de error más descriptivo en español
        if "duplicate" in error_lower or "unique" in error_lower:
            mensaje = "Error al guardar: Se intentó insertar registros duplicados. El sistema ya verifica duplicados, pero puede haber un problema de concurrencia."
        elif "foreign key" in error_lower:
            mensaje = "Error al guardar: Violación de restricción de clave foránea. Verifica que los datos de referencia (grupo de cliente) existan en la base de datos."
        elif "connection" in error_lower or "timeout" in error_lower:
            mensaje = "Error al guardar: No se pudo conectar con la base de datos o se agotó el tiempo de espera. Por favor, intenta nuevamente."
        else:
            mensaje = f"Error al guardar los datos en la base de datos: {error_str}"
        
        raise Exception(mensaje)
    
    return {
        "insertados": insertados,
        "duplicados": duplicados,
        "errores": errores
    }


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
    planta: Optional[str] = None
) -> List[Venta]:
    """Lista ventas con filtros opcionales."""
    query = select(Venta).options(selectinload(Venta.grupo))
    
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


async def count_ventas(
    db: AsyncSession,
    search: Optional[str] = None,
    cliente: Optional[str] = None,
    codigo_cliente: Optional[int] = None,
    periodo_inicio: Optional[datetime] = None,
    periodo_fin: Optional[datetime] = None,
    producto: Optional[str] = None,
    planta: Optional[str] = None
) -> int:
    """Cuenta el total de ventas con filtros opcionales."""
    query = select(func.count(Venta.id))
    
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
        .options(selectinload(CargaProveedor.proveedor))
    )
    return result.scalar_one_or_none()


async def get_carga_proveedor_by_codigo(db: AsyncSession, codigo_proveedor: str) -> Optional[CargaProveedor]:
    """Obtiene un registro de carga de proveedor por código de proveedor."""
    result = await db.execute(
        select(CargaProveedor)
        .where(CargaProveedor.codigo_proveedor == codigo_proveedor)
        .options(selectinload(CargaProveedor.proveedor))
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
    query = select(CargaProveedor).options(selectinload(CargaProveedor.proveedor))
    
    if codigo_proveedor:
        query = query.where(CargaProveedor.codigo_proveedor.ilike(f"%{codigo_proveedor}%"))
    
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
        query = query.where(CargaProveedor.codigo_proveedor.ilike(f"%{codigo_proveedor}%"))
    
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
        CargaProveedorHistorial.codigo_proveedor == codigo_proveedor,
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
        query = query.where(CargaProveedorHistorial.codigo_proveedor.ilike(f"%{codigo_proveedor}%"))
    
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
        query = query.where(CargaProveedorHistorial.codigo_proveedor.ilike(f"%{codigo_proveedor}%"))
    
    if operacion:
        query = query.where(CargaProveedorHistorial.operacion == operacion)
    
    result = await db.execute(query)
    return result.scalar() or 0


async def get_historial_por_proveedor(
    db: AsyncSession,
    codigo_proveedor: str,
    limit: int = 50
) -> List[CargaProveedorHistorial]:
    """Obtiene el historial de un proveedor específico."""
    query = select(CargaProveedorHistorial).where(
        CargaProveedorHistorial.codigo_proveedor == codigo_proveedor
    ).order_by(desc(CargaProveedorHistorial.created_at)).limit(limit)
    
    result = await db.execute(query)
    return list(result.scalars().all())


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
    estatus: Optional[str] = None
) -> List[CargaCliente]:
    """Lista registros de carga de clientes con filtros opcionales."""
    query = select(CargaCliente)
    
    if codigo_cliente is not None:
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
    estatus: Optional[str] = None
) -> int:
    """Cuenta el total de registros de carga de clientes con filtros opcionales."""
    query = select(func.count(CargaCliente.id))
    
    if codigo_cliente is not None:
        query = query.where(CargaCliente.codigo_cliente == codigo_cliente)
    
    if cliente_proveedor:
        query = query.where(CargaCliente.cliente_proveedor.ilike(f"%{cliente_proveedor}%"))
    
    if estatus:
        query = query.where(CargaCliente.estatus == estatus)
    
    result = await db.execute(query)
    return result.scalar() or 0


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
            # Crear nuevo cliente con estatus Alta
            nuevo_cliente = CargaCliente(
                codigo_cliente=codigo,
                nombre=cliente_venta.get("nombre"),
                cliente_proveedor="Cliente",
                estatus="Alta"
            )
            db.add(nuevo_cliente)
            await db.flush()  # Para obtener el ID
            
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
    
    # 5. Commit de todos los cambios
    await db.commit()
    
    return resumen


# ============================================================================
# CRUD para MasterUnificadoVirtuales
# ============================================================================

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
    firma: Optional[str] = None
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
        firma=firma
    )
    db.add(master)
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
    mes: Optional[str] = None
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
        query = query.where(MasterUnificadoVirtuales.mes == mes)
    
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
        query = query.where(MasterUnificadoVirtuales.mes == mes)
    
    result = await db.execute(query)
    return result.scalar() or 0


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
    tipo_exportacion: Optional[str] = None
) -> Optional[MasterUnificadoVirtuales]:
    """Actualiza un registro de master unificado virtuales. Identificador: numero."""
    master = await get_master_unificado_virtuales_by_numero(db, numero)
    if not master:
        return None
    
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
    
    await db.commit()
    await db.refresh(master)
    return master


async def delete_master_unificado_virtuales(db: AsyncSession, numero: int) -> bool:
    """Elimina un registro de master unificado virtuales. Identificador: numero."""
    master = await get_master_unificado_virtuales_by_numero(db, numero)
    if not master:
        return False
    
    await db.delete(master)
    await db.commit()
    return True
