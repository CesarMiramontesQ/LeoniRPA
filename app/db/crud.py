"""Operaciones CRUD para usuarios, ejecuciones y BOM."""
from sqlalchemy import select, desc, func, or_, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.postgresql import insert
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal
from app.db.models import User, ExecutionHistory, SalesExecutionHistory, ExecutionStatus, Part, BomFlat, PartRole, Proveedor, Material, PrecioMaterial, Compra, PaisOrigenMaterial, ProveedorHistorial, ProveedorOperacion
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


# ==================== CRUD para Materiales ====================

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
    descripcion_material: Optional[str] = None
) -> Material:
    """Crea un nuevo material."""
    material = Material(
        numero_material=numero_material,
        descripcion_material=descripcion_material
    )
    db.add(material)
    await db.commit()
    await db.refresh(material)
    return material


async def update_material(
    db: AsyncSession,
    material_id: int,
    descripcion_material: Optional[str] = None
) -> Optional[Material]:
    """Actualiza un material."""
    material = await get_material_by_id(db, material_id)
    if not material:
        return None
    
    if descripcion_material is not None:
        material.descripcion_material = descripcion_material
    
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
    return result.scalar_one()


# ==================== CRUD para Precios Materiales ====================

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
    Comentario: Optional[str] = None
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
    await db.commit()
    await db.refresh(precio_material)
    return precio_material


async def update_precio_material(
    db: AsyncSession,
    precio_id: int,
    precio: Optional[Decimal] = None,
    currency_uom: Optional[str] = None,
    country_origin: Optional[str] = None,
    Porcentaje_Compra: Optional[Decimal] = None,
    Comentario: Optional[str] = None
) -> Optional[PrecioMaterial]:
    """Actualiza un precio de material."""
    precio_material = await get_precio_material_by_id(db, precio_id)
    if not precio_material:
        return None
    
    if precio is not None:
        precio_material.precio = precio
    if currency_uom is not None:
        precio_material.currency_uom = currency_uom
    if country_origin is not None:
        precio_material.country_origin = country_origin
    if Porcentaje_Compra is not None:
        precio_material.Porcentaje_Compra = Porcentaje_Compra
    if Comentario is not None:
        precio_material.Comentario = Comentario
    
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
    Comentario: Optional[str] = None
) -> PrecioMaterial:
    """Crea o actualiza un precio de material (upsert)."""
    existing = await get_precio_material_by_proveedor_material(db, codigo_proveedor, numero_material)
    
    if existing:
        # Actualizar existente
        if precio is not None:
            existing.precio = precio
        if currency_uom is not None:
            existing.currency_uom = currency_uom
        if country_origin is not None:
            existing.country_origin = country_origin
        if Porcentaje_Compra is not None:
            existing.Porcentaje_Compra = Porcentaje_Compra
        if Comentario is not None:
            existing.Comentario = Comentario
        
        await db.commit()
        await db.refresh(existing)
        return existing
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
            Comentario=Comentario
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
    pais_origen: str
) -> PaisOrigenMaterial:
    """Crea un nuevo país de origen de material."""
    pais_origen_material = PaisOrigenMaterial(
        codigo_proveedor=codigo_proveedor,
        numero_material=numero_material,
        pais_origen=pais_origen
    )
    db.add(pais_origen_material)
    await db.commit()
    await db.refresh(pais_origen_material)
    return pais_origen_material


async def update_pais_origen_material(
    db: AsyncSession,
    pais_id: int,
    pais_origen: Optional[str] = None
) -> Optional[PaisOrigenMaterial]:
    """Actualiza un país de origen de material."""
    pais_origen_material = await get_pais_origen_material_by_id(db, pais_id)
    if not pais_origen_material:
        return None
    
    if pais_origen is not None:
        pais_origen_material.pais_origen = pais_origen
    
    await db.commit()
    await db.refresh(pais_origen_material)
    return pais_origen_material


async def upsert_pais_origen_material(
    db: AsyncSession,
    codigo_proveedor: str,
    numero_material: str,
    pais_origen: str
) -> PaisOrigenMaterial:
    """Crea o actualiza un país de origen de material (upsert)."""
    existing = await get_pais_origen_material_by_proveedor_material(db, codigo_proveedor, numero_material)
    
    if existing:
        # Actualizar existente
        existing.pais_origen = pais_origen
        await db.commit()
        await db.refresh(existing)
        return existing
    else:
        # Crear nuevo
        return await create_pais_origen_material(
            db,
            codigo_proveedor=codigo_proveedor,
            numero_material=numero_material,
            pais_origen=pais_origen
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
