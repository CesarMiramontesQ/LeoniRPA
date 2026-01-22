"""Operaciones CRUD para usuarios, ejecuciones y BOM."""
from sqlalchemy import select, desc, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.postgresql import insert
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal
from app.db.models import User, ExecutionHistory, SalesExecutionHistory, ExecutionStatus, Part, BomFlat, PartRole, Proveedor, Material, PrecioMaterial, Compra
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

async def get_proveedor_by_id(db: AsyncSession, proveedor_id: int) -> Optional[Proveedor]:
    """Obtiene un proveedor por ID."""
    result = await db.execute(select(Proveedor).where(Proveedor.id == proveedor_id))
    return result.scalar_one_or_none()


async def get_proveedor_by_codigo_cliente(db: AsyncSession, codigo_cliente: str) -> Optional[Proveedor]:
    """Obtiene un proveedor por código de cliente."""
    result = await db.execute(select(Proveedor).where(Proveedor.codigo_cliente == codigo_cliente))
    return result.scalar_one_or_none()


async def create_proveedor(
    db: AsyncSession,
    nombre: str,
    pais: Optional[str] = None,
    domicilio: Optional[str] = None,
    estatus: bool = True,
    codigo_cliente: Optional[str] = None
) -> Proveedor:
    """Crea un nuevo proveedor."""
    proveedor = Proveedor(
        nombre=nombre,
        pais=pais,
        domicilio=domicilio,
        estatus=estatus,
        codigo_cliente=codigo_cliente
    )
    db.add(proveedor)
    await db.commit()
    await db.refresh(proveedor)
    return proveedor


async def update_proveedor(
    db: AsyncSession,
    proveedor_id: int,
    nombre: Optional[str] = None,
    pais: Optional[str] = None,
    domicilio: Optional[str] = None,
    estatus: Optional[bool] = None,
    codigo_cliente: Optional[str] = None
) -> Optional[Proveedor]:
    """Actualiza un proveedor."""
    proveedor = await get_proveedor_by_id(db, proveedor_id)
    if not proveedor:
        return None
    
    # Verificar si el código_cliente ya existe en otro proveedor
    if codigo_cliente is not None:
        existing_proveedor = await get_proveedor_by_codigo_cliente(db, codigo_cliente)
        if existing_proveedor and existing_proveedor.id != proveedor_id:
            raise ValueError("El código de cliente ya está registrado")
    
    if nombre is not None:
        proveedor.nombre = nombre
    if pais is not None:
        proveedor.pais = pais
    if domicilio is not None:
        proveedor.domicilio = domicilio
    if estatus is not None:
        proveedor.estatus = estatus
    if codigo_cliente is not None:
        proveedor.codigo_cliente = codigo_cliente
    
    await db.commit()
    await db.refresh(proveedor)
    return proveedor


async def delete_proveedor(db: AsyncSession, proveedor_id: int) -> bool:
    """Elimina un proveedor."""
    from sqlalchemy import delete
    
    proveedor = await get_proveedor_by_id(db, proveedor_id)
    if not proveedor:
        return False
    
    stmt = delete(Proveedor).where(Proveedor.id == proveedor_id)
    await db.execute(stmt)
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
                Proveedor.codigo_cliente.ilike(search_pattern),
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
    query = select(func.count(Proveedor.id))
    
    if estatus is not None:
        query = query.where(Proveedor.estatus == estatus)
    
    if pais:
        query = query.where(Proveedor.pais == pais)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Proveedor.nombre.ilike(search_pattern),
                Proveedor.codigo_cliente.ilike(search_pattern),
                Proveedor.domicilio.ilike(search_pattern)
            )
        )
    
    result = await db.execute(query)
    return result.scalar_one()


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
    codigo_cliente: str,
    numero_material: str
) -> Optional[PrecioMaterial]:
    """Obtiene un precio de material por código de cliente y número de material."""
    result = await db.execute(
        select(PrecioMaterial).where(
            PrecioMaterial.codigo_cliente == codigo_cliente,
            PrecioMaterial.numero_material == numero_material
        )
    )
    return result.scalar_one_or_none()


async def create_precio_material(
    db: AsyncSession,
    codigo_cliente: str,
    numero_material: str,
    precio: Decimal,
    currency_uom: Optional[str] = None,
    country_origin: Optional[str] = None,
    Porcentaje_Compra: Optional[Decimal] = None,
    Comentario: Optional[str] = None
) -> PrecioMaterial:
    """Crea un nuevo precio de material."""
    precio_material = PrecioMaterial(
        codigo_cliente=codigo_cliente,
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
    codigo_cliente: str,
    numero_material: str,
    precio: Decimal,
    currency_uom: Optional[str] = None,
    country_origin: Optional[str] = None,
    Porcentaje_Compra: Optional[Decimal] = None,
    Comentario: Optional[str] = None
) -> PrecioMaterial:
    """Crea o actualiza un precio de material (upsert)."""
    existing = await get_precio_material_by_proveedor_material(db, codigo_cliente, numero_material)
    
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
            codigo_cliente=codigo_cliente,
            numero_material=numero_material,
            precio=precio,
            currency_uom=currency_uom,
            country_origin=country_origin,
            Porcentaje_Compra=Porcentaje_Compra,
            Comentario=Comentario
        )


async def list_precios_materiales(
    db: AsyncSession,
    codigo_cliente: Optional[str] = None,
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
    
    if codigo_cliente:
        query = query.where(PrecioMaterial.codigo_cliente == codigo_cliente)
    
    if numero_material:
        query = query.where(PrecioMaterial.numero_material == numero_material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PrecioMaterial.codigo_cliente.ilike(search_pattern),
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
    codigo_cliente: Optional[str] = None,
    numero_material: Optional[str] = None,
    search: Optional[str] = None
) -> int:
    """Cuenta el total de precios de materiales con filtros opcionales."""
    query = select(func.count(PrecioMaterial.id))
    
    if codigo_cliente:
        query = query.where(PrecioMaterial.codigo_cliente == codigo_cliente)
    
    if numero_material:
        query = query.where(PrecioMaterial.numero_material == numero_material)
    
    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                PrecioMaterial.codigo_cliente.ilike(search_pattern),
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
    gr_blck_stock_oun: Optional[Decimal] = None,
    gr_blocked_stck_opun: Optional[Decimal] = None,
    delivery_completed: Optional[str] = None,
    fisc_year_ref_doc: Optional[str] = None,
    reference_document: Optional[str] = None,
    reference_doc_item: Optional[str] = None,
    invoice_value: Optional[Decimal] = None,
    numero_material: Optional[str] = None,
    plant: Optional[str] = None,
    descripcion_material: Optional[str] = None,
    nombre_proveedor: Optional[str] = None,
    numero_proveedor: Optional[int] = None,
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
        gr_blck_stock_oun=gr_blck_stock_oun,
        gr_blocked_stck_opun=gr_blocked_stck_opun,
        delivery_completed=delivery_completed,
        fisc_year_ref_doc=fisc_year_ref_doc,
        reference_document=reference_document,
        reference_doc_item=reference_doc_item,
        invoice_value=invoice_value,
        numero_material=numero_material,
        plant=plant,
        descripcion_material=descripcion_material,
        nombre_proveedor=nombre_proveedor,
        numero_proveedor=numero_proveedor,
        price=price,
    )
    db.add(db_compra)
    await db.commit()
    await db.refresh(db_compra)
    return db_compra


async def get_compra_by_purchasing_and_material(
    db: AsyncSession,
    purchasing_document: Optional[int],
    numero_material: Optional[str]
) -> Optional[Compra]:
    """Busca una compra por purchasing_document y numero_material."""
    if purchasing_document is None or numero_material is None:
        return None
    
    query = select(Compra).where(
        Compra.purchasing_document == purchasing_document,
        Compra.numero_material == numero_material
    )
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def bulk_create_or_update_compras(
    db: AsyncSession,
    compras_data: List[Dict[str, Any]]
) -> Dict[str, int]:
    """Inserta o actualiza múltiples registros de compras.
    Si ya existe un registro con la misma combinación de purchasing_document y numero_material,
    se actualiza. Si no existe, se crea uno nuevo.
    
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
        purchasing_doc = compra_data.get('purchasing_document')
        numero_mat = compra_data.get('numero_material')
        
        # Buscar si ya existe
        compra_existente = await get_compra_by_purchasing_and_material(
            db, purchasing_doc, numero_mat
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
    offset: int = 0
) -> List[Compra]:
    """Lista compras con paginación."""
    query = select(Compra).order_by(desc(Compra.created_at)).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return list(result.scalars().all())


async def count_compras(
    db: AsyncSession
) -> int:
    """Cuenta el total de compras."""
    query = select(func.count(Compra.id))
    result = await db.execute(query)
    return result.scalar() or 0
