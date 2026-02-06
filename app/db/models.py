"""Modelos de base de datos."""
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, ForeignKey, Text, Enum as SQLEnum, Numeric, Index, UniqueConstraint, CheckConstraint, Date
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


class Proveedor(Base):
    """Modelo para proveedores."""
    __tablename__ = "proveedores"
    
    # Código Proveedor (clave primaria)
    codigo_proveedor = Column(String, primary_key=True, index=True)
    
    # Nombre del proveedor
    nombre = Column(String, nullable=False, index=True)
    
    # País
    pais = Column(String, nullable=True, index=True)
    
    # Domicilio
    domicilio = Column(Text, nullable=True)
    
    # Población
    poblacion = Column(String, nullable=True)
    
    # Código Postal
    cp = Column(String, nullable=True)
    
    # Estatus (activo/inactivo)
    estatus = Column(Boolean, default=True, nullable=False, index=True)
    
    # Estatus de compras
    estatus_compras = Column(String, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<Proveedor(codigo_proveedor={self.codigo_proveedor}, nombre={self.nombre})>"


class ProveedorOperacion(PyEnum):
    """Tipos de operaciones en el historial de proveedores."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class ProveedorHistorial(Base):
    """Modelo para el historial de cambios en proveedores."""
    __tablename__ = "proveedores_historial"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Código del proveedor (puede ser NULL si se elimina)
    codigo_proveedor = Column(String, nullable=True, index=True)
    
    # Tipo de operación
    operacion = Column(
        SQLEnum(ProveedorOperacion, name="proveedor_operacion_enum"),
        nullable=False,
        index=True
    )
    
    # Usuario que realizó la operación
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user = relationship("User", backref="proveedores_historial")
    
    # Datos antes del cambio (NULL para CREATE)
    datos_antes = Column(JSONB, nullable=True)
    
    # Datos después del cambio (NULL para DELETE)
    datos_despues = Column(JSONB, nullable=True)
    
    # Campos que cambiaron (solo para UPDATE)
    campos_modificados = Column(JSONB, nullable=True)
    
    # Comentario opcional
    comentario = Column(Text, nullable=True)
    
    # Timestamp
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    
    def __repr__(self):
        return f"<ProveedorHistorial(id={self.id}, codigo_proveedor={self.codigo_proveedor}, operacion={self.operacion.value}, created_at={self.created_at})>"


class Material(Base):
    """Modelo para materiales."""
    __tablename__ = "materiales"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Número de material
    numero_material = Column(String, unique=True, nullable=False, index=True)
    
    # Descripción del material
    descripcion_material = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relaciones
    precios = relationship("PrecioMaterial", back_populates="material")
    
    def __repr__(self):
        return f"<Material(id={self.id}, numero_material={self.numero_material}, descripcion_material={self.descripcion_material})>"


class MaterialOperacion(PyEnum):
    """Tipos de operaciones en el historial de materiales."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class MaterialHistorial(Base):
    """Modelo para el historial de cambios en materiales."""
    __tablename__ = "materiales_historial"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Número del material (puede ser NULL si se elimina)
    numero_material = Column(String, nullable=True, index=True)
    
    # ID del material (referencia adicional)
    material_id = Column(Integer, nullable=True, index=True)
    
    # Tipo de operación
    operacion = Column(
        SQLEnum(MaterialOperacion, name="material_operacion_enum"),
        nullable=False,
        index=True
    )
    
    # Usuario que realizó la operación
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user = relationship("User", backref="materiales_historial")
    
    # Datos antes del cambio (NULL para CREATE)
    datos_antes = Column(JSONB, nullable=True)
    
    # Datos después del cambio (NULL para DELETE)
    datos_despues = Column(JSONB, nullable=True)
    
    # Campos que cambiaron (solo para UPDATE)
    campos_modificados = Column(JSONB, nullable=True)
    
    # Comentario opcional
    comentario = Column(Text, nullable=True)
    
    # Timestamp
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    
    def __repr__(self):
        return f"<MaterialHistorial(id={self.id}, numero_material={self.numero_material}, operacion={self.operacion.value}, created_at={self.created_at})>"


class PrecioMaterial(Base):
    """Modelo para precios de materiales por proveedor."""
    __tablename__ = "precios_materiales"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Claves foráneas
    codigo_proveedor = Column(String, ForeignKey("proveedores.codigo_proveedor"), nullable=False, index=True)
    numero_material = Column(String, ForeignKey("materiales.numero_material"), nullable=False, index=True)
    
    # Precio
    precio = Column(Numeric(18, 6), nullable=False)
    
    # Moneda y unidad de medida (ej. USD/KG, EUR/KG)
    currency_uom = Column(String, nullable=True)
    
    # País de origen
    country_origin = Column(String, nullable=True)
    
    # Porcentaje de compra
    Porcentaje_Compra = Column(Numeric(18, 6), nullable=True)
    
    # Comentario
    Comentario = Column(Text, nullable=True)
    
    # Timestamp de actualización
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relaciones
    proveedor = relationship("Proveedor", foreign_keys=[codigo_proveedor], backref="precios_materiales")
    material = relationship("Material", foreign_keys=[numero_material], back_populates="precios")
    
    # Restricciones
    __table_args__ = (
        # Constraint único: un solo precio vigente por combinación de proveedor y material
        UniqueConstraint(
            'codigo_proveedor', 'numero_material',
            name='uq_precios_materiales_proveedor_material'
        ),
    )
    
    def __repr__(self):
        return f"<PrecioMaterial(id={self.id}, codigo_proveedor={self.codigo_proveedor}, numero_material={self.numero_material}, precio={self.precio})>"


class Compra(Base):
    """Modelo para compras."""
    __tablename__ = "compras"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Purchasing Document
    purchasing_document = Column(BigInteger, nullable=True, index=True)
    
    # Item
    item = Column(Integer, nullable=True)
    
    # Material Doc. Year
    material_doc_year = Column(Integer, nullable=True, index=True)
    
    # Material Document
    material_document = Column(BigInteger, nullable=True, index=True)
    
    # Material Doc.Item
    material_doc_item = Column(Integer, nullable=True)
    
    # Movement Type
    movement_type = Column(String, nullable=True)
    
    # Posting Date
    posting_date = Column(DateTime(timezone=True), nullable=True, index=True)
    
    # Quantity
    quantity = Column(Integer, nullable=True)
    
    # Order Unit
    order_unit = Column(String, nullable=True)
    
    # Quantity in OPUn
    quantity_in_opun = Column(Integer, nullable=True)
    
    # Order Price Unit
    order_price_unit = Column(String, nullable=True)
    
    # Amount in LC
    amount_in_lc = Column(Numeric(18, 6), nullable=True)
    
    # Local currency
    local_currency = Column(String, nullable=True)
    
    # Amount
    amount = Column(Numeric(18, 6), nullable=True)
    
    # Currency
    currency = Column(String, nullable=True)
    
    # GR/IR clearing value in local currency
    gr_ir_clearing_value_lc = Column(Numeric(18, 6), nullable=True)
    
    # Invoice Value
    invoice_value = Column(Numeric(18, 6), nullable=True)
    
    # numero_Material
    numero_material = Column(String, nullable=True, index=True)
    
    # Plant
    plant = Column(String, nullable=True, index=True)
    
    # descripcion_material
    descripcion_material = Column(Text, nullable=True)
    
    # nombre_proveedor
    nombre_proveedor = Column(String, nullable=True, index=True)
    
    # codigo_proveedor
    codigo_proveedor = Column(String, nullable=True, index=True)
    
    # price
    price = Column(Numeric(18, 6), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<Compra(id={self.id}, purchasing_document={self.purchasing_document}, material_document={self.material_document}, posting_date={self.posting_date})>"


class PaisOrigenMaterial(Base):
    """Modelo para país de origen de materiales por proveedor."""
    __tablename__ = "pais_origen_material"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Claves foráneas
    codigo_proveedor = Column(String, ForeignKey("proveedores.codigo_proveedor"), nullable=False, index=True)
    numero_material = Column(String, ForeignKey("materiales.numero_material"), nullable=False, index=True)
    
    # País de origen
    pais_origen = Column(String, nullable=False)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relaciones
    proveedor = relationship("Proveedor", foreign_keys=[codigo_proveedor], backref="paises_origen_materiales")
    material = relationship("Material", foreign_keys=[numero_material], backref="paises_origen")
    
    # Restricciones
    __table_args__ = (
        # Constraint único: un solo país de origen por combinación de proveedor y material
        UniqueConstraint(
            'codigo_proveedor', 'numero_material',
            name='uq_pais_origen_material_proveedor_material'
        ),
    )
    
    def __repr__(self):
        return f"<PaisOrigenMaterial(id={self.id}, codigo_proveedor={self.codigo_proveedor}, numero_material={self.numero_material}, pais_origen={self.pais_origen})>"


class PaisOrigenMaterialOperacion(PyEnum):
    """Tipos de operaciones en el historial de países de origen."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class PaisOrigenMaterialHistorial(Base):
    """Modelo para el historial de cambios en países de origen de materiales."""
    __tablename__ = "pais_origen_material_historial"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # ID del país de origen (puede ser NULL si se elimina)
    pais_origen_id = Column(Integer, nullable=True, index=True)
    
    # Código del proveedor (puede ser NULL si se elimina)
    codigo_proveedor = Column(String, nullable=True, index=True)
    
    # Número del material (puede ser NULL si se elimina)
    numero_material = Column(String, nullable=True, index=True)
    
    # Tipo de operación
    operacion = Column(
        SQLEnum(PaisOrigenMaterialOperacion, name="pais_origen_material_operacion_enum"),
        nullable=False,
        index=True
    )
    
    # Usuario que realizó la operación
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user = relationship("User", backref="pais_origen_material_historial")
    
    # Datos antes del cambio (NULL para CREATE)
    datos_antes = Column(JSONB, nullable=True)
    
    # Datos después del cambio (NULL para DELETE)
    datos_despues = Column(JSONB, nullable=True)
    
    # Campos que cambiaron (solo para UPDATE)
    campos_modificados = Column(JSONB, nullable=True)
    
    # Comentario opcional
    comentario = Column(Text, nullable=True)
    
    # Timestamp
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    
    def __repr__(self):
        return f"<PaisOrigenMaterialHistorial(id={self.id}, pais_origen_id={self.pais_origen_id}, operacion={self.operacion.value}, created_at={self.created_at})>"


class PrecioMaterialOperacion(PyEnum):
    """Tipos de operaciones en el historial de precios de materiales."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class PrecioMaterialHistorial(Base):
    """Modelo para el historial de cambios en precios de materiales."""
    __tablename__ = "precios_materiales_historial"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # ID del precio de material (puede ser NULL si se elimina)
    precio_material_id = Column(Integer, nullable=True, index=True)
    
    # Código del proveedor (puede ser NULL si se elimina)
    codigo_proveedor = Column(String, nullable=True, index=True)
    
    # Número del material (puede ser NULL si se elimina)
    numero_material = Column(String, nullable=True, index=True)
    
    # Tipo de operación
    operacion = Column(
        SQLEnum(PrecioMaterialOperacion, name="precio_material_operacion_enum"),
        nullable=False,
        index=True
    )
    
    # Usuario que realizó la operación
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user = relationship("User", backref="precios_materiales_historial")
    
    # Datos antes del cambio (NULL para CREATE)
    datos_antes = Column(JSONB, nullable=True)
    
    # Datos después del cambio (NULL para DELETE)
    datos_despues = Column(JSONB, nullable=True)
    
    # Campos que cambiaron (solo para UPDATE)
    campos_modificados = Column(JSONB, nullable=True)
    
    # Comentario opcional
    comentario = Column(Text, nullable=True)
    
    # Timestamp
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    
    def __repr__(self):
        return f"<PrecioMaterialHistorial(id={self.id}, precio_material_id={self.precio_material_id}, operacion={self.operacion.value}, created_at={self.created_at})>"


class ClienteGrupo(Base):
    """Modelo para grupos de clientes."""
    __tablename__ = "cliente_grupo"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Código del cliente
    codigo_cliente = Column(Integer, nullable=False, index=True)
    
    # Grupo viejo
    grupo_viejo = Column(String, nullable=True)
    
    # Grupo
    grupo = Column(String, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<ClienteGrupo(id={self.id}, codigo_cliente={self.codigo_cliente}, grupo={self.grupo})>"


class Venta(Base):
    """Modelo para ventas."""
    __tablename__ = "ventas"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Cliente
    cliente = Column(String, nullable=True, index=True)
    
    # Código del cliente (FK)
    codigo_cliente = Column(BigInteger, nullable=True, index=True)
    
    # Grupo (FK a ClienteGrupo)
    grupo_id = Column(Integer, ForeignKey("cliente_grupo.id"), nullable=True, index=True)
    grupo = relationship("ClienteGrupo", foreign_keys=[grupo_id], backref="ventas")
    
    # Unidad de negocio
    unidad_negocio = Column(String, nullable=True)
    
    # Período (mes y año) - almacenado como Date (primer día del mes)
    periodo = Column(Date, nullable=True, index=True)
    
    # Producto condensado
    producto_condensado = Column(String, nullable=True)
    
    # Región ASC
    region_asc = Column(String, nullable=True)
    
    # Planta
    planta = Column(String, nullable=True, index=True)
    
    # Ship to party
    ship_to_party = Column(String, nullable=True)
    
    # Producto
    producto = Column(String, nullable=True, index=True)
    
    # Descripción del producto
    descripcion_producto = Column(Text, nullable=True)
    
    # Turnover w/o metal
    turnover_wo_metal = Column(Numeric(18, 6), nullable=True)
    
    # OE/Turnover like FI
    oe_turnover_like_fi = Column(Numeric(18, 6), nullable=True)
    
    # Copper Sales (CUV)
    copper_sales_cuv = Column(Numeric(18, 6), nullable=True)
    
    # CU-Sales effect
    cu_sales_effect = Column(Numeric(18, 6), nullable=True)
    
    # CU result
    cu_result = Column(Numeric(18, 6), nullable=True)
    
    # Quantity OE/TO M
    quantity_oe_to_m = Column(Numeric(18, 6), nullable=True)
    
    # Quantity OE/TO FT
    quantity_oe_to_ft = Column(Numeric(18, 6), nullable=True)
    
    # CU Weight techn. CUT
    cu_weight_techn_cut = Column(Numeric(18, 6), nullable=True)
    
    # CU weight Sales CUV
    cu_weight_sales_cuv = Column(Numeric(18, 6), nullable=True)
    
    # Conversion de FT a M
    conversion_ft_a_m = Column(Numeric(18, 6), nullable=True)
    
    # Sales total MTS
    sales_total_mts = Column(Numeric(18, 6), nullable=True)
    
    # Sales KM
    sales_km = Column(Numeric(18, 6), nullable=True)
    
    # Precio Exmetal KM
    precio_exmetal_km = Column(Numeric(18, 6), nullable=True)
    
    # Precio Full Metal KM
    precio_full_metal_km = Column(Numeric(18, 6), nullable=True)
    
    # Precio Exmetal M
    precio_exmetal_m = Column(Numeric(18, 6), nullable=True)
    
    # Precio Full Metal M
    precio_full_metal_m = Column(Numeric(18, 6), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<Venta(id={self.id}, cliente={self.cliente}, codigo_cliente={self.codigo_cliente}, periodo={self.periodo})>"


class CargaProveedor(Base):
    """Modelo para carga de proveedores en aduanas."""
    __tablename__ = "carga_proveedores"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Clave foránea a proveedores
    codigo_proveedor = Column(String, ForeignKey("proveedores.codigo_proveedor"), nullable=False, index=True)
    
    # Nombre (referencia a proveedores.nombre a través de la relación)
    nombre = Column(String, nullable=True)
    
    # Apellidos
    apellido_paterno = Column(String, nullable=True)
    apellido_materno = Column(String, nullable=True)
    
    # País (referencia a proveedores.pais a través de la relación)
    pais = Column(String, nullable=True, index=True)
    
    # Domicilio (referencia a proveedores.domicilio a través de la relación)
    domicilio = Column(Text, nullable=True)
    
    # Cliente o Proveedor
    cliente_proveedor = Column(String, nullable=True)
    
    # Estatus
    estatus = Column(String, nullable=True, index=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relaciones
    proveedor = relationship("Proveedor", foreign_keys=[codigo_proveedor], backref="carga_proveedores")
    
    def __repr__(self):
        return f"<CargaProveedor(id={self.id}, codigo_proveedor={self.codigo_proveedor}, cliente_proveedor={self.cliente_proveedor})>"


class CargaProveedorOperacion(PyEnum):
    """Tipos de operaciones en el historial de carga de proveedores."""
    CREATE = "CREATE"      # Proveedor nuevo agregado (Alta)
    UPDATE = "UPDATE"      # Estatus modificado
    DELETE = "DELETE"      # Proveedor eliminado


class CargaProveedorHistorial(Base):
    """Modelo para el historial de cambios en carga de proveedores."""
    __tablename__ = "carga_proveedores_historial"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # ID del registro de carga_proveedores
    carga_proveedor_id = Column(Integer, nullable=True, index=True)
    
    # Código del proveedor
    codigo_proveedor = Column(String, nullable=True, index=True)
    
    # Tipo de operación
    operacion = Column(
        SQLEnum(CargaProveedorOperacion, name="carga_proveedor_operacion_enum"),
        nullable=False,
        index=True
    )
    
    # Estatus anterior (antes del cambio)
    estatus_anterior = Column(String, nullable=True)
    
    # Estatus nuevo (después del cambio)
    estatus_nuevo = Column(String, nullable=True)
    
    # Datos completos antes del cambio (JSON)
    datos_antes = Column(JSONB, nullable=True)
    
    # Datos completos después del cambio (JSON)
    datos_despues = Column(JSONB, nullable=True)
    
    # Motivo del cambio
    motivo = Column(String, nullable=True)
    
    # Timestamp
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    
    def __repr__(self):
        return f"<CargaProveedorHistorial(id={self.id}, codigo_proveedor={self.codigo_proveedor}, operacion={self.operacion.value}, created_at={self.created_at})>"


class CargaProveedoresNacional(Base):
    """Modelo para carga de proveedores nacionales."""
    __tablename__ = "carga_proveedores_nacional"

    id = Column(Integer, primary_key=True, index=True)

    codigo_proveedor = Column(String, nullable=True, index=True)
    nombre = Column(String, nullable=True)
    apellido_paterno = Column(String, nullable=True)
    apellido_materno = Column(String, nullable=True)
    pais = Column(String, nullable=True, index=True)
    domicilio = Column(Text, nullable=True)
    cliente_proveedor = Column(String, nullable=True)
    estatus = Column(String, nullable=True, index=True)
    rfc = Column(String, nullable=True, index=True)
    operacion = Column(String, nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<CargaProveedoresNacional(id={self.id}, codigo_proveedor={self.codigo_proveedor}, operacion={self.operacion})>"


class CargaProveedoresNacionalHistorial(Base):
    """Historial de cambios en carga de proveedores nacionales (solo MX)."""
    __tablename__ = "carga_proveedores_nacional_historial"

    id = Column(Integer, primary_key=True, index=True)
    carga_proveedores_nacional_id = Column(Integer, nullable=True, index=True)
    codigo_proveedor = Column(String, nullable=True, index=True)
    operacion = Column(String, nullable=False, index=True)  # CREATE, UPDATE, DELETE
    estatus_anterior = Column(String, nullable=True)
    estatus_nuevo = Column(String, nullable=True)
    motivo = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)

    def __repr__(self):
        return f"<CargaProveedoresNacionalHistorial(id={self.id}, codigo_proveedor={self.codigo_proveedor}, operacion={self.operacion})>"


class CargaCliente(Base):
    """Modelo para carga de clientes en aduanas."""
    __tablename__ = "carga_clientes"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Código del cliente (Integer, similar a ClienteGrupo y Venta)
    codigo_cliente = Column(BigInteger, nullable=False, index=True)
    
    # Nombre del cliente
    nombre = Column(String, nullable=True)
    
    # País
    pais = Column(String, nullable=True, index=True)
    
    # Domicilio
    domicilio = Column(String, nullable=True)
    
    # Cliente o Proveedor
    cliente_proveedor = Column(String, nullable=True)
    
    # Estatus
    estatus = Column(String, nullable=True, index=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<CargaCliente(id={self.id}, codigo_cliente={self.codigo_cliente}, nombre={self.nombre})>"


class CargaClienteOperacion(PyEnum):
    """Tipos de operaciones en el historial de carga de clientes."""
    CREATE = "CREATE"      # Cliente nuevo agregado (Alta)
    UPDATE = "UPDATE"      # Estatus modificado
    DELETE = "DELETE"      # Cliente eliminado
    EJECUCION = "EJECUCION"  # Ejecución del proceso de actualización


class CargaClienteHistorial(Base):
    """Modelo para el historial de cambios en carga de clientes."""
    __tablename__ = "carga_clientes_historial"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # ID del registro de carga_clientes
    carga_cliente_id = Column(Integer, nullable=True, index=True)
    
    # Código del cliente
    codigo_cliente = Column(BigInteger, nullable=True, index=True)
    
    # Tipo de operación
    operacion = Column(
        SQLEnum(CargaClienteOperacion, name="carga_cliente_operacion_enum"),
        nullable=False,
        index=True
    )
    
    # Estatus anterior (antes del cambio)
    estatus_anterior = Column(String, nullable=True)
    
    # Estatus nuevo (después del cambio)
    estatus_nuevo = Column(String, nullable=True)
    
    # Datos completos antes del cambio (JSON)
    datos_antes = Column(JSONB, nullable=True)
    
    # Datos completos después del cambio (JSON)
    datos_despues = Column(JSONB, nullable=True)
    
    # Motivo del cambio
    motivo = Column(String, nullable=True)
    
    # Timestamp
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    
    def __repr__(self):
        return f"<CargaClienteHistorial(id={self.id}, codigo_cliente={self.codigo_cliente}, operacion={self.operacion.value}, created_at={self.created_at})>"


class Cliente(Base):
    """Modelo para clientes."""
    __tablename__ = "clientes"
    
    # Código del cliente (clave primaria)
    codigo_cliente = Column(BigInteger, primary_key=True, index=True)
    
    # Nombre del cliente
    nombre = Column(String, nullable=False, index=True)
    
    # Domicilio
    domicilio = Column(Text, nullable=True)
    
    # País
    pais = Column(String, nullable=True, index=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<Cliente(codigo_cliente={self.codigo_cliente}, nombre={self.nombre})>"


class MasterUnificadoVirtuales(Base):
    """Modelo para master unificado de virtuales. Identificador: numero."""
    __tablename__ = "master_unificado_virtuales"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Solicitud previo
    solicitud_previo = Column(Boolean, nullable=True, default=False)
    
    # Agente
    agente = Column(String, nullable=True)
    
    # Pedimento
    pedimento = Column(BigInteger, nullable=True, index=True)
    
    # Aduana
    aduana = Column(Integer, nullable=True, index=True)
    
    # Patente
    patente = Column(Integer, nullable=True, index=True)
    
    # Destino
    destino = Column(Integer, nullable=True)
    
    # Cliente Space
    cliente_space = Column(String, nullable=True)
    
    # Impo/Expo
    impo_expo = Column(String, nullable=True)
    
    # Proveedor/Cliente
    proveedor_cliente = Column(String, nullable=True)
    
    # Mes
    mes = Column(String, nullable=True)
    
    # Complemento
    complemento = Column(String, nullable=True)
    
    # Tipo IMMEX
    tipo_immex = Column(String, nullable=True)
    
    # Factura
    factura = Column(String, nullable=True)
    
    # Fecha de pago
    fecha_pago = Column(Date, nullable=True, index=True)
    
    # Información
    informacion = Column(String, nullable=True)
    
    # Estatus
    estatus = Column(String, nullable=True, index=True)
    
    # OP Regular
    op_regular = Column(Boolean, nullable=True, default=False)

    # Tipo
    tipo = Column(String, nullable=True)
    
    # Número
    numero = Column(BigInteger, nullable=True, index=True)
    
    # Carretes
    carretes = Column(Boolean, nullable=True, default=False)
    
    # Servicio Cliente
    servicio_cliente = Column(String, nullable=True)
    
    # Plazo
    plazo = Column(String, nullable=True)
    
    # Firma
    firma = Column(String, nullable=True)
    
    # Incoterm
    incoterm = Column(String, nullable=True)
    
    # Tipo Exportación
    tipo_exportacion = Column(String, nullable=True)
    
    # Escenario
    escenario = Column(String, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<MasterUnificadoVirtuales(id={self.id}, pedimento={self.pedimento}, numero={self.numero})>"


class MasterUnificadoVirtualOperacion(PyEnum):
    """Tipos de operaciones registradas en el historial de virtuales."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class MasterUnificadoVirtualHistorial(Base):
    """Historial de cambios del master unificado de virtuales."""
    __tablename__ = "master_unificado_virtuales_historial"
    
    id = Column(Integer, primary_key=True, index=True)
    numero = Column(BigInteger, nullable=True, index=True)
    operacion = Column(
        SQLEnum(MasterUnificadoVirtualOperacion, name="master_virtual_operacion_enum"),
        nullable=False,
        index=True
    )
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user = relationship("User", backref="virtuales_historial")
    datos_antes = Column(JSONB, nullable=True)
    datos_despues = Column(JSONB, nullable=True)
    campos_modificados = Column(JSONB, nullable=True)
    comentario = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    
    def __repr__(self):
        return f"<MasterUnificadoVirtualHistorial(id={self.id}, numero={self.numero}, operacion={self.operacion.value})>"

