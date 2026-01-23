from fastapi import FastAPI, Request, Depends, Form, UploadFile, File
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.auth.router import router as auth_router, get_current_user, AuthenticationError
from app.db.init_db import init_db
from app.db.base import get_db
from app.db.models import User, ExecutionStatus
from app.db import crud
import threading
import sys
import socket
import platform
import os

# Para manejo de zonas horarias
try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Fallback para Python < 3.9
    from backports.zoneinfo import ZoneInfo

# Inicializar base de datos al iniciar
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicializa la base de datos al iniciar la aplicación."""
    await init_db()
    yield

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(GZipMiddleware)

# Exception handler para redirecciones de autenticación
@app.exception_handler(AuthenticationError)
async def authentication_exception_handler(request: Request, exc: AuthenticationError):
    """Redirige a login cuando hay error de autenticación."""
    return RedirectResponse(url="/auth/login", status_code=302)

# Incluir router de autenticación
app.include_router(auth_router)

# Redirigir raíz a dashboard
@app.get("/")
async def root():
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/dashboard")
async def dashboard(request: Request, current_user: User = Depends(get_current_user)):
    """Dashboard principal - requiere autenticación."""
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "current_user": current_user,
            "active_page": "dashboard"
        }
    )


@app.get("/ventas")
async def ventas(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de ventas - requiere autenticación."""
    # Obtener el historial de ejecuciones para mostrar en la tabla
    executions = await crud.list_sales_executions(db, user_id=current_user.id, limit=50)
    
    return templates.TemplateResponse(
        "ventas.html",
        {
            "request": request,
            "active_page": "ventas",
            "current_user": current_user,
            "executions": executions
        }
    )


def convertir_a_cdmx(dt: Optional[datetime]) -> Optional[datetime]:
    """Convierte una fecha UTC a la zona horaria de Ciudad de México."""
    if dt is None:
        return None
    
    # Si la fecha ya tiene timezone info, convertirla
    if dt.tzinfo is not None:
        return dt.astimezone(ZoneInfo("America/Mexico_City"))
    else:
        # Asumir que está en UTC si no tiene timezone
        dt_utc = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt_utc.astimezone(ZoneInfo("America/Mexico_City"))


# Agregar filtro personalizado a Jinja2 para convertir fechas a CDMX
def datetime_cdmx_filter(dt: Optional[datetime], format_str: str = '%d/%m/%Y %H:%M') -> str:
    """Filtro de Jinja2 para convertir datetime a CDMX y formatear."""
    if dt is None:
        return 'N/A'
    dt_cdmx = convertir_a_cdmx(dt)
    return dt_cdmx.strftime(format_str)


# Registrar el filtro en el entorno de templates
templates.env.filters['datetime_cdmx'] = datetime_cdmx_filter


@app.get("/compras")
async def compras(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de compras - requiere autenticación."""
    # Obtener el historial de ejecuciones para mostrar en la tabla (últimos 5)
    executions = await crud.list_executions(db, user_id=current_user.id, limit=5)
    
    return templates.TemplateResponse(
        "compras.html",
        {
            "request": request,
            "active_page": "compras",
            "current_user": current_user,
            "executions": executions
        }
    )


@app.get("/todas-compras")
async def todas_compras(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página para ver todas las compras con filtros - requiere autenticación."""
    # Obtener el total de compras para mostrar en la estadística
    total_compras = await crud.count_compras(db)
    
    return templates.TemplateResponse(
        "todas_compras.html",
        {
            "request": request,
            "active_page": "todas_compras",
            "current_user": current_user,
            "total_compras": total_compras
        }
    )


@app.get("/boms")
async def boms(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de BOMs - Lista de materiales - requiere autenticación."""
    # Cargar los BOMs desde la base de datos
    boms_data = await crud.list_bom_flat(db, limit=5000)
    
    # Calcular estadísticas
    total_registros = len(boms_data)
    partes_unicas = len(set(bom.fg_part_no for bom in boms_data))
    materiales_unicos = len(set(bom.material for bom in boms_data))
    
    return templates.TemplateResponse(
        "boms.html",
        {
            "request": request,
            "active_page": "boms",
            "current_user": current_user,
            "boms": boms_data,
            "total_registros": total_registros,
            "partes_unicas": partes_unicas,
            "materiales_unicos": materiales_unicos
        }
    )


@app.get("/proveedores")
async def proveedores(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de proveedores - requiere autenticación."""
    # Cargar los proveedores desde la base de datos
    proveedores_data = await crud.list_proveedores(db, limit=1000)
    
    # Calcular estadísticas
    total_proveedores = await crud.count_proveedores(db)
    proveedores_activos = await crud.count_proveedores(db, estatus=True)
    proveedores_inactivos = await crud.count_proveedores(db, estatus=False)
    
    return templates.TemplateResponse(
        "proveedores.html",
        {
            "request": request,
            "active_page": "proveedores",
            "current_user": current_user,
            "proveedores": proveedores_data,
            "total_proveedores": total_proveedores,
            "proveedores_activos": proveedores_activos,
            "proveedores_inactivos": proveedores_inactivos
        }
    )


@app.get("/materiales")
async def materiales(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de materiales - requiere autenticación."""
    # Cargar los materiales desde la base de datos
    materiales_data = await crud.list_materiales(db, limit=1000)
    
    # Calcular estadísticas
    total_materiales = await crud.count_materiales(db)
    
    return templates.TemplateResponse(
        "materiales.html",
        {
            "request": request,
            "active_page": "materiales",
            "current_user": current_user,
            "materiales": materiales_data,
            "total_materiales": total_materiales
        }
    )


@app.get("/precios-compra")
async def precios_compra(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de precios de compra - requiere autenticación."""
    # Cargar los precios de materiales desde la base de datos con relaciones
    precios_data = await crud.list_precios_materiales(db, limit=1000)
    
    # Calcular estadísticas
    total_precios = await crud.count_precios_materiales(db)
    
    return templates.TemplateResponse(
        "precios_compra.html",
        {
            "request": request,
            "active_page": "precios_compra",
            "current_user": current_user,
            "precios": precios_data,
            "total_precios": total_precios
        }
    )


@app.get("/paises-origen")
async def paises_origen(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de países de origen de materiales - requiere autenticación."""
    # Cargar los países de origen desde la base de datos con relaciones
    paises_data = await crud.list_paises_origen_material(db, limit=1000)
    
    # Calcular estadísticas
    total_paises = await crud.count_paises_origen_material(db)
    
    # Calcular número de partes únicos (numero_material únicos)
    from sqlalchemy import select, func, distinct
    from app.db.models import PaisOrigenMaterial
    
    result = await db.execute(
        select(func.count(distinct(PaisOrigenMaterial.numero_material)))
    )
    total_partes_unicos = result.scalar() or 0
    
    return templates.TemplateResponse(
        "paises_origen.html",
        {
            "request": request,
            "active_page": "paises_origen",
            "current_user": current_user,
            "paises": paises_data,
            "total_paises": total_paises,
            "total_partes_unicos": total_partes_unicos
        }
    )


@app.get("/api/proveedores")
async def api_proveedores(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    search: Optional[str] = None,
    estatus: Optional[bool] = None,
    pais: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """API para obtener proveedores con filtros y paginación."""
    proveedores = await crud.list_proveedores(
        db,
        estatus=estatus,
        pais=pais,
        search=search,
        limit=limit,
        offset=offset
    )
    
    total = await crud.count_proveedores(
        db,
        estatus=estatus,
        pais=pais,
        search=search
    )
    
    return JSONResponse({
        "proveedores": [
            {
                "codigo_proveedor": p.codigo_proveedor,
                "nombre": p.nombre,
                "pais": p.pais,
                "domicilio": p.domicilio,
                "poblacion": p.poblacion,
                "cp": p.cp,
                "estatus": p.estatus,
                "estatus_compras": p.estatus_compras,
                "created_at": p.created_at.isoformat() if p.created_at else None,
                "updated_at": p.updated_at.isoformat() if p.updated_at else None
            }
            for p in proveedores
        ],
        "total": total,
        "limit": limit,
        "offset": offset
    })


@app.get("/api/compras")
async def api_compras(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    search: Optional[str] = None,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    numero_proveedor: Optional[int] = None,
    numero_material: Optional[str] = None,
    purchasing_document: Optional[int] = None,
    material_document: Optional[int] = None,
    limit: int = 100,
    offset: int = 0
):
    """API para obtener compras con filtros y paginación."""
    # Convertir fechas de string a datetime si están presentes
    fecha_inicio_dt = None
    fecha_fin_dt = None
    
    if fecha_inicio:
        try:
            fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": "Formato de fecha_inicio inválido. Use formato YYYY-MM-DD"}
            )
    
    if fecha_fin:
        try:
            fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": "Formato de fecha_fin inválido. Use formato YYYY-MM-DD"}
            )
    
    compras = await crud.list_compras(
        db,
        search=search,
        fecha_inicio=fecha_inicio_dt,
        fecha_fin=fecha_fin_dt,
        numero_proveedor=numero_proveedor,
        numero_material=numero_material,
        purchasing_document=purchasing_document,
        material_document=material_document,
        limit=limit,
        offset=offset
    )
    
    total = await crud.count_compras(
        db,
        search=search,
        fecha_inicio=fecha_inicio_dt,
        fecha_fin=fecha_fin_dt,
        numero_proveedor=numero_proveedor,
        numero_material=numero_material,
        purchasing_document=purchasing_document,
        material_document=material_document
    )
    
    return JSONResponse({
        "compras": [
            {
                "id": c.id,
                "purchasing_document": c.purchasing_document,
                "item": c.item,
                "material_doc_year": c.material_doc_year,
                "material_document": c.material_document,
                "material_doc_item": c.material_doc_item,
                "movement_type": c.movement_type,
                "posting_date": c.posting_date.isoformat() if c.posting_date else None,
                "quantity": c.quantity,
                "order_unit": c.order_unit,
                "quantity_in_opun": c.quantity_in_opun,
                "order_price_unit": c.order_price_unit,
                "amount_in_lc": float(c.amount_in_lc) if c.amount_in_lc else None,
                "local_currency": c.local_currency,
                "amount": float(c.amount) if c.amount else None,
                "currency": c.currency,
                "gr_ir_clearing_value_lc": float(c.gr_ir_clearing_value_lc) if c.gr_ir_clearing_value_lc else None,
                "gr_blck_stock_oun": float(c.gr_blck_stock_oun) if c.gr_blck_stock_oun else None,
                "gr_blocked_stck_opun": float(c.gr_blocked_stck_opun) if c.gr_blocked_stck_opun else None,
                "delivery_completed": c.delivery_completed,
                "fisc_year_ref_doc": c.fisc_year_ref_doc,
                "reference_document": c.reference_document,
                "reference_doc_item": c.reference_doc_item,
                "invoice_value": float(c.invoice_value) if c.invoice_value else None,
                "numero_material": c.numero_material,
                "plant": c.plant,
                "descripcion_material": c.descripcion_material,
                "nombre_proveedor": c.nombre_proveedor,
                "numero_proveedor": c.numero_proveedor,
                "price": float(c.price) if c.price else None,
                "created_at": c.created_at.isoformat() if c.created_at else None,
                "updated_at": c.updated_at.isoformat() if c.updated_at else None
            }
            for c in compras
        ],
        "total": total,
        "limit": limit,
        "offset": offset
    })


@app.post("/api/compras/iniciar-descarga")
async def iniciar_descarga(
    request: Request,
    fecha_inicio: str = Form(...),
    fecha_fin: str = Form(...),
    carpeta_salida: str = Form(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Inicia el proceso de descarga de compras y registra la actividad."""
    try:
        # Validar y convertir fechas
        # El input type="date" devuelve formato YYYY-MM-DD
        try:
            fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
            # Agregar hora al final del día para fecha_fin para incluir todo el día
            fecha_fin_dt = fecha_fin_dt.replace(hour=23, minute=59, second=59)
        except ValueError as e:
            return JSONResponse(
                status_code=400,
                content={"error": f"Formato de fecha inválido: {str(e)}. Use formato YYYY-MM-DD"}
            )
        
        # Validar que fecha fin sea posterior a fecha inicio
        if fecha_fin_dt < fecha_inicio_dt:
            return JSONResponse(
                status_code=400,
                content={"error": "La fecha fin debe ser posterior a la fecha inicio"}
            )
        
        # Obtener información de la máquina
        try:
            hostname = socket.gethostname()
        except:
            hostname = platform.node() or "unknown"
        
        # Crear el registro de ejecución
        execution = await crud.create_execution(
            db=db,
            user_id=current_user.id,
            fecha_inicio_periodo=fecha_inicio_dt,
            fecha_fin_periodo=fecha_fin_dt,
            sistema_sap="SAP ECC",  # Valor por defecto, puede actualizarse después
            transaccion="ME23N",  # Valor por defecto, puede actualizarse después
            maquina=hostname
        )
        
        # Construir el nombre del archivo esperado
        fecha_inicio_str = fecha_inicio_dt.strftime("%Y%m%d")
        fecha_fin_str = fecha_fin_dt.strftime("%Y%m%d")
        nombre_archivo = f"compras_{fecha_inicio_str}_{fecha_fin_str}.xlsx"
        ruta_completa = f"{carpeta_salida.rstrip('/').rstrip('\\')}{os.sep}{nombre_archivo}"
        
        # Actualizar con la información del archivo esperado
        await crud.update_execution_status(
            db=db,
            execution_id=execution.id,
            estado=execution.estado,  # Mantener PENDING
            archivo_ruta=ruta_completa,
            archivo_nombre=nombre_archivo
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Proceso de descarga iniciado",
                "execution_id": execution.id,
                "archivo_esperado": nombre_archivo,
                "ruta_completa": ruta_completa
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Error al iniciar la descarga: {str(e)}"}
        )


@app.post("/api/compras/procesar-archivos")
async def procesar_archivos(
    request: Request,
    archivo_ventas: UploadFile = File(...),
    archivo_po: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Procesa dos archivos Excel: cruza información usando 'Purchasing Document' y sube los datos a la tabla compras."""
    import tempfile
    import shutil
    import os
    from pathlib import Path
    import pandas as pd
    import traceback
    import time
    
    # Variables para el registro de ejecución
    execution = None
    execution_id = None
    fecha_inicio_ejecucion = None
    
    try:
        # Obtener información de la máquina
        try:
            hostname = socket.gethostname()
        except:
            hostname = platform.node() or "unknown"
        
        # Fecha de hoy para el periodo (el procesamiento no tiene periodo específico)
        fecha_hoy = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Crear el registro de ejecución inicial
        execution = await crud.create_execution(
            db=db,
            user_id=current_user.id,
            fecha_inicio_periodo=fecha_hoy,
            fecha_fin_periodo=fecha_hoy,
            sistema_sap="Procesamiento de Archivos",
            transaccion="Procesar Archivos Excel",
            maquina=hostname
        )
        execution_id = execution.id
        fecha_inicio_ejecucion = datetime.now()
        
        # Actualizar estado a RUNNING
        await crud.update_execution_status(
            db=db,
            execution_id=execution_id,
            estado=ExecutionStatus.RUNNING,
            fecha_inicio_ejecucion=fecha_inicio_ejecucion
        )
        
        # Validar que sean archivos Excel
        extensiones_permitidas = ['.xlsx', '.xls']
        
        nombre_archivo_ventas = archivo_ventas.filename or ''
        nombre_archivo_po = archivo_po.filename or ''
        
        extension_ventas = Path(nombre_archivo_ventas).suffix.lower()
        extension_po = Path(nombre_archivo_po).suffix.lower()
        
        if extension_ventas not in extensiones_permitidas:
            error_msg = "El archivo de Reporte de ventas debe ser un archivo Excel (.xlsx o .xls)"
            if execution_id:
                await crud.update_execution_status(
                    db=db,
                    execution_id=execution_id,
                    estado=ExecutionStatus.FAILED,
                    mensaje_error=error_msg
                )
            return JSONResponse(
                status_code=400,
                content={"error": error_msg}
            )
        
        if extension_po not in extensiones_permitidas:
            error_msg = "El archivo de Purchase Order History debe ser un archivo Excel (.xlsx o .xls)"
            if execution_id:
                await crud.update_execution_status(
                    db=db,
                    execution_id=execution_id,
                    estado=ExecutionStatus.FAILED,
                    mensaje_error=error_msg
                )
            return JSONResponse(
                status_code=400,
                content={"error": error_msg}
            )
        
        # Crear directorio temporal para los archivos
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Guardar archivos temporalmente
            archivo_ventas_path = temp_path / f"ventas_{nombre_archivo_ventas}"
            archivo_po_path = temp_path / f"po_{nombre_archivo_po}"
            
            with open(archivo_ventas_path, "wb") as f:
                shutil.copyfileobj(archivo_ventas.file, f)
            
            with open(archivo_po_path, "wb") as f:
                shutil.copyfileobj(archivo_po.file, f)
            
            # Leer archivos Excel
            try:
                # Leer Archivo 1 (base/destino) - donde se escribirá Proveedor
                if extension_ventas == '.xlsx':
                    df_archivo1 = pd.read_excel(archivo_ventas_path, engine='openpyxl')
                else:
                    df_archivo1 = pd.read_excel(archivo_ventas_path, engine='xlrd')
                
                # Leer Archivo 2 (referencia) - del que se obtendrá Name 1
                if extension_po == '.xlsx':
                    df_archivo2 = pd.read_excel(archivo_po_path, engine='openpyxl')
                else:
                    df_archivo2 = pd.read_excel(archivo_po_path, engine='xlrd')
            except Exception as e:
                error_msg = f"Error al leer los archivos Excel: {str(e)}"
                if execution_id:
                    await crud.update_execution_status(
                        db=db,
                        execution_id=execution_id,
                        estado=ExecutionStatus.FAILED,
                        mensaje_error=error_msg,
                        stack_trace=traceback.format_exc()
                    )
                return JSONResponse(
                    status_code=400,
                    content={"error": error_msg}
                )
            
            # Buscar la columna "Purchasing Document" en ambos archivos (tal cual esté escrita)
            columna_purchasing_doc = None
            for col in df_archivo1.columns:
                if str(col).strip() == "Purchasing Document":
                    columna_purchasing_doc = col
                    break
            
            if columna_purchasing_doc is None:
                error_msg = "No se encontró la columna 'Purchasing Document' en el Archivo 1 (base/destino)"
                if execution_id:
                    await crud.update_execution_status(
                        db=db,
                        execution_id=execution_id,
                        estado=ExecutionStatus.FAILED,
                        mensaje_error=error_msg
                    )
                return JSONResponse(
                    status_code=400,
                    content={"error": error_msg}
                )
            
            # Verificar que también existe en el Archivo 2
            columna_purchasing_doc_archivo2 = None
            for col in df_archivo2.columns:
                if str(col).strip() == "Purchasing Document":
                    columna_purchasing_doc_archivo2 = col
                    break
            
            if columna_purchasing_doc_archivo2 is None:
                error_msg = "No se encontró la columna 'Purchasing Document' en el Archivo 2 (referencia)"
                if execution_id:
                    await crud.update_execution_status(
                        db=db,
                        execution_id=execution_id,
                        estado=ExecutionStatus.FAILED,
                        mensaje_error=error_msg
                    )
                return JSONResponse(
                    status_code=400,
                    content={"error": error_msg}
                )
            
            # Buscar la columna "Name 1" en el Archivo 2
            columna_name1 = None
            for col in df_archivo2.columns:
                if str(col).strip() == "Name 1":
                    columna_name1 = col
                    break
            
            if columna_name1 is None:
                error_msg = "No se encontró la columna 'Name 1' en el Archivo 2 (referencia)"
                if execution_id:
                    await crud.update_execution_status(
                        db=db,
                        execution_id=execution_id,
                        estado=ExecutionStatus.FAILED,
                        mensaje_error=error_msg
                    )
                return JSONResponse(
                    status_code=400,
                    content={"error": error_msg}
                )
            
            # Buscar la columna "Supplier" en el Archivo 2 para numero_proveedor
            columna_supplier = None
            for col in df_archivo2.columns:
                if str(col).strip().lower() == "supplier":
                    columna_supplier = col
                    break
            
            if columna_supplier is None:
                error_msg = "No se encontró la columna 'Supplier' en el Archivo 2 (referencia)"
                if execution_id:
                    await crud.update_execution_status(
                        db=db,
                        execution_id=execution_id,
                        estado=ExecutionStatus.FAILED,
                        mensaje_error=error_msg
                    )
                return JSONResponse(
                    status_code=400,
                    content={"error": error_msg}
                )
            
            # Buscar columna "Proveedor" original en Archivo 1 para numero_proveedor (antes del cruce)
            # Esta columna puede contener el número del proveedor
            columna_proveedor_numero = None
            for col in df_archivo1.columns:
                if str(col).strip().lower() == "proveedor":
                    columna_proveedor_numero = col
                    break
            
            # Guardar los valores originales de "Proveedor" para numero_proveedor si existe
            # Esto debe hacerse ANTES de que se modifique la columna "Proveedor" con el cruce
            if columna_proveedor_numero is not None:
                # Guardar una copia del valor original antes del cruce
                df_archivo1["Proveedor_Numero_Original"] = df_archivo1[columna_proveedor_numero].copy()
            else:
                # Si no existe "Proveedor" originalmente, crear columna vacía
                df_archivo1["Proveedor_Numero_Original"] = None
            
            # Buscar o crear columna "Proveedor" en Archivo 1 para nombre_proveedor (búsqueda case-insensitive)
            columna_proveedor = None
            for col in df_archivo1.columns:
                if str(col).strip().lower() == "proveedor":
                    columna_proveedor = col
                    break
            
            # Si no existe, crear la columna "Proveedor" para el nombre
            if columna_proveedor is None:
                df_archivo1["Proveedor"] = ""
                columna_proveedor = "Proveedor"
            
            # Crear columna para numero_proveedor (Supplier del archivo 2)
            df_archivo1["Supplier_Numero"] = None
            
            # Crear diccionarios de mapeo desde Archivo 2
            # Clave: Purchasing Document (normalizado), Valor: Name 1 (para nombre_proveedor)
            # Clave: Purchasing Document (normalizado), Valor: Supplier (para numero_proveedor)
            # Usar la primera coincidencia si hay múltiples
            mapa_proveedores = {}
            mapa_suppliers = {}
            for idx, row in df_archivo2.iterrows():
                purchasing_doc = row[columna_purchasing_doc_archivo2]
                name1 = row[columna_name1]
                supplier = row[columna_supplier]
                
                # Normalizar: convertir a string, trim, y asegurar tipo comparable
                if pd.notna(purchasing_doc):
                    purchasing_doc_normalizado = str(purchasing_doc).strip()
                    
                    # Mapeo para nombre_proveedor (Name 1)
                    if pd.notna(name1) and purchasing_doc_normalizado and purchasing_doc_normalizado not in mapa_proveedores:
                        name1_valor = str(name1).strip()
                        mapa_proveedores[purchasing_doc_normalizado] = name1_valor
                    
                    # Mapeo para numero_proveedor (Supplier)
                    if pd.notna(supplier) and purchasing_doc_normalizado and purchasing_doc_normalizado not in mapa_suppliers:
                        supplier_valor = str(supplier).strip()
                        mapa_suppliers[purchasing_doc_normalizado] = supplier_valor
            
            # Procesar cada fila del Archivo 1
            for idx, row in df_archivo1.iterrows():
                purchasing_doc_valor = row[columna_purchasing_doc]
                
                # Normalizar el valor para comparar
                if pd.notna(purchasing_doc_valor):
                    purchasing_doc_normalizado = str(purchasing_doc_valor).strip()
                    
                    # Buscar coincidencia en el mapa para nombre_proveedor (Name 1)
                    if purchasing_doc_normalizado in mapa_proveedores:
                        # Asignar el valor de Name 1 a Proveedor
                        df_archivo1.at[idx, columna_proveedor] = mapa_proveedores[purchasing_doc_normalizado]
                    else:
                        # Si no hay coincidencia, dejar vacío (o conservar si ya existe)
                        if pd.isna(df_archivo1.at[idx, columna_proveedor]) or df_archivo1.at[idx, columna_proveedor] == "":
                            df_archivo1.at[idx, columna_proveedor] = ""
                    
                    # Buscar coincidencia en el mapa para numero_proveedor (Supplier)
                    if purchasing_doc_normalizado in mapa_suppliers:
                        # Asignar el valor de Supplier a Supplier_Numero
                        df_archivo1.at[idx, "Supplier_Numero"] = mapa_suppliers[purchasing_doc_normalizado]
                    else:
                        # Si no hay coincidencia, dejar None
                        df_archivo1.at[idx, "Supplier_Numero"] = None
                else:
                    # Si el valor de Purchasing Document está vacío, dejar vacío
                    if pd.isna(df_archivo1.at[idx, columna_proveedor]) or df_archivo1.at[idx, columna_proveedor] == "":
                        df_archivo1.at[idx, columna_proveedor] = ""
                    df_archivo1.at[idx, "Supplier_Numero"] = None
            
            # Crear columna "U/P" = Invoice Value / Quantity in OPUn
            # Buscar columnas de manera case-insensitive
            columna_invoice_value = None
            columna_quantity_opun = None
            
            for col in df_archivo1.columns:
                col_normalizado = str(col).strip().lower()
                if col_normalizado == "invoice value":
                    columna_invoice_value = col
                elif col_normalizado == "quantity in opun":
                    columna_quantity_opun = col
            
            # Verificar que ambas columnas existan
            if columna_invoice_value is None:
                error_msg = "No se encontró la columna 'Invoice Value' en el Archivo 1 (base/destino)"
                if execution_id:
                    await crud.update_execution_status(
                        db=db,
                        execution_id=execution_id,
                        estado=ExecutionStatus.FAILED,
                        mensaje_error=error_msg
                    )
                return JSONResponse(
                    status_code=400,
                    content={"error": error_msg}
                )
            
            if columna_quantity_opun is None:
                error_msg = "No se encontró la columna 'Quantity in OPUn' en el Archivo 1 (base/destino)"
                if execution_id:
                    await crud.update_execution_status(
                        db=db,
                        execution_id=execution_id,
                        estado=ExecutionStatus.FAILED,
                        mensaje_error=error_msg
                    )
                return JSONResponse(
                    status_code=400,
                    content={"error": error_msg}
                )
            
            # Crear la columna "U/P" y calcular el valor
            df_archivo1["U/P"] = ""
            
            for idx, row in df_archivo1.iterrows():
                invoice_value = row[columna_invoice_value]
                quantity_opun = row[columna_quantity_opun]
                
                # Convertir a numérico si es posible
                try:
                    invoice_value_num = pd.to_numeric(invoice_value, errors='coerce')
                    quantity_opun_num = pd.to_numeric(quantity_opun, errors='coerce')
                    
                    # Realizar la división solo si ambos valores son válidos y quantity no es cero
                    if pd.notna(invoice_value_num) and pd.notna(quantity_opun_num) and quantity_opun_num != 0:
                        df_archivo1.at[idx, "U/P"] = invoice_value_num / quantity_opun_num
                    else:
                        # Si hay valores nulos o quantity es cero, dejar vacío
                        df_archivo1.at[idx, "U/P"] = ""
                except (ValueError, TypeError, ZeroDivisionError):
                    # Si hay error en la conversión o división, dejar vacío
                    df_archivo1.at[idx, "U/P"] = ""
            
            # Mapear columnas del DataFrame a la tabla compras
            # Función auxiliar para convertir valores
            def convertir_valor(valor, tipo='int'):
                """Convierte un valor de pandas a tipo Python apropiado."""
                if pd.isna(valor) or valor == '' or valor is None:
                    return None
                try:
                    if tipo == 'int':
                        # Para valores grandes, Python maneja automáticamente enteros de cualquier tamaño
                        # Convertir directamente sin pasar por float para evitar pérdida de precisión
                        if isinstance(valor, int):
                            return valor
                        elif isinstance(valor, float):
                            # Si es float, convertir a int (puede manejar valores grandes)
                            return int(valor)
                        else:
                            # Para strings u otros tipos, convertir directamente
                            # Usar pd.to_numeric para manejar mejor los valores grandes
                            valor_numerico = pd.to_numeric(valor, errors='coerce')
                            if pd.notna(valor_numerico):
                                return int(valor_numerico)
                            return None
                    elif tipo == 'float':
                        return float(valor) if pd.notna(valor) else None
                    elif tipo == 'date':
                        if isinstance(valor, pd.Timestamp):
                            return valor.to_pydatetime()
                        elif isinstance(valor, datetime):
                            return valor
                        elif isinstance(valor, str):
                            return pd.to_datetime(valor, errors='coerce').to_pydatetime() if pd.notna(pd.to_datetime(valor, errors='coerce')) else None
                        return None
                    else:
                        return str(valor).strip() if pd.notna(valor) else None
                except (ValueError, TypeError, OverflowError):
                    return None
            
            # Función para mapear una fila del DataFrame a un diccionario para Compra
            def mapear_fila_a_compra(row, df_columns):
                """Mapea una fila del DataFrame a un diccionario para insertar en Compra."""
                compra_data = {}
                
                # Mapeo de columnas (case-insensitive)
                columnas_mapeo = {
                    'purchasing_document': ['Purchasing Document'],
                    'item': ['Item'],
                    'material_doc_year': ['Material Doc. Year'],
                    'material_document': ['Material Document'],
                    'material_doc_item': ['Material Doc.Item'],
                    'movement_type': ['Movement Type'],
                    'posting_date': ['Posting Date'],
                    'quantity': ['Quantity'],
                    'order_unit': ['Order Unit'],
                    'quantity_in_opun': ['Quantity in OPUn'],
                    'order_price_unit': ['Order Price Unit'],
                    'amount_in_lc': ['Amount in LC'],
                    'local_currency': ['Local currency'],
                    'amount': ['Amount'],
                    'currency': ['Currency'],
                    'gr_ir_clearing_value_lc': ['GR/IR clearing value in local currency'],
                    'gr_blck_stock_oun': ['GR Blck.Stock in OUn'],
                    'gr_blocked_stck_opun': ['GR blocked stck.OPUn'],
                    'delivery_completed': ['Delivery Completed'],
                    'fisc_year_ref_doc': ['Fisc. Year Ref. Doc.'],
                    'reference_document': ['Reference Document'],
                    'reference_doc_item': ['Reference Doc. Item'],
                    'invoice_value': ['Invoice Value'],
                    'numero_material': ['numero_Material', 'Material'],
                    'plant': ['Plant'],
                    'descripcion_material': ['Short Text', 'descripcion_material', 'Material Description', 'Short text'],
                    'nombre_proveedor': ['Supplier', 'supplier', 'SUPPLIER', 'Proveedor', 'proveedor', 'PROVEEDOR', 'nombre_proveedor'],
                    'numero_proveedor': ['Supplier_Numero', 'Proveedor_Numero_Original', 'Proveedor', 'proveedor', 'PROVEEDOR', 'numero_proveedor'],
                    'price': ['U/P', 'u/p', 'U/p', 'u/P', 'precio', 'Precio', 'PRECIO', 'Price', 'price', 'PRICE'],
                }
                
                # Buscar y mapear cada columna
                for campo_db, nombres_posibles in columnas_mapeo.items():
                    valor = None
                    for nombre_col in nombres_posibles:
                        # Buscar columna case-insensitive
                        for col in df_columns:
                            if str(col).strip().lower() == str(nombre_col).strip().lower():
                                valor = row[col]
                                break
                        if valor is not None and not pd.isna(valor):
                            break
                    
                    # Convertir según el tipo de campo
                    if campo_db in ['purchasing_document', 'item', 'material_doc_year', 'material_document', 
                                    'material_doc_item', 'quantity', 'quantity_in_opun']:
                        compra_data[campo_db] = convertir_valor(valor, 'int')
                    elif campo_db == 'numero_proveedor':
                        # numero_proveedor puede ser texto o número, intentar convertir a int primero
                        # si falla, intentar extraer solo números del string
                        if valor is not None and not pd.isna(valor):
                            # Intentar convertir directamente
                            valor_int = convertir_valor(valor, 'int')
                            if valor_int is not None:
                                compra_data[campo_db] = valor_int
                            else:
                                # Si falla, intentar extraer números del string
                                import re
                                valor_str = str(valor).strip()
                                numeros = re.findall(r'\d+', valor_str)
                                if numeros:
                                    try:
                                        compra_data[campo_db] = int(''.join(numeros))
                                    except (ValueError, TypeError):
                                        compra_data[campo_db] = None
                                else:
                                    compra_data[campo_db] = None
                        else:
                            compra_data[campo_db] = None
                    elif campo_db == 'posting_date':
                        compra_data[campo_db] = convertir_valor(valor, 'date')
                    elif campo_db in ['amount_in_lc', 'amount', 'gr_ir_clearing_value_lc', 'gr_blck_stock_oun', 
                                     'gr_blocked_stck_opun', 'invoice_value', 'price']:
                        compra_data[campo_db] = convertir_valor(valor, 'float')
                    else:
                        # numero_material y otros campos string
                        compra_data[campo_db] = convertir_valor(valor, 'str')
                
                return compra_data
            
            # Preparar datos para inserción en la base de datos
            compras_data = []
            for idx, row in df_archivo1.iterrows():
                compra_data = mapear_fila_a_compra(row, df_archivo1.columns)
                compras_data.append(compra_data)
            
            # Insertar o actualizar datos en la base de datos
            try:
                resultado = await crud.bulk_create_or_update_compras(db, compras_data)
                registros_insertados = resultado["insertados"]
                registros_actualizados = resultado["actualizados"]
            except Exception as e:
                error_msg = f"Error al insertar/actualizar datos en la base de datos: {str(e)}"
                if execution_id:
                    await crud.update_execution_status(
                        db=db,
                        execution_id=execution_id,
                        estado=ExecutionStatus.FAILED,
                        mensaje_error=error_msg,
                        stack_trace=traceback.format_exc()
                    )
                return JSONResponse(
                    status_code=500,
                    content={"error": error_msg}
                )
            
            # Calcular duración
            fecha_fin_ejecucion = datetime.now()
            duracion_segundos = int((fecha_fin_ejecucion - fecha_inicio_ejecucion).total_seconds())
            
            # Actualizar ejecución como exitosa
            if execution_id:
                await crud.update_execution_status(
                    db=db,
                    execution_id=execution_id,
                    estado=ExecutionStatus.SUCCESS,
                    fecha_fin_ejecucion=fecha_fin_ejecucion,
                    duracion_segundos=duracion_segundos
                )
            
            # Construir mensaje
            mensaje = f"Datos procesados exitosamente."
            if registros_insertados > 0:
                mensaje += f" Se insertaron {registros_insertados} registros nuevos."
            if registros_actualizados > 0:
                mensaje += f" Se actualizaron {registros_actualizados} registros existentes."
            
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": mensaje,
                    "registros_insertados": registros_insertados,
                    "registros_actualizados": registros_actualizados,
                    "archivos_recibidos": {
                        "archivo_base": nombre_archivo_ventas,
                        "archivo_referencia": nombre_archivo_po
                    },
                    "execution_id": execution_id
                }
            )
        
    except Exception as e:
        # Manejar cualquier error no capturado
        error_msg = f"Error al procesar los archivos: {str(e)}"
        stack_trace_str = traceback.format_exc()
        
        if execution_id:
            fecha_fin_ejecucion = datetime.now()
            duracion_segundos = int((fecha_fin_ejecucion - fecha_inicio_ejecucion).total_seconds()) if fecha_inicio_ejecucion else None
            
            await crud.update_execution_status(
                db=db,
                execution_id=execution_id,
                estado=ExecutionStatus.FAILED,
                fecha_fin_ejecucion=fecha_fin_ejecucion,
                duracion_segundos=duracion_segundos,
                mensaje_error=error_msg,
                stack_trace=stack_trace_str
            )
        
        return JSONResponse(
            status_code=500,
            content={"error": error_msg}
        )


@app.post("/api/ventas/iniciar-descarga")
async def iniciar_descarga_ventas(
    request: Request,
    fecha_inicio: str = Form(...),
    fecha_fin: str = Form(...),
    carpeta_salida: str = Form(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Inicia el proceso de descarga de ventas y registra la actividad."""
    try:
        # Validar y convertir fechas
        # El input type="date" devuelve formato YYYY-MM-DD
        try:
            fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
            # Agregar hora al final del día para fecha_fin para incluir todo el día
            fecha_fin_dt = fecha_fin_dt.replace(hour=23, minute=59, second=59)
        except ValueError as e:
            return JSONResponse(
                status_code=400,
                content={"error": f"Formato de fecha inválido: {str(e)}. Use formato YYYY-MM-DD"}
            )
        
        # Validar que fecha fin sea posterior a fecha inicio
        if fecha_fin_dt < fecha_inicio_dt:
            return JSONResponse(
                status_code=400,
                content={"error": "La fecha fin debe ser posterior a la fecha inicio"}
            )
        
        # Obtener información de la máquina
        try:
            hostname = socket.gethostname()
        except:
            hostname = platform.node() or "unknown"
        
        # Crear el registro de ejecución
        execution = await crud.create_sales_execution(
            db=db,
            user_id=current_user.id,
            fecha_inicio_periodo=fecha_inicio_dt,
            fecha_fin_periodo=fecha_fin_dt,
            sistema_sap="SAP ECC",  # Valor por defecto, puede actualizarse después
            transaccion="ME23N",  # Valor por defecto, puede actualizarse después
            maquina=hostname
        )
        
        # Construir el nombre del archivo esperado
        fecha_inicio_str = fecha_inicio_dt.strftime("%Y%m%d")
        fecha_fin_str = fecha_fin_dt.strftime("%Y%m%d")
        nombre_archivo = f"ventas_{fecha_inicio_str}_{fecha_fin_str}.xlsx"
        ruta_completa = f"{carpeta_salida.rstrip('/').rstrip('\\')}{os.sep}{nombre_archivo}"
        
        # Actualizar con la información del archivo esperado
        await crud.update_sales_execution_status(
            db=db,
            execution_id=execution.id,
            estado=execution.estado,  # Mantener PENDING
            archivo_ruta=ruta_completa,
            archivo_nombre=nombre_archivo
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Proceso de descarga iniciado",
                "execution_id": execution.id,
                "archivo_esperado": nombre_archivo,
                "ruta_completa": ruta_completa
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Error al iniciar la descarga: {str(e)}"}
        )


def _procesar_df_historial(df: "pd.DataFrame") -> "pd.DataFrame":
    """Aplica transformaciones al historial: eliminar columnas, filtros, precio unitario."""
    import pandas as pd

    columnas_a_eliminar = [
        'GR Blck.Stock in OUn', 'GR blocked stck.OPUn', 'Delivery Completed',
        'Fisc. Year Ref. Doc.', 'Reference Document', 'Reference Doc. Item'
    ]
    columnas_encontradas = []
    for col in df.columns:
        col_str = str(col).strip()
        for col_eliminar in columnas_a_eliminar:
            if col_str == col_eliminar or col_str.lower() == col_eliminar.lower():
                columnas_encontradas.append(col)
                break
    if columnas_encontradas:
        df = df.drop(columns=columnas_encontradas)

    if 'nombre proveedor' not in df.columns:
        df['nombre proveedor'] = ''
    if 'codigo proveedor' not in df.columns:
        df['codigo proveedor'] = ''

    col_plant = next((c for c in df.columns if str(c).strip().lower() == 'plant'), None)
    if col_plant is not None:
        df[col_plant] = df[col_plant].astype(str).str.strip()
        df = df[(~df[col_plant].isin(['MX10', 'MX11'])) & (df[col_plant] != '') & (df[col_plant] != 'nan') & (df[col_plant].notna())]

    col_material = next((c for c in df.columns if str(c).strip().lower() == 'material'), None)
    if col_material is not None:
        df[col_material] = df[col_material].astype(str).str.strip()
        df = df[(df[col_material] != '') & (df[col_material] != 'nan') & (df[col_material].notna())]

    col_inv = next((c for c in df.columns if str(c).strip().lower() == 'invoice value'), None)
    if col_inv is not None:
        vals = df[col_inv].astype(str).str.strip()
        df = df[~vals.isin(['0', '0.0', '0.00'])]

    col_inv = next((c for c in df.columns if str(c).strip().lower() == 'invoice value'), None)
    col_qty = next((c for c in df.columns if str(c).strip().lower() == 'quantity in opun'), None)
    if col_inv is not None and col_qty is not None:
        inv_n = pd.to_numeric(df[col_inv], errors='coerce')
        qty_n = pd.to_numeric(df[col_qty], errors='coerce')
        df['precio unitario'] = inv_n / qty_n
        df.loc[(qty_n <= 0) | qty_n.isna(), 'precio unitario'] = pd.NA
        df['precio unitario'] = df['precio unitario'].replace([float('inf'), float('-inf')], pd.NA)
    else:
        df['precio unitario'] = pd.NA

    return df


@app.post("/api/compras/procesar-compras-historial")
async def procesar_compras_historial(
    request: Request,
    archivo_compras: UploadFile = File(...),
    archivo_historial: UploadFile = File(...),
    carpeta_salida: str = Form(...),
    current_user: User = Depends(get_current_user),
):
    """Procesa historial de compras, enriquece con Compras (Supplier->codigo proveedor, Name 1->nombre proveedor) y guarda archivo final."""
    import tempfile
    import shutil
    from pathlib import Path
    import pandas as pd

    ext_ok = ['.xlsx', '.xls']
    n_compras = archivo_compras.filename or ''
    n_hist = archivo_historial.filename or ''
    e_compras = Path(n_compras).suffix.lower()
    e_hist = Path(n_hist).suffix.lower()

    if e_compras not in ext_ok or e_hist not in ext_ok:
        return JSONResponse(status_code=400, content={"error": "Ambos archivos deben ser Excel (.xlsx o .xls)"})

    carpeta = Path(carpeta_salida.strip().rstrip('/').rstrip('\\'))
    if not carpeta.exists() or not carpeta.is_dir():
        return JSONResponse(status_code=400, content={"error": f"La carpeta de salida no existe o no es válida: {carpeta_salida}"})

    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            path_compras = tmp / f"compras_{n_compras}"
            path_hist = tmp / f"hist_{n_hist}"

            with open(path_compras, "wb") as f:
                shutil.copyfileobj(archivo_compras.file, f)
            with open(path_hist, "wb") as f:
                shutil.copyfileobj(archivo_historial.file, f)

            df_compras = pd.read_excel(path_compras, engine='openpyxl' if e_compras == '.xlsx' else 'xlrd')
            df = pd.read_excel(path_hist, engine='openpyxl' if e_hist == '.xlsx' else 'xlrd')

            def col(df, name):
                name = name.strip().lower()
                for c in df.columns:
                    if str(c).strip().lower() == name:
                        return c
                return None

            pc = col(df_compras, 'Purchasing Document')
            sc = col(df_compras, 'Supplier')
            n1 = col(df_compras, 'Name 1')
            if not all([pc, sc, n1]):
                return JSONResponse(
                    status_code=400,
                    content={"error": "El archivo Compras debe tener columnas: Purchasing Document, Supplier, Name 1"}
                )

            def to_int_codigo(v):
                if pd.isna(v) or v == '' or v is None:
                    return pd.NA
                try:
                    return int(pd.to_numeric(v, errors='raise'))
                except (ValueError, TypeError):
                    return pd.NA

            mapa = {}
            for _, row in df_compras.iterrows():
                k = str(row[pc]).strip() if pd.notna(row[pc]) else None
                if not k or k in mapa:
                    continue
                mapa[k] = (
                    to_int_codigo(row[sc]),
                    str(row[n1]).strip() if pd.notna(row[n1]) else ''
                )

            df = _procesar_df_historial(df)

            col_pd = col(df, 'Purchasing Document')
            if col_pd is None:
                return JSONResponse(
                    status_code=400,
                    content={"error": "El archivo Historial debe tener la columna Purchasing Document"}
                )

            def lookup(key):
                k = str(key).strip() if pd.notna(key) else None
                return mapa.get(k, (pd.NA, ''))

            codigos = df[col_pd].map(lambda x: lookup(x)[0])
            df['codigo proveedor'] = codigos.astype('Int64')
            df['nombre proveedor'] = df[col_pd].map(lambda x: lookup(x)[1])

            out = carpeta / "historial_compras_procesado.xlsx"
            df.to_excel(out, index=False, engine='openpyxl')

        return JSONResponse(
            status_code=200,
            content={"success": True, "message": "Archivo procesado correctamente", "archivo_guardado": str(out)}
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/compras/procesar-historial")
async def procesar_historial_compras(
    request: Request,
    archivo_po: UploadFile = File(...),
    carpeta_salida: str = Form(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Procesa el archivo de historial de compras: elimina columnas específicas y guarda el archivo procesado."""
    import tempfile
    import shutil
    import os
    from pathlib import Path
    import pandas as pd
    import traceback
    
    try:
        # Validar que sea un archivo Excel
        extensiones_permitidas = ['.xlsx', '.xls']
        nombre_archivo_po = archivo_po.filename or ''
        extension_po = Path(nombre_archivo_po).suffix.lower()
        
        if extension_po not in extensiones_permitidas:
            return JSONResponse(
                status_code=400,
                content={"error": "El archivo debe ser un archivo Excel (.xlsx o .xls)"}
            )
        
        # Validar carpeta de salida
        carpeta_salida_path = Path(carpeta_salida)
        if not carpeta_salida_path.exists():
            return JSONResponse(
                status_code=400,
                content={"error": f"La carpeta de salida no existe: {carpeta_salida}"}
            )
        
        if not carpeta_salida_path.is_dir():
            return JSONResponse(
                status_code=400,
                content={"error": f"La ruta especificada no es una carpeta: {carpeta_salida}"}
            )
        
        # Crear directorio temporal para el archivo
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Guardar archivo temporalmente
            archivo_po_path = temp_path / f"po_{nombre_archivo_po}"
            
            with open(archivo_po_path, "wb") as f:
                shutil.copyfileobj(archivo_po.file, f)
            
            # Leer archivo Excel
            try:
                if extension_po == '.xlsx':
                    df = pd.read_excel(archivo_po_path, engine='openpyxl')
                else:
                    df = pd.read_excel(archivo_po_path, engine='xlrd')
            except Exception as e:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"Error al leer el archivo Excel: {str(e)}"}
                )
            
            # Columnas a eliminar (con posibles variaciones)
            columnas_a_eliminar = [
                'GR Blck.Stock in OUn',
                'GR blocked stck.OPUn',
                'Delivery Completed',
                'Fisc. Year Ref. Doc.',
                'Reference Document',
                'Reference Doc. Item'
            ]
            
            # Buscar y eliminar columnas (comparación exacta y case-insensitive)
            columnas_encontradas = []
            for col in df.columns:
                col_str = str(col).strip()
                for col_eliminar in columnas_a_eliminar:
                    # Comparación exacta
                    if col_str == col_eliminar:
                        columnas_encontradas.append(col)
                        break
                    # Comparación case-insensitive
                    elif col_str.lower() == col_eliminar.lower():
                        columnas_encontradas.append(col)
                        break
            
            # Eliminar las columnas encontradas
            if columnas_encontradas:
                df = df.drop(columns=columnas_encontradas)
            
            # Agregar nuevas columnas: nombre proveedor y codigo proveedor
            # Verificar si las columnas ya existen para no duplicarlas
            if 'nombre proveedor' not in df.columns:
                df['nombre proveedor'] = ''
            if 'codigo proveedor' not in df.columns:
                df['codigo proveedor'] = ''
            
            # Eliminar filas donde la columna "Plant" tenga "MX10", "MX11" o esté vacía
            # Buscar la columna "Plant" (case-insensitive)
            columna_plant = None
            for col in df.columns:
                if str(col).strip().lower() == 'plant':
                    columna_plant = col
                    break
            
            if columna_plant is not None:
                # Contar filas antes de eliminar
                filas_antes = len(df)
                # Convertir a string y limpiar espacios
                df[columna_plant] = df[columna_plant].astype(str).str.strip()
                # Eliminar filas donde Plant sea "MX10", "MX11" o esté vacía (nan, None, o string vacío)
                df = df[
                    (~df[columna_plant].isin(['MX10', 'MX11'])) & 
                    (df[columna_plant] != '') & 
                    (df[columna_plant] != 'nan') & 
                    (df[columna_plant].notna())
                ]
                filas_eliminadas = filas_antes - len(df)
            else:
                filas_eliminadas = 0
            
            # Eliminar filas donde la columna "Material" esté vacía
            # Buscar la columna "Material" (case-insensitive)
            columna_material = None
            for col in df.columns:
                if str(col).strip().lower() == 'material':
                    columna_material = col
                    break
            
            if columna_material is not None:
                # Contar filas antes de eliminar
                filas_antes_material = len(df)
                # Convertir a string y limpiar espacios
                df[columna_material] = df[columna_material].astype(str).str.strip()
                # Eliminar filas donde Material esté vacía (nan, None, o string vacío)
                df = df[
                    (df[columna_material] != '') & 
                    (df[columna_material] != 'nan') & 
                    (df[columna_material].notna())
                ]
                filas_eliminadas_material = filas_antes_material - len(df)
            else:
                filas_eliminadas_material = 0
            
            # Eliminar filas donde la columna "Invoice Value" sea "0", "0.0" o "0.00"
            # Buscar la columna "Invoice Value" (case-insensitive)
            columna_invoice = None
            for col in df.columns:
                if str(col).strip().lower() == 'invoice value':
                    columna_invoice = col
                    break
            
            if columna_invoice is not None:
                # Convertir a string y limpiar espacios para comparación
                valores_invoice = df[columna_invoice].astype(str).str.strip()
                # Eliminar filas donde Invoice Value sea "0", "0.0" o "0.00"
                df = df[~valores_invoice.isin(['0', '0.0', '0.00'])]
            
            # Crear columna "precio unitario" = Invoice Value / Quantity in OPUn
            col_invoice = None
            col_quantity = None
            for col in df.columns:
                c = str(col).strip().lower()
                if c == 'invoice value':
                    col_invoice = col
                elif c == 'quantity in opun':
                    col_quantity = col
            
            if col_invoice is not None and col_quantity is not None:
                invoice_num = pd.to_numeric(df[col_invoice], errors='coerce')
                quantity_num = pd.to_numeric(df[col_quantity], errors='coerce')
                df['precio unitario'] = invoice_num / quantity_num
                # Evitar división por cero: donde quantity sea 0 o NaN, precio unitario = NaN
                df.loc[(quantity_num <= 0) | quantity_num.isna(), 'precio unitario'] = pd.NA
                df['precio unitario'] = df['precio unitario'].replace([float('inf'), float('-inf')], pd.NA)
            else:
                df['precio unitario'] = pd.NA
            
            # Guardar el archivo procesado
            nombre_archivo_salida = "historial_compras_procesado.xlsx"
            ruta_archivo_salida = carpeta_salida_path / nombre_archivo_salida
            
            # Guardar como Excel
            try:
                df.to_excel(ruta_archivo_salida, index=False, engine='openpyxl')
            except Exception as e:
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Error al guardar el archivo procesado: {str(e)}"}
                )
            
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": f"Archivo procesado exitosamente. Se eliminaron {len(columnas_encontradas)} columnas.",
                    "archivo_guardado": str(ruta_archivo_salida),
                    "columnas_eliminadas": columnas_encontradas,
                    "total_columnas_eliminadas": len(columnas_encontradas)
                }
            )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Error al procesar el historial: {str(e)}"}
        )


@app.post("/api/ventas/procesar-archivos")
async def procesar_archivos_ventas(
    request: Request,
    archivo_ventas: UploadFile = File(...),
    archivo_po: UploadFile = File(...),
    carpeta_salida: str = Form(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Procesa dos archivos Excel (Reporte de ventas y Purchase Order History) y genera un archivo final."""
    import tempfile
    import shutil
    import os
    from pathlib import Path
    
    try:
        # Validar que sean archivos Excel
        extensiones_permitidas = ['.xlsx', '.xls']
        
        nombre_archivo_ventas = archivo_ventas.filename or ''
        nombre_archivo_po = archivo_po.filename or ''
        
        extension_ventas = Path(nombre_archivo_ventas).suffix.lower()
        extension_po = Path(nombre_archivo_po).suffix.lower()
        
        if extension_ventas not in extensiones_permitidas:
            return JSONResponse(
                status_code=400,
                content={"error": "El archivo de Reporte de ventas debe ser un archivo Excel (.xlsx o .xls)"}
            )
        
        if extension_po not in extensiones_permitidas:
            return JSONResponse(
                status_code=400,
                content={"error": "El archivo de Purchase Order History debe ser un archivo Excel (.xlsx o .xls)"}
            )
        
        # Validar carpeta de salida
        if not carpeta_salida or carpeta_salida.strip() == '':
            return JSONResponse(
                status_code=400,
                content={"error": "La carpeta de salida es requerida"}
            )
        
        # Asegurar que la carpeta termine con el separador correcto
        carpeta_salida_path = Path(carpeta_salida)
        if not carpeta_salida_path.exists():
            return JSONResponse(
                status_code=400,
                content={"error": f"La carpeta de salida no existe: {carpeta_salida}"}
            )
        
        if not carpeta_salida_path.is_dir():
            return JSONResponse(
                status_code=400,
                content={"error": f"La ruta especificada no es una carpeta: {carpeta_salida}"}
            )
        
        # Crear directorio temporal para los archivos
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Guardar archivos temporalmente
            archivo_ventas_path = temp_path / f"ventas_{nombre_archivo_ventas}"
            archivo_po_path = temp_path / f"po_{nombre_archivo_po}"
            
            with open(archivo_ventas_path, "wb") as f:
                shutil.copyfileobj(archivo_ventas.file, f)
            
            with open(archivo_po_path, "wb") as f:
                shutil.copyfileobj(archivo_po.file, f)
            
            # Aquí se procesarían los archivos Excel y se generaría el archivo final
            # Por ahora, vamos a crear una estructura básica
            # TODO: Implementar la lógica de procesamiento específica
            
            # Crear un archivo de resultado (por ahora solo como ejemplo)
            # En producción, aquí se procesarían los Excel y se generaría el archivo final
            archivo_resultado_path = temp_path / "archivo_procesado.xlsx"
            
            # Procesar los archivos (implementar lógica específica aquí)
            # Por ahora, solo devolvemos un mensaje de éxito
            # Cuando se implemente el procesamiento real, aquí se generaría el archivo final
            # y se guardaría en carpeta_salida_path
            
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": f"Los archivos se recibieron correctamente. El procesamiento se implementará próximamente. Carpeta de salida: {carpeta_salida}",
                    "archivos_recibidos": {
                        "ventas": nombre_archivo_ventas,
                        "purchase_order_history": nombre_archivo_po
                    },
                    "carpeta_salida": str(carpeta_salida_path)
                }
            )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Error al procesar los archivos: {str(e)}"}
        )


@app.get("/api/select-folder")
async def select_folder(request: Request, current_user: User = Depends(get_current_user)):
    """Abre un diálogo nativo para seleccionar una carpeta."""
    import os
    import platform
    import subprocess
    
    try:
        system = platform.system()
        folder_path = None
        
        if system == "Darwin":  # macOS
            # Usar AppleScript para abrir el diálogo nativo de macOS
            script = '''
            tell application "System Events"
                activate
                set folderPath to choose folder with prompt "Seleccionar carpeta de salida"
                return POSIX path of folderPath
            end tell
            '''
            try:
                result = subprocess.run(
                    ["osascript", "-e", script],
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                if result.returncode == 0:
                    folder_path = result.stdout.strip()
                    # Convertir ruta POSIX a formato estándar
                    if folder_path and not folder_path.endswith(os.sep):
                        folder_path += os.sep
            except subprocess.TimeoutExpired:
                return JSONResponse(
                    status_code=400,
                    content={"error": "Tiempo de espera agotado"}
                )
            except Exception as e:
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Error al abrir el diálogo: {str(e)}"}
                )
        
        elif system == "Windows":
            # Usar tkinter para Windows (puede ejecutarse en hilo)
            try:
                import tkinter as tk
                from tkinter import filedialog
                
                selected_folder = [None]
                
                def open_dialog():
                    try:
                        root = tk.Tk()
                        root.withdraw()
                        root.attributes('-topmost', True)
                        folder = filedialog.askdirectory(
                            title="Seleccionar carpeta de salida",
                            mustexist=True
                        )
                        selected_folder[0] = folder
                        root.destroy()
                    except Exception as e:
                        selected_folder[0] = f"Error: {str(e)}"
                
                thread = threading.Thread(target=open_dialog)
                thread.daemon = True
                thread.start()
                thread.join(timeout=60)
                
                if selected_folder[0] and not selected_folder[0].startswith("Error"):
                    folder_path = selected_folder[0]
                elif selected_folder[0] and selected_folder[0].startswith("Error"):
                    return JSONResponse(
                        status_code=500,
                        content={"error": selected_folder[0]}
                    )
            except ImportError:
                return JSONResponse(
                    status_code=500,
                    content={"error": "tkinter no está disponible"}
                )
        
        else:  # Linux y otros
            # Usar tkinter para Linux
            try:
                import tkinter as tk
                from tkinter import filedialog
                
                selected_folder = [None]
                
                def open_dialog():
                    try:
                        root = tk.Tk()
                        root.withdraw()
                        folder = filedialog.askdirectory(
                            title="Seleccionar carpeta de salida",
                            mustexist=True
                        )
                        selected_folder[0] = folder
                        root.destroy()
                    except Exception as e:
                        selected_folder[0] = f"Error: {str(e)}"
                
                thread = threading.Thread(target=open_dialog)
                thread.daemon = True
                thread.start()
                thread.join(timeout=60)
                
                if selected_folder[0] and not selected_folder[0].startswith("Error"):
                    folder_path = selected_folder[0]
                elif selected_folder[0] and selected_folder[0].startswith("Error"):
                    return JSONResponse(
                        status_code=500,
                        content={"error": selected_folder[0]}
                    )
            except ImportError:
                return JSONResponse(
                    status_code=500,
                    content={"error": "tkinter no está disponible"}
                )
        
        if folder_path:
            # Asegurar que termine con el separador correcto
            if not folder_path.endswith(os.sep):
                folder_path += os.sep
            
            return JSONResponse(
                content={"folder_path": folder_path}
            )
        else:
            return JSONResponse(
                status_code=400,
                content={"error": "No se seleccionó ninguna carpeta"}
            )
            
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Error inesperado: {str(e)}"}
        )


@app.get("/admin")
async def admin(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Panel de administración - requiere rol admin."""
    if current_user.rol != "admin":
        return RedirectResponse(url="/dashboard", status_code=302)
    
    from sqlalchemy import select
    from app.db.models import User
    
    # Obtener todos los usuarios
    result = await db.execute(select(User).order_by(User.created_at.desc()))
    users = result.scalars().all()
    
    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "current_user": current_user,
            "users": users,
            "active_page": "admin"
        }
    )


@app.post("/admin/users/create")
async def admin_create_user(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    nombre: Optional[str] = Form(None),
    rol: str = Form("operador"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Crear nuevo usuario - solo admin."""
    if current_user.rol != "admin":
        return RedirectResponse(url="/dashboard", status_code=302)
    
    from app.db import crud
    
    # Validaciones
    if len(password) < 8:
        return RedirectResponse(
            url=f"/admin?error=El password debe tener al menos 8 caracteres",
            status_code=302
        )
    
    if len(password.encode('utf-8')) > 72:
        return RedirectResponse(
            url=f"/admin?error=El password es demasiado largo (máximo 72 bytes)",
            status_code=302
        )
    
    # Validar rol
    if rol not in ["admin", "operador", "auditor"]:
        rol = "operador"
    
    # Verificar si el email ya existe
    existing_user = await crud.get_user_by_email(db, email)
    if existing_user:
        return RedirectResponse(
            url=f"/admin?error=El email ya está registrado",
            status_code=302
        )
    
    try:
        new_user = await crud.create_user(
            db=db,
            email=email,
            password=password,
            nombre=nombre,
            rol=rol
        )
        return RedirectResponse(url="/admin?success=Usuario creado exitosamente", status_code=302)
    except Exception as e:
        return RedirectResponse(
            url=f"/admin?error=Error al crear usuario: {str(e)}",
            status_code=302
        )


@app.post("/admin/users/{user_id}/update-role")
async def admin_update_user_role(
    user_id: int,
    request: Request,
    rol: str = Form(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Actualizar rol de usuario - solo admin."""
    if current_user.rol != "admin":
        return RedirectResponse(url="/dashboard", status_code=302)
    
    from app.db import crud
    
    # Validar rol
    if rol not in ["admin", "operador", "auditor"]:
        return RedirectResponse(url="/admin?error=Rol inválido", status_code=302)
    
    try:
        await crud.update_user_role(db, user_id, rol)
        return RedirectResponse(url="/admin?success=Rol actualizado exitosamente", status_code=302)
    except Exception as e:
        return RedirectResponse(
            url=f"/admin?error=Error al actualizar rol: {str(e)}",
            status_code=302
        )


@app.post("/admin/users/{user_id}/update")
async def admin_update_user(
    user_id: int,
    request: Request,
    email: str = Form(...),
    nombre: Optional[str] = Form(None),
    rol: str = Form("operador"),
    password: Optional[str] = Form(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Actualizar usuario - solo admin."""
    if current_user.rol != "admin":
        return RedirectResponse(url="/dashboard", status_code=302)
    
    from app.db import crud
    
    # Validar rol
    if rol not in ["admin", "operador", "auditor"]:
        return RedirectResponse(url="/admin?error=Rol inválido", status_code=302)
    
    # Validar contraseña si se proporciona
    if password:
        if len(password) < 8:
            return RedirectResponse(
                url=f"/admin?error=El password debe tener al menos 8 caracteres",
                status_code=302
            )
        
        if len(password.encode('utf-8')) > 72:
            return RedirectResponse(
                url=f"/admin?error=El password es demasiado largo (máximo 72 bytes)",
                status_code=302
            )
    
    try:
        updated_user = await crud.update_user(
            db=db,
            user_id=user_id,
            email=email,
            nombre=nombre,
            rol=rol,
            password=password if password else None
        )
        
        if not updated_user:
            return RedirectResponse(
                url=f"/admin?error=Usuario no encontrado",
                status_code=302
            )
        
        return RedirectResponse(url="/admin?success=Usuario actualizado exitosamente", status_code=302)
    except ValueError as e:
        return RedirectResponse(
            url=f"/admin?error={str(e)}",
            status_code=302
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/admin?error=Error al actualizar usuario: {str(e)}",
            status_code=302
        )


@app.get("/hello/{name}")
async def say_hello(name: str):
    """Endpoint de ejemplo - no requiere autenticación."""
    return {"message": f"Hello {name}"}
