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
    # Obtener el historial de ejecuciones para mostrar en la tabla
    executions = await crud.list_executions(db, user_id=current_user.id, limit=50)
    
    return templates.TemplateResponse(
        "compras.html",
        {
            "request": request,
            "active_page": "compras",
            "current_user": current_user,
            "executions": executions
        }
    )


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
    carpeta_salida: str = Form(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Procesa dos archivos Excel: cruza información usando 'Purchasing Document' y actualiza la columna 'Proveedor'."""
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
            
            # Buscar o crear columna "Proveedor" en Archivo 1 (búsqueda case-insensitive)
            columna_proveedor = None
            for col in df_archivo1.columns:
                if str(col).strip().lower() == "proveedor":
                    columna_proveedor = col
                    break
            
            # Si no existe, crear la columna "Proveedor"
            if columna_proveedor is None:
                df_archivo1["Proveedor"] = ""
                columna_proveedor = "Proveedor"
            
            # Crear un diccionario de mapeo desde Archivo 2
            # Clave: Purchasing Document (normalizado), Valor: Name 1
            # Usar la primera coincidencia si hay múltiples
            mapa_proveedores = {}
            for idx, row in df_archivo2.iterrows():
                purchasing_doc = row[columna_purchasing_doc_archivo2]
                name1 = row[columna_name1]
                
                # Normalizar: convertir a string, trim, y asegurar tipo comparable
                if pd.notna(purchasing_doc) and pd.notna(name1):
                    purchasing_doc_normalizado = str(purchasing_doc).strip()
                    name1_valor = str(name1).strip()
                    
                    # Solo agregar si no existe ya (para usar la primera coincidencia)
                    if purchasing_doc_normalizado and purchasing_doc_normalizado not in mapa_proveedores:
                        mapa_proveedores[purchasing_doc_normalizado] = name1_valor
            
            # Procesar cada fila del Archivo 1
            for idx, row in df_archivo1.iterrows():
                purchasing_doc_valor = row[columna_purchasing_doc]
                
                # Normalizar el valor para comparar
                if pd.notna(purchasing_doc_valor):
                    purchasing_doc_normalizado = str(purchasing_doc_valor).strip()
                    
                    # Buscar coincidencia en el mapa
                    if purchasing_doc_normalizado in mapa_proveedores:
                        # Asignar el valor de Name 1 a Proveedor
                        df_archivo1.at[idx, columna_proveedor] = mapa_proveedores[purchasing_doc_normalizado]
                    else:
                        # Si no hay coincidencia, dejar vacío (o conservar si ya existe)
                        if pd.isna(df_archivo1.at[idx, columna_proveedor]) or df_archivo1.at[idx, columna_proveedor] == "":
                            df_archivo1.at[idx, columna_proveedor] = ""
                else:
                    # Si el valor de Purchasing Document está vacío, dejar Proveedor vacío
                    if pd.isna(df_archivo1.at[idx, columna_proveedor]) or df_archivo1.at[idx, columna_proveedor] == "":
                        df_archivo1.at[idx, columna_proveedor] = ""
            
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
            
            # Generar nombre del archivo de salida con sufijo
            nombre_base = Path(nombre_archivo_ventas).stem
            extension_base = Path(nombre_archivo_ventas).suffix
            nombre_archivo_procesado = f"{nombre_base}_procesado{extension_base}"
            archivo_resultado_path = carpeta_salida_path / nombre_archivo_procesado
            
            # Guardar el archivo procesado
            try:
                # Asegurar que el archivo se guarde como .xlsx
                if extension_base.lower() == '.xls':
                    archivo_resultado_path = carpeta_salida_path / f"{nombre_base}_procesado.xlsx"
                
                df_archivo1.to_excel(archivo_resultado_path, index=False, engine='openpyxl')
            except Exception as e:
                error_msg = f"Error al guardar el archivo procesado: {str(e)}"
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
                    duracion_segundos=duracion_segundos,
                    archivo_ruta=str(archivo_resultado_path),
                    archivo_nombre=nombre_archivo_procesado
                )
            
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": f"Archivo procesado exitosamente. Guardado en: {str(archivo_resultado_path)}",
                    "archivo_resultado": str(archivo_resultado_path),
                    "nombre_archivo": nombre_archivo_procesado,
                    "archivos_recibidos": {
                        "archivo_base": nombre_archivo_ventas,
                        "archivo_referencia": nombre_archivo_po
                    },
                    "carpeta_salida": str(carpeta_salida_path),
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
