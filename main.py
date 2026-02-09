from fastapi import FastAPI, Request, Depends, Form, UploadFile, File
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse, StreamingResponse
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.auth.router import router as auth_router, get_current_user, AuthenticationError
from app.db.init_db import init_db
from app.db.base import get_db
from app.db.models import User, ExecutionStatus, MasterUnificadoVirtualOperacion
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

# Meses en español para filtrar virtuales
MESES_VIRTUALES_ES = (
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
)

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
async def dashboard(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Dashboard principal - requiere autenticación. Incluye estadísticas y acciones de actualización centralizadas."""
    mes_actual = MESES_VIRTUALES_ES[datetime.now().month - 1]
    stats = {
        "total_ventas": await crud.count_ventas(db),
        "total_compras": await crud.count_compras(db),
        "total_virtuales": await crud.count_master_unificado_virtuales(db, mes=mes_actual),
        "total_clientes": await crud.count_clientes(db),
        "total_proveedores": await crud.count_proveedores(db),
        "total_materiales": await crud.count_materiales(db),
        "total_precios_compra": await crud.count_precios_materiales(db),
    }
    año_actual = datetime.now().year
    años_disponibles = list(range(año_actual, año_actual - 6, -1))
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "current_user": current_user,
            "active_page": "dashboard",
            "stats": stats,
            "años_disponibles": años_disponibles,
            "mes_actual_nombre": mes_actual,
        },
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


@app.get("/ventas-registros")
async def ventas_registros(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de registros de ventas - requiere autenticación."""
    total_ventas = await crud.count_ventas(db)
    
    return templates.TemplateResponse(
        "ventas_registros.html",
        {
            "request": request,
            "active_page": "ventas_registros",
            "current_user": current_user,
            "total_ventas": total_ventas
        }
    )


@app.get("/api/ventas")
async def api_ventas(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    search: Optional[str] = None,
    cliente: Optional[str] = None,
    codigo_cliente: Optional[int] = None,
    periodo_inicio: Optional[str] = None,
    periodo_fin: Optional[str] = None,
    producto: Optional[str] = None,
    planta: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """API para obtener ventas con filtros y paginación."""
    # Convertir fechas de string a datetime si están presentes
    periodo_inicio_dt = None
    periodo_fin_dt = None
    
    if periodo_inicio:
        try:
            periodo_inicio_dt = datetime.strptime(periodo_inicio, "%Y-%m-%d")
        except ValueError:
            pass
    
    if periodo_fin:
        try:
            periodo_fin_dt = datetime.strptime(periodo_fin, "%Y-%m-%d")
        except ValueError:
            pass
    
    ventas = await crud.list_ventas(
        db=db,
        limit=limit,
        offset=offset,
        search=search,
        cliente=cliente,
        codigo_cliente=codigo_cliente,
        periodo_inicio=periodo_inicio_dt,
        periodo_fin=periodo_fin_dt,
        producto=producto,
        planta=planta
    )
    
    total = await crud.count_ventas(
        db=db,
        search=search,
        cliente=cliente,
        codigo_cliente=codigo_cliente,
        periodo_inicio=periodo_inicio_dt,
        periodo_fin=periodo_fin_dt,
        producto=producto,
        planta=planta
    )
    
    # Convertir a diccionarios para JSON
    ventas_dict = []
    for venta in ventas:
        venta_data = {
            "id": venta.id,
            "cliente": venta.cliente,
            "codigo_cliente": venta.codigo_cliente,
            "grupo": venta.grupo.grupo if venta.grupo else None,
            "unidad_negocio": venta.unidad_negocio,
            "periodo": venta.periodo.strftime("%Y-%m-%d") if venta.periodo else None,
            "producto_condensado": venta.producto_condensado,
            "region_asc": venta.region_asc,
            "planta": venta.planta,
            "ship_to_party": venta.ship_to_party,
            "producto": venta.producto,
            "descripcion_producto": venta.descripcion_producto,
            "turnover_wo_metal": float(venta.turnover_wo_metal) if venta.turnover_wo_metal else None,
            "oe_turnover_like_fi": float(venta.oe_turnover_like_fi) if venta.oe_turnover_like_fi else None,
            "copper_sales_cuv": float(venta.copper_sales_cuv) if venta.copper_sales_cuv else None,
            "cu_sales_effect": float(venta.cu_sales_effect) if venta.cu_sales_effect else None,
            "cu_result": float(venta.cu_result) if venta.cu_result else None,
            "quantity_oe_to_m": float(venta.quantity_oe_to_m) if venta.quantity_oe_to_m else None,
            "quantity_oe_to_ft": float(venta.quantity_oe_to_ft) if venta.quantity_oe_to_ft else None,
            "cu_weight_techn_cut": float(venta.cu_weight_techn_cut) if venta.cu_weight_techn_cut else None,
            "cu_weight_sales_cuv": float(venta.cu_weight_sales_cuv) if venta.cu_weight_sales_cuv else None,
            "conversion_ft_a_m": float(venta.conversion_ft_a_m) if venta.conversion_ft_a_m else None,
            "sales_total_mts": float(venta.sales_total_mts) if venta.sales_total_mts else None,
            "sales_km": float(venta.sales_km) if venta.sales_km else None,
            "precio_exmetal_km": float(venta.precio_exmetal_km) if venta.precio_exmetal_km else None,
            "precio_full_metal_km": float(venta.precio_full_metal_km) if venta.precio_full_metal_km else None,
            "precio_exmetal_m": float(venta.precio_exmetal_m) if venta.precio_exmetal_m else None,
            "precio_full_metal_m": float(venta.precio_full_metal_m) if venta.precio_full_metal_m else None,
            "created_at": venta.created_at.isoformat() if venta.created_at else None
        }
        ventas_dict.append(venta_data)
    
    return JSONResponse({
        "ventas": ventas_dict,
        "total": total,
        "limit": limit,
        "offset": offset
    })


@app.get("/clientes")
async def clientes(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de clientes - requiere autenticación."""
    # Cargar los clientes desde la base de datos
    clientes_data = await crud.list_clientes(db, limit=5000)
    
    # Calcular estadísticas
    total_clientes = await crud.count_clientes(db)
    
    # Obtener países únicos para filtros
    paises = await crud.get_paises_clientes(db)
    total_paises = len(paises)
    
    # Obtener última actualización
    if clientes_data:
        ultima_actualizacion = max([c.updated_at for c in clientes_data if c.updated_at], default=None)
        if ultima_actualizacion:
            ultima_actualizacion = ultima_actualizacion.strftime('%d/%m/%Y %H:%M')
    else:
        ultima_actualizacion = None
    
    return templates.TemplateResponse(
        "clientes.html",
        {
            "request": request,
            "active_page": "clientes",
            "current_user": current_user,
            "clientes": clientes_data,
            "total_clientes": total_clientes,
            "paises": paises,
            "total_paises": total_paises,
            "ultima_actualizacion": ultima_actualizacion
        }
    )


@app.get("/grupos-clientes")
async def grupos_clientes(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de grupos de clientes - requiere autenticación."""
    # Cargar los grupos de clientes desde la base de datos
    grupos_data = await crud.list_cliente_grupos(db, limit=10000)
    
    # Calcular estadísticas
    total_grupos = await crud.count_cliente_grupos(db)
    
    # Obtener grupos únicos para filtros
    grupos_unicos = sorted(set([g.grupo for g in grupos_data if g.grupo]), key=lambda x: x or "")
    grupos_viejos_unicos = sorted(set([g.grupo_viejo for g in grupos_data if g.grupo_viejo]), key=lambda x: x or "")
    
    return templates.TemplateResponse(
        "grupos_clientes.html",
        {
            "request": request,
            "active_page": "grupos_clientes",
            "current_user": current_user,
            "grupos": grupos_data,
            "total_grupos": total_grupos,
            "grupos_unicos": grupos_unicos,
            "grupos_viejos_unicos": grupos_viejos_unicos
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
    
    # Obtener la fecha de la última compra para cada proveedor (optimizado con una sola consulta)
    from sqlalchemy import select, func
    from app.db.models import Compra
    
    # Obtener todas las fechas de última compra agrupadas por proveedor en una sola consulta
    codigos_proveedores = [p.codigo_proveedor for p in proveedores_data]
    if codigos_proveedores:
        query_ultimas_compras = select(
            Compra.codigo_proveedor,
            func.max(Compra.posting_date).label('fecha_ultima_compra')
        ).where(
            Compra.codigo_proveedor.in_(codigos_proveedores),
            Compra.posting_date.isnot(None)
        ).group_by(Compra.codigo_proveedor)
        
        result = await db.execute(query_ultimas_compras)
        fechas_por_proveedor = {row.codigo_proveedor: row.fecha_ultima_compra for row in result.all()}
    else:
        fechas_por_proveedor = {}
    
    # Agregar la fecha de la última compra a cada proveedor
    for proveedor in proveedores_data:
        proveedor.fecha_ultima_compra = fechas_por_proveedor.get(proveedor.codigo_proveedor)
    
    # Calcular estadísticas
    total_proveedores = await crud.count_proveedores(db)
    proveedores_activos = await crud.count_proveedores(db, estatus=True)
    proveedores_inactivos = await crud.count_proveedores(db, estatus=False)
    
    # Obtener los últimos 5 movimientos del historial
    historial_reciente = await crud.list_proveedor_historial(db, limit=5, offset=0)
    
    return templates.TemplateResponse(
        "proveedores.html",
        {
            "request": request,
            "active_page": "proveedores",
            "current_user": current_user,
            "proveedores": proveedores_data,
            "total_proveedores": total_proveedores,
            "proveedores_activos": proveedores_activos,
            "proveedores_inactivos": proveedores_inactivos,
            "historial_reciente": historial_reciente
        }
    )


@app.get("/materiales")
async def materiales(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de materiales - requiere autenticación."""
    # Cargar los materiales desde la base de datos
    materiales_data = await crud.list_materiales(db, limit=1000)
    
    # Calcular estadísticas
    total_materiales = await crud.count_materiales(db)
    
    # Obtener los últimos 5 movimientos del historial
    historial_reciente = await crud.list_material_historial(db, limit=5, offset=0)
    
    return templates.TemplateResponse(
        "materiales.html",
        {
            "request": request,
            "active_page": "materiales",
            "current_user": current_user,
            "materiales": materiales_data,
            "total_materiales": total_materiales,
            "historial_reciente": historial_reciente
        }
    )


@app.get("/precios-compra")
async def precios_compra(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página de precios de compra - requiere autenticación."""
    # Cargar los precios de materiales desde la base de datos con relaciones
    precios_data = await crud.list_precios_materiales(db, limit=1000)
    
    # Calcular estadísticas
    total_precios = await crud.count_precios_materiales(db)
    
    # Obtener los últimos 5 movimientos del historial
    historial_reciente = await crud.list_precio_material_historial(db, limit=5, offset=0)
    
    return templates.TemplateResponse(
        "precios_compra.html",
        {
            "request": request,
            "active_page": "precios_compra",
            "current_user": current_user,
            "precios": precios_data,
            "total_precios": total_precios,
            "historial_reciente": historial_reciente
        }
    )


@app.post("/api/precios-compra/actualizar")
async def actualizar_precios_compra_desde_compras(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Sincroniza precios de materiales desde la tabla compras.
    
    Busca todas las compras con numero_material, codigo_proveedor y price válidos
    y crea o actualiza los precios en precios_materiales.
    """
    try:
        resultado = await crud.sincronizar_precios_materiales_desde_compras(
            db=db,
            user_id=current_user.id
        )
        
        # Determinar el mensaje según el resultado
        if resultado["nuevos_creados"] > 0 or resultado["actualizados"] > 0:
            mensaje = (
                f"✓ Sincronización completada. Se crearon {resultado['nuevos_creados']} "
                f"precio(s) nuevo(s) y se actualizaron {resultado['actualizados']} precio(s) "
                f"de {resultado['total_encontrados']} encontrados en compras."
            )
        elif resultado["total_encontrados"] > 0:
            mensaje = (
                f"✓ Sincronización completada. Todos los precios "
                f"({resultado['total_encontrados']}) ya están actualizados."
            )
        else:
            mensaje = "✓ Sincronización completada. No se encontraron datos válidos en compras."
        
        if resultado["errores"]:
            mensaje += f" Se encontraron {len(resultado['errores'])} error(es)."
        
        # Considerar exitoso si no hay errores críticos
        success = len(resultado["errores"]) == 0 or resultado["nuevos_creados"] > 0 or resultado["actualizados"] > 0
        
        return JSONResponse({
            "success": success,
            "total_encontrados": resultado["total_encontrados"],
            "nuevos_creados": resultado["nuevos_creados"],
            "actualizados": resultado["actualizados"],
            "errores": resultado["errores"],
            "mensaje": mensaje
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "mensaje": f"Error al sincronizar precios desde compras: {str(e)}"
            }
        )


@app.get("/api/precios-compra/{precio_id}/ultimas-compras")
async def api_precio_ultimas_compras(
    precio_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Devuelve las últimas compras para el material+proveedor de ese precio."""
    from sqlalchemy import select, desc
    from app.db.models import Compra, PrecioMaterial

    precio = await crud.get_precio_material_by_id(db, precio_id)
    if not precio:
        return JSONResponse({"compras": [], "error": "Precio no encontrado"}, status_code=404)

    q = (
        select(
            Compra.posting_date,
            Compra.purchasing_document,
            Compra.numero_material,
            Compra.nombre_proveedor,
            Compra.price,
        )
        .where(
            Compra.codigo_proveedor == precio.codigo_proveedor,
            Compra.numero_material == precio.numero_material,
        )
        .order_by(desc(Compra.posting_date), desc(Compra.id))
        .limit(100)
    )
    result = await db.execute(q)
    rows = result.all()

    items = []
    for r in rows:
        items.append({
            "fecha": r.posting_date.isoformat() if r.posting_date else None,
            "orden_compra": r.purchasing_document,
            "material": r.numero_material,
            "proveedor": r.nombre_proveedor,
            "precio": float(r.price) if r.price is not None else None,
        })
    return JSONResponse({"compras": items})


@app.get("/api/precios-compra/historial")
async def api_precios_compra_historial(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Devuelve los últimos 15 movimientos del historial de precios de materiales."""
    historial = await crud.list_precio_material_historial(
        db,
        limit=15,
        offset=0
    )
    items = []
    for h in historial:
        items.append({
            "id": h.id,
            "precio_material_id": h.precio_material_id,
            "codigo_proveedor": h.codigo_proveedor,
            "numero_material": h.numero_material,
            "operacion": h.operacion.value,
            "user_email": h.user.email if h.user else None,
            "user_nombre": h.user.nombre if h.user else None,
            "datos_antes": h.datos_antes,
            "datos_despues": h.datos_despues,
            "campos_modificados": h.campos_modificados,
            "created_at": h.created_at.isoformat() if h.created_at else None,
        })
    return JSONResponse({"movimientos": items})


@app.post("/api/precios-compra/actualizar-pais-origen")
async def actualizar_pais_origen_precios(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Actualiza el país de origen en precios_materiales usando datos de pais_origen_material.
    
    Busca todos los registros en precios_materiales donde country_origin esté vacío o sea null,
    y para cada uno busca en pais_origen_material usando codigo_proveedor y numero_material
    para obtener el pais_origen correspondiente.
    """
    try:
        resultado = await crud.actualizar_pais_origen_en_precios_materiales(
            db=db,
            user_id=current_user.id
        )
        
        # Determinar el mensaje según el resultado
        if resultado["actualizados"] > 0:
            mensaje = (
                f"✓ Actualización completada. Se actualizaron {resultado['actualizados']} "
                f"país(es) de origen de {resultado['total_procesados']} registros procesados."
            )
            if resultado["no_encontrados"] > 0:
                mensaje += f" {resultado['no_encontrados']} registro(s) no tienen país de origen definido en la tabla de países."
        elif resultado["total_procesados"] > 0:
            mensaje = (
                f"✓ Actualización completada. No se requirieron cambios en los "
                f"{resultado['total_procesados']} registros procesados."
            )
            if resultado["no_encontrados"] > 0:
                mensaje += f" {resultado['no_encontrados']} registro(s) no tienen país de origen definido en la tabla de países."
        else:
            mensaje = "✓ No hay registros sin país de origen para actualizar."
        
        if resultado["errores"]:
            mensaje += f" Se encontraron {len(resultado['errores'])} error(es)."
        
        return JSONResponse({
            "success": resultado["exitoso"],
            "mensaje": mensaje,
            "total_procesados": resultado["total_procesados"],
            "actualizados": resultado["actualizados"],
            "no_encontrados": resultado["no_encontrados"],
            "errores": resultado["errores"][:10] if resultado["errores"] else []
        })
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "mensaje": f"Error al actualizar países de origen: {str(e)}"
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
    
    # Calcular materiales con país de origen "Pendiente"
    result_pendientes = await db.execute(
        select(func.count(distinct(PaisOrigenMaterial.numero_material))).where(
            PaisOrigenMaterial.pais_origen == "Pendiente"
        )
    )
    materiales_pendientes = result_pendientes.scalar() or 0
    
    # Obtener los últimos 5 movimientos del historial
    historial_reciente = await crud.list_pais_origen_material_historial(db, limit=5, offset=0)
    
    return templates.TemplateResponse(
        "paises_origen.html",
        {
            "request": request,
            "active_page": "paises_origen",
            "current_user": current_user,
            "paises": paises_data,
            "total_paises": total_paises,
            "total_partes_unicos": total_partes_unicos,
            "materiales_pendientes": materiales_pendientes,
            "historial_reciente": historial_reciente
        }
    )


@app.get("/carga-proveedor")
async def carga_proveedor(
    request: Request, 
    current_user: User = Depends(get_current_user), 
    db: AsyncSession = Depends(get_db),
    page: int = 1,
    search: str = "",
    estatus: str = ""
):
    """Página de carga de cliente/proveedor - requiere autenticación."""
    limit = 50
    offset = (page - 1) * limit
    
    # Obtener proveedores
    proveedores = await crud.list_carga_proveedores(
        db, 
        limit=limit, 
        offset=offset,
        codigo_proveedor=search if search else None,
        estatus=estatus if estatus else None
    )
    total_proveedores = await crud.count_carga_proveedores(
        db,
        codigo_proveedor=search if search else None,
        estatus=estatus if estatus else None
    )
    
    total_pages = (total_proveedores + limit - 1) // limit
    
    # Obtener historial reciente de movimientos
    historial = await crud.list_carga_proveedor_historial(db, limit=20)
    total_historial = await crud.count_carga_proveedor_historial(db)
    
    # Obtener conteos por estatus para las tarjetas
    count_alta = await crud.count_carga_proveedores(db, estatus="Alta")
    count_baja = await crud.count_carga_proveedores(db, estatus="Baja")
    count_sin_modificacion = await crud.count_carga_proveedores(db, estatus="Sin modificacion")
    count_total = await crud.count_carga_proveedores(db)
    
    return templates.TemplateResponse(
        "carga_cliente_proveedor.html",
        {
            "request": request,
            "active_page": "carga_proveedor",
            "current_user": current_user,
            "proveedores": proveedores,
            "total_proveedores": total_proveedores,
            "page": page,
            "total_pages": total_pages,
            "search": search,
            "estatus": estatus,
            "historial": historial,
            "total_historial": total_historial,
            "count_alta": count_alta,
            "count_baja": count_baja,
            "count_sin_modificacion": count_sin_modificacion,
            "count_total": count_total,
        }
    )


@app.post("/carga-proveedor/actualizar")
async def actualizar_carga_proveedores(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Actualiza los estatus de proveedores según las compras de los últimos 6 meses.
    Solo permite la actualización si no hay registros creados en el historial este mes.
    """
    mes_actual = MESES_VIRTUALES_ES[datetime.now().month - 1]
    count_este_mes = await crud.count_carga_proveedor_historial_este_mes(db)
    if count_este_mes > 0:
        return JSONResponse(
            status_code=200,
            content={
                "success": False,
                "ya_actualizado": True,
                "message": f"Ese mes ({mes_actual}) ya se actualizaron los registros de Carga Proveedor.",
                "mes": mes_actual,
            }
        )
    resultado = await crud.actualizar_estatus_carga_proveedores_por_compras(db)
    
    if resultado["exitoso"]:
        return JSONResponse(
            content={
                "success": True,
                "message": f"Actualización completada. Nuevos: {resultado['proveedores_nuevos']}, Sin modificación: {resultado['proveedores_sin_modificacion']}, Baja: {resultado['proveedores_marcados_baja']}, Omitidos (MX): {resultado.get('proveedores_omitidos_mx', 0)}",
                "data": resultado
            },
            status_code=200
        )
    else:
        return JSONResponse(
            content={
                "success": False,
                "message": f"Error durante la actualización: {resultado.get('error', 'Error desconocido')}",
                "data": resultado
            },
            status_code=500
        )


@app.get("/carga-proveedores-nacional")
async def carga_proveedores_nacional(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    page: int = 1,
    search: str = "",
    estatus: str = "",
    operacion: str = ""
):
    """Vista de carga de proveedores nacionales (Aduanas) - requiere autenticación."""
    limit = 50
    offset = (page - 1) * limit
    registros = await crud.list_carga_proveedores_nacional(
        db,
        limit=limit,
        offset=offset,
        codigo_proveedor=search if search else None,
        estatus=estatus if estatus else None,
        operacion=operacion if operacion else None,
    )
    total = await crud.count_carga_proveedores_nacional(
        db,
        codigo_proveedor=search if search else None,
        estatus=estatus if estatus else None,
        operacion=operacion if operacion else None,
    )
    total_pages = max(1, (total + limit - 1) // limit)
    count_alta = await crud.count_carga_proveedores_nacional(db, estatus="Alta")
    count_baja = await crud.count_carga_proveedores_nacional(db, estatus="Baja")
    count_sin_modificacion = await crud.count_carga_proveedores_nacional(db, estatus="Sin modificacion")
    count_total = await crud.count_carga_proveedores_nacional(db)
    historial = await crud.list_carga_proveedores_nacional_historial(db, limit=50)
    total_historial = await crud.count_carga_proveedores_nacional_historial(db)
    return templates.TemplateResponse(
        "carga_proveedores_nacional.html",
        {
            "request": request,
            "active_page": "carga_proveedores_nacional",
            "current_user": current_user,
            "registros": registros,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "search": search,
            "estatus": estatus,
            "operacion": operacion,
            "count_alta": count_alta,
            "count_baja": count_baja,
            "count_sin_modificacion": count_sin_modificacion,
            "count_total": count_total,
            "historial": historial,
            "total_historial": total_historial,
        }
    )


@app.post("/carga-proveedores-nacional/actualizar")
async def actualizar_carga_proveedores_nacional(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Actualiza los estatus de proveedores nacionales (solo MX) según compras de los últimos 6 meses.
    Solo permite la actualización si no hay registros creados en el historial este mes.
    """
    mes_actual = MESES_VIRTUALES_ES[datetime.now().month - 1]
    count_este_mes = await crud.count_carga_proveedores_nacional_historial_este_mes(db)
    if count_este_mes > 0:
        return JSONResponse(
            status_code=200,
            content={
                "success": False,
                "ya_actualizado": True,
                "message": f"Ese mes ({mes_actual}) ya se actualizaron los registros de Carga Proveedores Nacional.",
                "mes": mes_actual,
            }
        )
    resultado = await crud.actualizar_estatus_carga_proveedores_nacional_por_compras(db)
    if resultado.get("exitoso"):
        return JSONResponse(
            content={
                "success": True,
                "message": f"Actualización completada. Nuevos: {resultado['proveedores_nuevos']}, Sin modificación: {resultado['proveedores_sin_modificacion']}, Baja: {resultado['proveedores_marcados_baja']}, Eliminados: {resultado['proveedores_eliminados']}",
                "data": resultado
            },
            status_code=200
        )
    return JSONResponse(
        content={
            "success": False,
            "message": resultado.get("error", "Error desconocido"),
            "data": resultado
        },
        status_code=500
    )


@app.get("/carga-cliente")
async def carga_cliente(
    request: Request, 
    current_user: User = Depends(get_current_user), 
    db: AsyncSession = Depends(get_db),
    page: int = 1,
    search: str = "",
    estatus: str = ""
):
    """Página de carga de cliente - requiere autenticación."""
    limit = 50
    offset = (page - 1) * limit

    search_param = search.strip() if search else None

    # Obtener clientes (filtro por código o nombre)
    clientes = await crud.list_carga_clientes(
        db,
        limit=limit,
        offset=offset,
        search=search_param,
        estatus=estatus if estatus else None
    )
    total_clientes = await crud.count_carga_clientes(
        db,
        search=search_param,
        estatus=estatus if estatus else None
    )
    
    total_pages = (total_clientes + limit - 1) // limit
    
    # Obtener conteos por estatus para las tarjetas
    count_alta = await crud.count_carga_clientes(db, estatus="Alta")
    count_baja = await crud.count_carga_clientes(db, estatus="Baja")
    count_sin_modificacion = await crud.count_carga_clientes(db, estatus="Sin modificacion")
    count_total = await crud.count_carga_clientes(db)
    
    # Obtener historial de movimientos (últimos 50)
    historial = await crud.list_carga_cliente_historial(db, limit=50)
    
    return templates.TemplateResponse(
        "carga_cliente.html",
        {
            "request": request,
            "active_page": "carga_cliente",
            "current_user": current_user,
            "clientes": clientes,
            "total_clientes": total_clientes,
            "page": page,
            "total_pages": total_pages,
            "search": search,
            "estatus": estatus,
            "count_alta": count_alta,
            "count_baja": count_baja,
            "count_sin_modificacion": count_sin_modificacion,
            "count_total": count_total,
            "historial": historial,
        }
    )


@app.post("/carga-cliente/actualizar")
async def actualizar_carga_clientes(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Actualiza los estatus de carga_clientes basándose en las ventas.
    Solo permite la actualización si no hay registros creados en el historial este mes.
    """
    mes_actual = MESES_VIRTUALES_ES[datetime.now().month - 1]
    count_este_mes = await crud.count_carga_cliente_historial_este_mes(db)
    if count_este_mes > 0:
        return {
            "success": False,
            "ya_actualizado": True,
            "message": f"Ese mes ({mes_actual}) ya se actualizaron los registros de Carga Cliente.",
            "mes": mes_actual,
        }
    try:
        resumen = await crud.actualizar_carga_clientes_desde_ventas(db)
        
        return {
            "success": True,
            "message": f"Actualización completada: {resumen['altas']} altas, {resumen['bajas']} bajas, {resumen['sin_modificacion']} sin modificación, {resumen['eliminados']} eliminados, {resumen['sin_cambios']} ya actualizados este mes",
            "resumen": resumen
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Error al actualizar: {str(e)}",
            "resumen": None
        }


@app.get("/carga-proveedor-cliente/descargar-excel")
async def descargar_carga_proveedor_cliente_excel(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Descarga un Excel con una sola hoja: primero proveedores (carga_proveedores) y luego clientes (carga_clientes)."""
    import io
    import pandas as pd

    COLUMNAS = [
        "Nombre o Razón social",
        "Apellido Paterno",
        "Apellido Materno",
        "País",
        "Domicilio",
        "Cliente/Proveedor",
        "Estatus",
    ]

    def _valor(v):
        if v is None:
            return ""
        if hasattr(v, "strftime"):
            return v.strftime("%Y-%m-%d") if hasattr(v, "hour") and v.hour == 0 and v.minute == 0 else v.strftime("%Y-%m-%d %H:%M")
        if isinstance(v, bool):
            return "Sí" if v else "No"
        return str(v)

    # Obtener datos
    proveedores = await crud.list_carga_proveedores(db, limit=50000, offset=0)
    clientes = await crud.list_carga_clientes(db, limit=50000, offset=0)

    filas = []

    # Primero proveedores (carga_proveedores)
    for p in proveedores:
        filas.append({
            "Nombre o Razón social": _valor(p.nombre),
            "Apellido Paterno": _valor(p.apellido_paterno),
            "Apellido Materno": _valor(p.apellido_materno),
            "País": _valor(p.pais),
            "Domicilio": _valor(p.domicilio),
            "Cliente/Proveedor": _valor(p.cliente_proveedor) or "Proveedor",
            "Estatus": _valor(p.estatus),
        })

    # Luego clientes (carga_clientes)
    for c in clientes:
        filas.append({
            "Nombre o Razón social": _valor(c.nombre),
            "Apellido Paterno": "",
            "Apellido Materno": "",
            "País": _valor(c.pais),
            "Domicilio": _valor(c.domicilio),
            "Cliente/Proveedor": _valor(c.cliente_proveedor) or "Cliente",
            "Estatus": _valor(c.estatus),
        })

    df = pd.DataFrame(filas, columns=COLUMNAS)
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl", sheet_name="Carga Proveedores y Clientes")
    buffer.seek(0)

    nombre_archivo = "carga_proveedores_clientes.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{nombre_archivo}"'},
    )


@app.get("/carga-proveedores-nacional/descargar-excel")
async def descargar_carga_proveedores_nacional_excel(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Descarga un Excel de carga_proveedores_nacional con columnas: RFC, Operaciones Virtuales, Estatus."""
    import io
    import pandas as pd

    COLUMNAS = ["RFC", "Operaciones Virtuales", "Estatus"]

    def _valor(v):
        if v is None:
            return ""
        return str(v)

    registros = await crud.list_carga_proveedores_nacional(db, limit=50000, offset=0)
    filas = [
        {
            "RFC": _valor(p.rfc),
            "Operaciones Virtuales": _valor(p.operacion),
            "Estatus": _valor(p.estatus),
        }
        for p in registros
    ]

    df = pd.DataFrame(filas, columns=COLUMNAS)
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl", sheet_name="Carga Proveedores Nacionales")
    buffer.seek(0)

    nombre_archivo = "carga_proveedores_nacionales.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{nombre_archivo}"'},
    )


@app.get("/virtuales/guia")
async def virtuales_guia(current_user: User = Depends(get_current_user)):
    """Sirve la guía de materialidad operaciones virtuales (PPTX) para abrir en nueva pestaña."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, "MATERIALIDAD OPERACIONES VIRTUALES 2026 (1) 2.pptx")
    if not os.path.isfile(path):
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Guía no encontrada")
    return FileResponse(
        path,
        filename="Materialidad_Operaciones_Virtuales_2026.pptx",
        media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
    )


@app.get("/virtuales")
async def virtuales(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Página Virtuales (master unificado) dentro de Aduanas - requiere autenticación."""
    mes_actual = MESES_VIRTUALES_ES[datetime.now().month - 1]

    # Tabla: todos los registros (sin filtrar por mes)
    virtuales_data = await crud.list_master_unificado_virtuales(db, limit=2000, offset=0)

    # Tarjetas de estadísticas: solo datos del mes actual
    total_virtuales = await crud.count_master_unificado_virtuales(db, mes=mes_actual)
    virtuales_mes_actual = await crud.list_master_unificado_virtuales(db, limit=5000, offset=0, mes=mes_actual)
    from collections import Counter
    estatus_counts = Counter(v.estatus or "Sin estatus" for v in virtuales_mes_actual)
    estatus_counts = dict(sorted(estatus_counts.items(), key=lambda x: x[1], reverse=True))

    # Últimos 10 movimientos del historial
    historial_reciente = await crud.list_master_unificado_virtuales_historial(
        db, limit=10, offset=0
    )

    # Años para el selector de descarga Excel (actual y 5 anteriores)
    año_actual = datetime.now().year
    años_disponibles = list(range(año_actual, año_actual - 6, -1))
    
    return templates.TemplateResponse(
        "virtuales.html",
        {
            "request": request,
            "active_page": "virtuales",
            "current_user": current_user,
            "virtuales": virtuales_data,
            "total_virtuales": total_virtuales,
            "estatus_counts": estatus_counts,
            "historial_reciente": historial_reciente,
            "años_disponibles": años_disponibles,
            "mes_actual_nombre": mes_actual,
        }
    )


@app.get("/virtuales/descargar-excel")
async def descargar_virtuales_excel(
    request: Request,
    mes: str = "",
    anio: Optional[int] = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Descarga los registros de virtuales del mes y año seleccionados en Excel."""
    import io
    import pandas as pd

    if not mes or not mes.strip():
        return JSONResponse(
            status_code=400,
            content={"error": "Seleccione un mes para descargar."},
        )
    año_actual = datetime.now().year
    if anio is None or anio < 2000 or anio > año_actual + 1:
        anio = año_actual

    mes = mes.strip()
    registros = await crud.list_master_unificado_virtuales(
        db, limit=50000, offset=0, mes=mes, año=anio
    )

    def _valor(v):
        if v is None:
            return ""
        if hasattr(v, "strftime"):
            return v.strftime("%Y-%m-%d") if hasattr(v, "hour") and v.hour == 0 and v.minute == 0 else v.strftime("%Y-%m-%d %H:%M")
        if isinstance(v, bool):
            return "Sí" if v else "No"
        return str(v)

    columnas = [
        "numero", "proveedor_cliente", "impo_expo", "agente", "mes", "estatus", "tipo",
        "incoterm", "tipo_exportacion", "escenario", "plazo", "pedimento", "aduana", "patente",
        "destino", "cliente_space", "complemento", "tipo_immex", "factura", "fecha_pago",
        "informacion", "servicio_cliente", "firma", "solicitud_previo", "op_regular", "carretes",
        "created_at",
    ]
    filas = []
    for r in registros:
        filas.append({
            "numero": _valor(r.numero),
            "proveedor_cliente": _valor(r.proveedor_cliente),
            "impo_expo": _valor(r.impo_expo),
            "agente": _valor(r.agente),
            "mes": _valor(r.mes),
            "estatus": _valor(r.estatus),
            "tipo": _valor(r.tipo),
            "incoterm": _valor(r.incoterm),
            "tipo_exportacion": _valor(r.tipo_exportacion),
            "escenario": _valor(r.escenario),
            "plazo": _valor(r.plazo),
            "pedimento": _valor(r.pedimento),
            "aduana": _valor(r.aduana),
            "patente": _valor(r.patente),
            "destino": _valor(r.destino),
            "cliente_space": _valor(r.cliente_space),
            "complemento": _valor(r.complemento),
            "tipo_immex": _valor(r.tipo_immex),
            "factura": _valor(r.factura),
            "fecha_pago": _valor(r.fecha_pago),
            "informacion": _valor(r.informacion),
            "servicio_cliente": _valor(r.servicio_cliente),
            "firma": _valor(r.firma),
            "solicitud_previo": _valor(r.solicitud_previo),
            "op_regular": _valor(r.op_regular),
            "carretes": _valor(r.carretes),
            "created_at": _valor(r.created_at),
        })

    df = pd.DataFrame(filas, columns=columnas)
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    nombre_archivo = f"virtuales_{mes}_{anio}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{nombre_archivo}"'},
    )


@app.post("/api/virtuales")
async def crear_virtual(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Crea un nuevo registro de virtuales."""
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={"error": "No se pudo leer el cuerpo de la solicitud"}
        )
    
    numero_raw = data.get("numero")
    if numero_raw in (None, "", "null"):
        return JSONResponse(
            status_code=400,
            content={"error": "El campo 'numero' es requerido"}
        )
    
    try:
        numero = int(numero_raw)
    except (ValueError, TypeError):
        return JSONResponse(
            status_code=400,
            content={"error": "El campo 'numero' debe ser numérico"}
        )
    
    existente = await crud.get_master_unificado_virtuales_by_numero(db, numero)
    if existente:
        return JSONResponse(
            status_code=409,
            content={"error": f"Ya existe un registro con el número {numero}"}
        )

    # Campos obligatorios al crear: numero (ya validado), nombre (proveedor_cliente), impo_expo, tipo
    proveedor_cliente = (data.get("proveedor_cliente") or "").strip()
    if not proveedor_cliente:
        return JSONResponse(
            status_code=400,
            content={"error": "El campo 'Nombre' (Proveedor/Cliente) es obligatorio"}
        )
    impo_expo = (data.get("impo_expo") or "").strip()
    if not impo_expo:
        return JSONResponse(
            status_code=400,
            content={"error": "El campo 'Impo/Expo' es obligatorio"}
        )
    tipo = (data.get("tipo") or "").strip()
    if not tipo:
        return JSONResponse(
            status_code=400,
            content={"error": "El campo 'Tipo' es obligatorio"}
        )
    
    def parse_bool(value):
        if value is None or str(value).strip() == "":
            return None
        if isinstance(value, bool):
            return value
        valor_normalizado = str(value).strip().lower()
        if valor_normalizado in {"si", "sí", "true", "1"}:
            return True
        if valor_normalizado in {"no", "false", "0"}:
            return False
        return None
    
    def parse_int(value):
        if value is None or str(value).strip() == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def parse_str(value):
        if value is None:
            return None
        valor = str(value).strip()
        return valor or None
    
    fecha_pago = None
    fecha_pago_raw = data.get("fecha_pago")
    if isinstance(fecha_pago_raw, str) and fecha_pago_raw.strip():
        for formato in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                fecha_pago = datetime.strptime(fecha_pago_raw.strip(), formato).date()
                break
            except ValueError:
                continue
    
    try:
        nuevo = await crud.create_master_unificado_virtuales(
            db=db,
            solicitud_previo=parse_bool(data.get("solicitud_previo")),
            agente=parse_str(data.get("agente")),
            pedimento=parse_int(data.get("pedimento")),
            aduana=parse_int(data.get("aduana")),
            patente=parse_int(data.get("patente")),
            destino=parse_int(data.get("destino")),
            cliente_space=parse_str(data.get("cliente_space")),
            impo_expo=parse_str(data.get("impo_expo")),
            proveedor_cliente=parse_str(data.get("proveedor_cliente")),
            mes=parse_str(data.get("mes")),
            complemento=parse_str(data.get("complemento")),
            tipo_immex=parse_str(data.get("tipo_immex")),
            factura=parse_str(data.get("factura")),
            fecha_pago=fecha_pago,
            informacion=parse_str(data.get("informacion")),
            estatus=parse_str(data.get("estatus")),
            op_regular=parse_bool(data.get("op_regular")),
            tipo=parse_str(data.get("tipo")),
            numero=numero,
            carretes=parse_bool(data.get("carretes")),
            servicio_cliente=parse_str(data.get("servicio_cliente")),
            plazo=parse_str(data.get("plazo")),
            firma=parse_str(data.get("firma")),
            incoterm=parse_str(data.get("incoterm")),
            tipo_exportacion=parse_str(data.get("tipo_exportacion")),
            escenario=parse_str(data.get("escenario")),
            user_id=current_user.id
        )
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Error al crear el registro: {str(exc)}"}
        )
    
    return JSONResponse(
        status_code=201,
        content={
            "success": True,
            "message": "Registro creado correctamente",
            "numero": nuevo.numero
        }
    )


@app.delete("/api/virtuales/{numero}")
async def eliminar_virtual(
    numero: int,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Elimina un registro de virtuales. Solo admin y solicitando detalle."""
    if (current_user.rol or "").lower() != "admin":
        return JSONResponse(
            status_code=403,
            content={"error": "Solo los usuarios administradores pueden eliminar registros"}
        )
    
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    
    detalle = (payload.get("detalle") or payload.get("comentario") or payload.get("motivo") or "").strip()
    if not detalle:
        return JSONResponse(
            status_code=400,
            content={"error": "El detalle de eliminación es obligatorio"}
        )
    
    registro = await crud.get_master_unificado_virtuales_by_numero(db, numero)
    if not registro:
        return JSONResponse(
            status_code=404,
            content={"error": f"No se encontró el registro con número {numero}"}
        )
    
    datos_antes = crud.master_unificado_virtual_to_dict(registro)
    eliminado = await crud.delete_master_unificado_virtuales(db, numero)
    if not eliminado:
        return JSONResponse(
            status_code=404,
            content={"error": f"No se encontró el registro con número {numero}"}
        )
    
    await crud.create_master_unificado_virtual_historial(
        db,
        numero=numero,
        operacion=MasterUnificadoVirtualOperacion.DELETE,
        user_id=current_user.id,
        datos_antes=datos_antes,
        datos_despues=None,
        campos_modificados=None,
        comentario=detalle
    )
    
    return JSONResponse(
        status_code=200,
        content={
            "success": True,
            "message": "Registro eliminado correctamente",
            "numero": numero
        }
    )


@app.post("/api/virtuales/actualizar")
async def actualizar_master_virtuales(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Ejecuta la actualización del master de virtuales basándose en compras
    del mes anterior. Crea registros para proveedores existentes y nuevos.
    Solo permite la actualización si no hay registros creados para el mes actual.
    """
    try:
        mes_actual = MESES_VIRTUALES_ES[datetime.now().month - 1]
        count_mes = await crud.count_master_unificado_virtuales(db, mes=mes_actual)
        if count_mes > 0:
            return JSONResponse(
                status_code=200,
                content={
                    "success": False,
                    "ya_actualizado": True,
                    "message": f"Ese mes ({mes_actual}) ya se actualizaron los registros.",
                    "mes": mes_actual,
                }
            )
        resumen = await crud.actualizar_master_virtuales_desde_compras(
            db=db,
            user_id=current_user.id
        )
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Actualización completada",
                "resumen": resumen
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "message": "Error al actualizar el master de virtuales"
            }
        )


@app.get("/api/virtuales/historial")
async def api_virtuales_historial(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    numero: Optional[int] = None,
    operacion: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """API para consultar el historial de movimientos del master virtuales."""
    from app.db.models import MasterUnificadoVirtualOperacion
    operacion_enum = None
    if operacion:
        try:
            operacion_enum = MasterUnificadoVirtualOperacion(operacion.upper())
        except ValueError:
            pass
    historial = await crud.list_master_unificado_virtuales_historial(
        db, numero=numero, operacion=operacion_enum, limit=limit, offset=offset
    )
    total = await crud.count_master_unificado_virtuales_historial(
        db, numero=numero, operacion=operacion_enum
    )
    items = []
    for h in historial:
        items.append({
            "id": h.id,
            "numero": h.numero,
            "operacion": h.operacion.value if h.operacion else None,
            "user_id": h.user_id,
            "user_email": h.user.email if h.user else None,
            "datos_antes": h.datos_antes,
            "datos_despues": h.datos_despues,
            "campos_modificados": h.campos_modificados,
            "comentario": h.comentario,
            "created_at": h.created_at.isoformat() if h.created_at else None,
        })
    return JSONResponse(
        status_code=200,
        content={"historial": items, "total": total}
    )


@app.get("/api/virtuales/movimientos")
async def api_virtuales_movimientos(
    request: Request,
    numero: Optional[str] = None,
    tipo: str = "",
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Devuelve las últimas 5 compras (si tipo=Proveedor) o ventas (si tipo=Cliente) para el numero dado."""
    if not numero or not tipo:
        return JSONResponse(
            status_code=400,
            content={"error": "Se requieren numero y tipo (Proveedor o Cliente)."},
        )
    tipo_lower = tipo.strip().lower()
    if "proveedor" in tipo_lower:
        compras = await crud.get_ultimas_compras_proveedor(db, str(numero).strip(), limit=5)
        items = []
        for c in compras:
            items.append({
                "posting_date": c.posting_date.isoformat() if c.posting_date else None,
                "purchasing_document": c.purchasing_document,
                "material_document": c.material_document,
                "numero_material": c.numero_material,
                "descripcion_material": (c.descripcion_material or "")[:80],
                "quantity": float(c.quantity) if c.quantity is not None else None,
                "amount": float(c.amount) if c.amount is not None else None,
                "currency": c.currency,
                "nombre_proveedor": c.nombre_proveedor,
            })
        return JSONResponse(content={"tipo": "compras", "movimientos": items})
    if "cliente" in tipo_lower:
        try:
            cod_cliente = int(str(numero).strip())
        except (TypeError, ValueError):
            cod_cliente = None
        ventas = await crud.get_ultimas_ventas_cliente(db, cod_cliente, limit=5)
        items = []
        for v in ventas:
            items.append({
                "periodo": v.periodo.isoformat() if v.periodo else None,
                "cliente": v.cliente,
                "codigo_cliente": v.codigo_cliente,
                "producto": v.producto,
                "descripcion_producto": (v.descripcion_producto or "")[:80],
                "sales_total_mts": float(v.sales_total_mts) if v.sales_total_mts is not None else None,
                "turnover_wo_metal": float(v.turnover_wo_metal) if v.turnover_wo_metal is not None else None,
                "planta": v.planta,
            })
        return JSONResponse(content={"tipo": "ventas", "movimientos": items})
    return JSONResponse(
        status_code=400,
        content={"error": "tipo debe ser Proveedor o Cliente."},
    )


@app.put("/api/virtuales/{numero}")
async def actualizar_virtual(
    numero: int,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Actualiza un registro de virtuales."""
    try:
        data = await request.json()
        
        # Convertir valores booleanos
        solicitud_previo = None
        if data.get("solicitud_previo"):
            solicitud_previo = data.get("solicitud_previo") == "Sí"
        
        op_regular = None
        if data.get("op_regular"):
            op_regular = data.get("op_regular") == "Sí"
        
        carretes = None
        if data.get("carretes"):
            carretes = data.get("carretes") == "Sí"
        
        # Convertir valores numéricos
        pedimento = None
        if data.get("pedimento"):
            try:
                pedimento = int(data.get("pedimento"))
            except (ValueError, TypeError):
                pass
        
        aduana = None
        if data.get("aduana"):
            try:
                aduana = int(data.get("aduana"))
            except (ValueError, TypeError):
                pass
        
        patente = None
        if data.get("patente"):
            try:
                patente = int(data.get("patente"))
            except (ValueError, TypeError):
                pass
        
        destino = None
        if data.get("destino"):
            try:
                destino = int(data.get("destino"))
            except (ValueError, TypeError):
                pass
        
        virtual_actualizado = await crud.update_master_unificado_virtuales(
            db=db,
            numero=numero,
            solicitud_previo=solicitud_previo,
            agente=data.get("agente") or None,
            pedimento=pedimento,
            aduana=aduana,
            patente=patente,
            destino=destino,
            cliente_space=data.get("cliente_space") or None,
            impo_expo=data.get("impo_expo") or None,
            proveedor_cliente=data.get("proveedor_cliente") or None,
            mes=data.get("mes") or None,
            complemento=data.get("complemento") or None,
            tipo_immex=data.get("tipo_immex") or None,
            factura=data.get("factura") or None,
            informacion=data.get("informacion") or None,
            estatus=data.get("estatus") or None,
            op_regular=op_regular,
            tipo=data.get("tipo") or None,
            carretes=carretes,
            servicio_cliente=data.get("servicio_cliente") or None,
            plazo=data.get("plazo") or None,
            firma=data.get("firma") or None,
            incoterm=data.get("incoterm") or None,
            tipo_exportacion=data.get("tipo_exportacion") or None,
            escenario=data.get("escenario") or None,
            user_id=current_user.id
        )
        
        if not virtual_actualizado:
            return JSONResponse(
                status_code=404,
                content={"error": f"No se encontró el registro con número {numero}"}
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Registro actualizado correctamente",
                "numero": numero
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Error al actualizar el registro: {str(e)}"}
        )


@app.post("/api/virtuales/{numero}/duplicar-impo")
async def duplicar_virtual_expo_a_impo(
    numero: int,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Duplica un registro EXPO como IMPO con la misma información."""
    try:
        # Obtener el registro EXPO más reciente para este numero
        registro_expo = await crud.get_master_unificado_virtuales_by_numero_impoexpo(
            db=db,
            numero=numero,
            impo_expo="EXPO"
        )
        
        if not registro_expo:
            return JSONResponse(
                status_code=404,
                content={"error": f"No se encontró un registro EXPO con número {numero}"}
            )
        
        # Verificar si ya existe un registro IMPO para este numero y mes
        if await crud.existe_virtual_numero_mes(
            db=db,
            numero=numero,
            mes_captura=registro_expo.mes or "",
            tipo=registro_expo.tipo or "",
            impo_expo="IMPO"
        ):
            return JSONResponse(
                status_code=409,
                content={"error": f"Ya existe un registro IMPO para el número {numero} en el mes {registro_expo.mes}"}
            )
        
        # Sincronizar secuencia de id para evitar UniqueViolationError (id ya existente)
        from sqlalchemy import text
        try:
            await db.execute(
                text(
                    "SELECT setval(pg_get_serial_sequence('master_unificado_virtuales', 'id'), "
                    "COALESCE((SELECT MAX(id) FROM master_unificado_virtuales), 1))"
                )
            )
        except Exception:
            pass  # Si la tabla no usa secuencia o falla, se intenta el create igualmente
        
        # Crear nuevo registro copiando todos los campos pero con impo_expo="IMPO"
        nuevo_impo = await crud.create_master_unificado_virtuales(
            db=db,
            solicitud_previo=registro_expo.solicitud_previo,
            agente=registro_expo.agente,
            pedimento=registro_expo.pedimento,
            aduana=registro_expo.aduana,
            patente=registro_expo.patente,
            destino=registro_expo.destino,
            cliente_space=registro_expo.cliente_space,
            impo_expo="IMPO",
            proveedor_cliente=registro_expo.proveedor_cliente,
            mes=registro_expo.mes,
            complemento=registro_expo.complemento,
            tipo_immex=registro_expo.tipo_immex,
            factura=registro_expo.factura,
            fecha_pago=registro_expo.fecha_pago,
            informacion=registro_expo.informacion,
            estatus=registro_expo.estatus,
            op_regular=registro_expo.op_regular,
            tipo=registro_expo.tipo,
            numero=registro_expo.numero,
            carretes=registro_expo.carretes,
            servicio_cliente=registro_expo.servicio_cliente,
            plazo=registro_expo.plazo,
            firma=registro_expo.firma,
            incoterm=registro_expo.incoterm,
            tipo_exportacion=registro_expo.tipo_exportacion,
            escenario=registro_expo.escenario,
            user_id=current_user.id
        )
        
        # Actualizar el comentario del historial para indicar que es una duplicación
        from app.db.models import MasterUnificadoVirtualHistorial
        from sqlalchemy import select, desc
        historial_mas_reciente = await db.execute(
            select(MasterUnificadoVirtualHistorial)
            .where(MasterUnificadoVirtualHistorial.numero == nuevo_impo.numero)
            .order_by(desc(MasterUnificadoVirtualHistorial.created_at))
            .limit(1)
        )
        historial = historial_mas_reciente.scalar_one_or_none()
        if historial:
            historial.comentario = f"Duplicado desde registro EXPO (número {numero})"
            await db.commit()
        
        return JSONResponse(
            status_code=201,
            content={
                "success": True,
                "message": "Registro duplicado como IMPO correctamente",
                "numero": nuevo_impo.numero
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Error al duplicar el registro: {str(e)}"}
        )


@app.post("/api/virtuales/actualizar-desde-excel")
async def actualizar_virtuales_desde_excel(
    archivo: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Lee un Excel con columnas Codigo cliente, Impo/Expo, Mes y opcionales patente, aduana, complemento, firma.
    Para cada fila busca el registro en master_unificado_virtuales por (numero, impo_expo, mes) y actualiza
    patente, aduana, complemento y firma. Los cambios se registran en el historial.
    """
    import tempfile
    import shutil
    from pathlib import Path
    import pandas as pd

    if not archivo.filename:
        return JSONResponse(status_code=400, content={"error": "No se envió ningún archivo"})
    ext = Path(archivo.filename).suffix.lower()
    if ext not in (".xlsx", ".xls"):
        return JSONResponse(
            status_code=400,
            content={"error": "El archivo debe ser Excel (.xlsx o .xls)"},
        )

    def _norm(s):
        if s is None or (isinstance(s, float) and pd.isna(s)):
            return ""
        return str(s).strip().lower().replace(" ", "_").replace("/", "_")

    def _find_col(df, opciones):
        cols_lower = {_norm(c): c for c in df.columns}
        for op in opciones:
            if _norm(op) in cols_lower:
                return cols_lower[_norm(op)]
        return None

    def _int_or_none(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return None

    def _str_or_none(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        return s if s else None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            shutil.copyfileobj(archivo.file, tmp)
            tmp_path = tmp.name
        try:
            engine = "openpyxl" if ext == ".xlsx" else "xlrd"
            df = pd.read_excel(tmp_path, engine=engine)
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

        col_codigo = _find_col(df, ["Codigo cliente", "codigo cliente", "numero", "codigo_cliente", "número", "numero de cliente"])
        col_impo = _find_col(df, ["Impo/Expo", "impo/expo", "impo_expo", "impo expo", "tipo"])
        col_mes = _find_col(df, ["Mes", "mes"])
        col_patente = _find_col(df, ["patente", "Patente"])
        col_aduana = _find_col(df, ["aduana", "Aduana"])
        col_complemento = _find_col(df, ["complemento", "Complemento"])
        col_firma = _find_col(df, ["firma", "Firma"])

        if not col_codigo or not col_impo or not col_mes:
            return JSONResponse(
                status_code=400,
                content={
                    "error": "El Excel debe tener las columnas: Codigo cliente, Impo/Expo y Mes. No se encontraron todas."
                },
            )

        actualizados = 0
        no_encontrados = []

        for idx, row in df.iterrows():
            codigo = _int_or_none(row.get(col_codigo))
            if codigo is None:
                continue
            impo_expo_raw = _str_or_none(row.get(col_impo))
            if not impo_expo_raw:
                continue
            impo_expo = impo_expo_raw.strip().upper()
            if impo_expo not in ("IMPO", "EXPO"):
                continue
            mes_raw = _str_or_none(row.get(col_mes))
            if not mes_raw:
                continue
            mes = mes_raw.strip()

            patente = _int_or_none(row.get(col_patente)) if col_patente else None
            aduana = _int_or_none(row.get(col_aduana)) if col_aduana else None
            complemento = _str_or_none(row.get(col_complemento)) if col_complemento else None
            firma = _str_or_none(row.get(col_firma)) if col_firma else None

            if patente is None and aduana is None and complemento is None and firma is None:
                continue

            master = await crud.update_master_unificado_virtuales_campos_desde_excel(
                db,
                numero=codigo,
                impo_expo=impo_expo,
                mes=mes,
                user_id=current_user.id,
                patente=patente,
                aduana=aduana,
                complemento=complemento,
                firma=firma,
            )
            if master:
                actualizados += 1
            else:
                no_encontrados.append({"fila": int(idx) + 2, "codigo": codigo, "impo_expo": impo_expo, "mes": mes})

        await db.commit()

        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": f"Se actualizaron {actualizados} registro(s) desde el Excel.",
                "actualizados": actualizados,
                "no_encontrados": no_encontrados,
            },
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"error": f"Error al procesar el Excel: {str(e)}"},
        )


@app.post("/api/virtuales/crear-carpeta")
async def crear_carpeta_virtual(
    request: Request,
    current_user: User = Depends(get_current_user),
):
    """Crea una carpeta en el Desktop con nombre [codigo] [nombre] [impo/expo] [mes]."""
    import re
    try:
        data = await request.json()
        numero = (data.get("numero") or "")
        nombre = (data.get("proveedor_cliente") or "").strip()
        impo_expo = (data.get("impo_expo") or "").strip()
        mes = (data.get("mes") or "").strip()
        # Nombre de carpeta: cu_[codigo] [nombre] [impo/expo] [mes]_erob1001
        parte_media = f"{numero} {nombre} {impo_expo} {mes}".strip()
        # Sanitizar para filesystem: quitar caracteres no válidos \ / : * ? " < > |
        parte_media = re.sub(r'[\\/:*?"<>|]', " ", parte_media)
        parte_media = re.sub(r"\s+", " ", parte_media).strip()
        nombre_carpeta = f"cu_{parte_media}_erob1001" if parte_media else ""
        if not nombre_carpeta:
            return JSONResponse(
                status_code=400,
                content={"error": "No se pudo generar un nombre de carpeta válido"}
            )
        desktop = os.path.expanduser("~/Desktop")
        ruta_carpeta = os.path.join(desktop, nombre_carpeta)
        if os.path.isdir(ruta_carpeta):
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": "La carpeta ya existe",
                    "ruta": ruta_carpeta,
                    "nombre_carpeta": nombre_carpeta,
                    "ya_existia": True,
                }
            )
        os.makedirs(ruta_carpeta, exist_ok=False)
        return JSONResponse(
            status_code=201,
            content={
                "success": True,
                "message": "Carpeta creada correctamente",
                "ruta": ruta_carpeta,
                "nombre_carpeta": nombre_carpeta,
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Error al crear la carpeta: {str(e)}"}
        )


@app.put("/api/paises-origen/{pais_id}")
async def actualizar_pais_origen(
    pais_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Actualiza un país de origen de material."""
    try:
        data = await request.json()
        pais_origen = data.get("pais_origen")
        
        if not pais_origen:
            return JSONResponse(
                status_code=400,
                content={"error": "El campo 'pais_origen' es requerido"}
            )
        
        pais_actualizado = await crud.update_pais_origen_material(
            db=db,
            pais_id=pais_id,
            pais_origen=pais_origen,
            user_id=current_user.id
        )
        
        if not pais_actualizado:
            return JSONResponse(
                status_code=404,
                content={"error": "País de origen no encontrado"}
            )
        
        return JSONResponse({
            "success": True,
            "message": "País de origen actualizado exitosamente",
            "pais": {
                "id": pais_actualizado.id,
                "codigo_proveedor": pais_actualizado.codigo_proveedor,
                "numero_material": pais_actualizado.numero_material,
                "pais_origen": pais_actualizado.pais_origen
            }
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "message": f"Error al actualizar país de origen: {str(e)}"
            }
        )


@app.get("/api/paises-origen/historial")
async def api_paises_origen_historial(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Devuelve los últimos 15 movimientos del historial de países de origen."""
    historial = await crud.list_pais_origen_material_historial(
        db,
        limit=15,
        offset=0
    )
    items = []
    for h in historial:
        items.append({
            "id": h.id,
            "pais_origen_id": h.pais_origen_id,
            "codigo_proveedor": h.codigo_proveedor,
            "numero_material": h.numero_material,
            "operacion": h.operacion.value,
            "user_email": h.user.email if h.user else None,
            "user_nombre": h.user.nombre if h.user else None,
            "datos_antes": h.datos_antes,
            "datos_despues": h.datos_despues,
            "campos_modificados": h.campos_modificados,
            "created_at": h.created_at.isoformat() if h.created_at else None,
        })
    return JSONResponse({"movimientos": items})


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


@app.post("/api/materiales/actualizar")
async def actualizar_materiales_desde_compras(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Sincroniza materiales desde la tabla compras.
    
    Busca todos los numero_material únicos en compras que no estén registrados
    en materiales y los crea automáticamente usando la descripcion_material de compras.
    """
    try:
        resultado = await crud.sincronizar_materiales_desde_compras(
            db=db,
            user_id=current_user.id
        )
        
        # Determinar el mensaje según el resultado
        if resultado["nuevos_creados"] > 0:
            mensaje = f"✓ Sincronización completada. Se crearon {resultado['nuevos_creados']} nuevo(s) material(es) de {resultado['total_encontrados']} encontrados en compras."
        elif resultado["total_encontrados"] > 0:
            mensaje = f"✓ Sincronización completada. Todos los materiales ({resultado['total_encontrados']}) ya están registrados."
        else:
            mensaje = "✓ Sincronización completada. No se encontraron materiales en la tabla de compras."
        
        if resultado["errores"]:
            mensaje += f" Se encontraron {len(resultado['errores'])} error(es)."
        
        # Considerar exitoso si no hay errores críticos
        success = len(resultado["errores"]) == 0 or resultado["nuevos_creados"] > 0
        
        return JSONResponse({
            "success": success,
            "total_encontrados": resultado["total_encontrados"],
            "nuevos_creados": resultado["nuevos_creados"],
            "errores": resultado["errores"],
            "mensaje": mensaje
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "mensaje": f"Error al sincronizar materiales desde compras: {str(e)}"
            }
        )


@app.post("/api/proveedores/actualizar")
async def actualizar_proveedores_desde_compras(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Sincroniza proveedores desde la tabla compras.
    
    Busca todos los codigo_proveedor únicos en compras que no estén registrados
    en proveedores y los crea automáticamente usando el nombre_proveedor de compras.
    """
    try:
        resultado = await crud.sincronizar_proveedores_desde_compras(
            db=db,
            user_id=current_user.id
        )
        
        # Determinar el mensaje según el resultado
        if resultado["nuevos_creados"] > 0:
            mensaje = f"✓ Sincronización completada. Se crearon {resultado['nuevos_creados']} nuevo(s) proveedor(es) de {resultado['total_encontrados']} encontrados en compras."
        elif resultado["total_encontrados"] > 0:
            mensaje = f"✓ Sincronización completada. Todos los proveedores ({resultado['total_encontrados']}) ya están registrados."
        else:
            mensaje = "✓ Sincronización completada. No se encontraron proveedores en la tabla de compras."
        
        if resultado["errores"]:
            mensaje += f" Se encontraron {len(resultado['errores'])} error(es)."
        
        # Considerar exitoso si no hay errores críticos
        success = len(resultado["errores"]) == 0 or resultado["nuevos_creados"] > 0
        
        return JSONResponse({
            "success": success,
            "total_encontrados": resultado["total_encontrados"],
            "nuevos_creados": resultado["nuevos_creados"],
            "errores": resultado["errores"],
            "mensaje": mensaje
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "mensaje": f"Error al sincronizar proveedores desde compras: {str(e)}"
            }
        )


@app.post("/api/proveedores/actualizar-estatus")
async def actualizar_estatus_proveedores(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Actualiza el estatus_compras de proveedores basándose en compras y fecha de creación.
    
    Reglas:
    - Cliente existente con compras en los últimos 6 meses → estatus_compras = "activo"
    - Cliente existente sin compras en los últimos 6 meses → estatus_compras = "baja"
    - Cliente nuevo creado este mes → estatus_compras = "alta"
    """
    try:
        resultado = await crud.actualizar_estatus_proveedores_por_compras(
            db=db,
            user_id=current_user.id
        )
        
        # Construir mensaje detallado
        partes_mensaje = []
        if resultado["proveedores_marcados_baja"] > 0:
            partes_mensaje.append(f"{resultado['proveedores_marcados_baja']} como baja")
        if resultado["proveedores_marcados_activo"] > 0:
            partes_mensaje.append(f"{resultado['proveedores_marcados_activo']} como activo")
        if resultado["proveedores_marcados_alta"] > 0:
            partes_mensaje.append(f"{resultado['proveedores_marcados_alta']} como alta")
        
        if partes_mensaje:
            mensaje = f"✓ Actualización completada. Se actualizaron {resultado['proveedores_actualizados']} proveedor(es): {', '.join(partes_mensaje)} de {resultado['total_proveedores']} totales."
        else:
            mensaje = f"✓ Actualización completada. No se requirieron cambios en los {resultado['total_proveedores']} proveedores."
        
        if resultado["errores"]:
            mensaje += f" Se encontraron {len(resultado['errores'])} error(es)."
        
        # Considerar exitoso si no hay errores críticos
        success = len(resultado["errores"]) == 0
        
        return JSONResponse({
            "success": success,
            "total_proveedores": resultado["total_proveedores"],
            "proveedores_actualizados": resultado["proveedores_actualizados"],
            "proveedores_marcados_baja": resultado["proveedores_marcados_baja"],
            "proveedores_marcados_activo": resultado["proveedores_marcados_activo"],
            "proveedores_marcados_alta": resultado["proveedores_marcados_alta"],
            "errores": resultado["errores"],
            "mensaje": mensaje
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "mensaje": f"Error al actualizar estatus de proveedores: {str(e)}"
            }
        )


@app.post("/api/paises-origen/actualizar")
async def actualizar_paises_origen_desde_compras(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Sincroniza países de origen desde la tabla compras.
    
    Busca combinaciones únicas de codigo_proveedor y numero_material en compras
    que no estén registradas en pais_origen_material y las crea con "Pendiente".
    """
    try:
        resultado = await crud.sincronizar_paises_origen_desde_compras(
            db=db,
            user_id=current_user.id
        )
        
        # Determinar el mensaje según el resultado
        if resultado["nuevos_creados"] > 0:
            mensaje = (
                f"✓ Sincronización completada. Se crearon {resultado['nuevos_creados']} "
                f"registro(s) de {resultado['total_encontrados']} encontrados en compras."
            )
        elif resultado["total_encontrados"] > 0:
            mensaje = (
                f"✓ Sincronización completada. Todos los registros "
                f"({resultado['total_encontrados']}) ya están registrados."
            )
        else:
            mensaje = "✓ Sincronización completada. No se encontraron datos válidos en compras."
        
        if resultado["errores"]:
            mensaje += f" Se encontraron {len(resultado['errores'])} error(es)."
        
        # Considerar exitoso si no hay errores críticos
        success = len(resultado["errores"]) == 0 or resultado["nuevos_creados"] > 0
        
        return JSONResponse({
            "success": success,
            "total_encontrados": resultado["total_encontrados"],
            "nuevos_creados": resultado["nuevos_creados"],
            "errores": resultado["errores"],
            "mensaje": mensaje
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "mensaje": f"Error al sincronizar países de origen desde compras: {str(e)}"
            }
        )


@app.get("/api/compras")
async def api_compras(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    search: Optional[str] = None,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    codigo_proveedor: Optional[str] = None,
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
            # Agregar zona horaria si no la tiene
            if fecha_inicio_dt.tzinfo is None:
                fecha_inicio_dt = fecha_inicio_dt.replace(tzinfo=ZoneInfo('America/Mexico_City'))
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": "Formato de fecha_inicio inválido. Use formato YYYY-MM-DD"}
            )
    
    if fecha_fin:
        try:
            fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
            # Agregar zona horaria si no la tiene
            if fecha_fin_dt.tzinfo is None:
                fecha_fin_dt = fecha_fin_dt.replace(tzinfo=ZoneInfo('America/Mexico_City'))
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
        codigo_proveedor=codigo_proveedor,
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
        codigo_proveedor=codigo_proveedor,
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
                "invoice_value": float(c.invoice_value) if c.invoice_value else None,
                "numero_material": c.numero_material,
                "plant": c.plant,
                "descripcion_material": c.descripcion_material,
                "nombre_proveedor": c.nombre_proveedor,
                "codigo_proveedor": c.codigo_proveedor,
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
            
            # Buscar la columna "Supplier" en el Archivo 2 para codigo_proveedor
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
            
            # Buscar columna "Proveedor" original en Archivo 1 para codigo_proveedor (antes del cruce)
            # Esta columna puede contener el número del proveedor
            columna_proveedor_numero = None
            for col in df_archivo1.columns:
                if str(col).strip().lower() == "proveedor":
                    columna_proveedor_numero = col
                    break
            
            # Guardar los valores originales de "Proveedor" para codigo_proveedor si existe
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
            
            # Crear columna para codigo_proveedor (Supplier del archivo 2)
            df_archivo1["Supplier_Numero"] = None
            
            # Crear diccionarios de mapeo desde Archivo 2
            # Clave: Purchasing Document (normalizado), Valor: Name 1 (para nombre_proveedor)
                    # Clave: Purchasing Document (normalizado), Valor: Supplier (para codigo_proveedor)
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
                    
                    # Mapeo para codigo_proveedor (Supplier)
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
                    
                    # Buscar coincidencia en el mapa para codigo_proveedor (Supplier)
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
                    'codigo_proveedor': ['Supplier_Numero', 'Proveedor_Numero_Original', 'Proveedor', 'proveedor', 'PROVEEDOR', 'codigo_proveedor'],
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
                    elif campo_db == 'codigo_proveedor':
                        # codigo_proveedor es un string, convertir a string directamente
                        if valor is not None and not pd.isna(valor):
                            compra_data[campo_db] = str(valor).strip()
                        else:
                            compra_data[campo_db] = None
                    elif campo_db == 'posting_date':
                        compra_data[campo_db] = convertir_valor(valor, 'date')
                    elif campo_db in ['amount_in_lc', 'amount', 'gr_ir_clearing_value_lc', 'invoice_value', 'price']:
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

    col_inv = next((c for c in df.columns if str(c).strip().lower() == 'amount'), None)
    col_qty = next((c for c in df.columns if str(c).strip().lower() == 'quantity'), None)
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
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Procesa historial de compras, enriquece con Compras (Supplier->codigo proveedor, Name 1->nombre proveedor) e inserta en la tabla compras."""
    import tempfile
    import shutil
    from pathlib import Path
    import pandas as pd
    from decimal import Decimal
    from datetime import datetime as dt

    ext_ok = ['.xlsx', '.xls']
    n_compras = archivo_compras.filename or ''
    n_hist = archivo_historial.filename or ''
    e_compras = Path(n_compras).suffix.lower()
    e_hist = Path(n_hist).suffix.lower()

    if e_compras not in ext_ok or e_hist not in ext_ok:
        return JSONResponse(status_code=400, content={"error": "Ambos archivos deben ser Excel (.xlsx o .xls)"})

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

            # Buscar todas las columnas necesarias una sola vez
            col_pd_map = col(df, 'Purchasing Document')
            col_item = col(df, 'Item')
            col_mdy = col(df, 'Material Doc. Year')
            col_md = col(df, 'Material Document')
            col_mdi = col(df, 'Material Doc.Item')
            col_mt = col(df, 'Movement Type')
            col_post = col(df, 'Posting Date')
            col_qty = col(df, 'Quantity')
            col_ou = col(df, 'Order Unit')
            col_qop = col(df, 'Quantity in OPUn')
            col_opu = col(df, 'Order Price Unit')
            col_alc = col(df, 'Amount in LC')
            col_lc = col(df, 'Local currency')
            col_amt = col(df, 'Amount')
            col_curr = col(df, 'Currency')
            col_grir = col(df, 'GR/IR clearing value in local currency')
            col_inv = col(df, 'Invoice Value')
            col_mat = col(df, 'Material')
            col_plant = col(df, 'Plant')
            # Buscar "Short Text" primero, luego otras variantes
            col_desc = col(df, 'Short Text') or col(df, 'descripcion_material') or col(df, 'Material Description') or col(df, 'Short text')
            col_nom_prov = col(df, 'nombre proveedor')
            col_cod_prov = col(df, 'codigo proveedor')
            col_price = col(df, 'precio unitario')

            # Funciones auxiliares para conversión segura
            def safe_int(val):
                if pd.isna(val) or val == '' or val is None:
                    return None
                try:
                    return int(pd.to_numeric(val, errors='raise'))
                except (ValueError, TypeError):
                    return None

            def safe_decimal(val):
                if pd.isna(val) or val == '' or val is None:
                    return None
                try:
                    return Decimal(str(val))
                except (ValueError, TypeError):
                    return None

            def safe_str(val):
                if pd.isna(val) or val == '' or val is None:
                    return None
                return str(val).strip()

            def safe_date(val):
                if pd.isna(val) or val == '' or val is None:
                    return None
                try:
                    # Convertir a datetime si es necesario
                    if isinstance(val, pd.Timestamp):
                        dt_val = val.to_pydatetime()
                    elif isinstance(val, dt):
                        dt_val = val
                    else:
                        dt_val = pd.to_datetime(val).to_pydatetime()
                    
                    # Si no tiene zona horaria, agregar zona horaria de México (CDMX)
                    if dt_val.tzinfo is None:
                        dt_val = dt_val.replace(tzinfo=ZoneInfo('America/Mexico_City'))
                    
                    return dt_val
                except Exception:
                    return None

            # Convertir DataFrame a lista de diccionarios
            compras_data = []
            for _, row in df.iterrows():
                compra_dict = {
                    'purchasing_document': safe_int(row[col_pd_map]) if col_pd_map else None,
                    'item': safe_int(row[col_item]) if col_item else None,
                    'material_doc_year': safe_int(row[col_mdy]) if col_mdy else None,
                    'material_document': safe_int(row[col_md]) if col_md else None,
                    'material_doc_item': safe_int(row[col_mdi]) if col_mdi else None,
                    'movement_type': safe_str(row[col_mt]) if col_mt else None,
                    'posting_date': safe_date(row[col_post]) if col_post else None,
                    'quantity': safe_int(row[col_qty]) if col_qty else None,
                    'order_unit': safe_str(row[col_ou]) if col_ou else None,
                    'quantity_in_opun': safe_int(row[col_qop]) if col_qop else None,
                    'order_price_unit': safe_str(row[col_opu]) if col_opu else None,
                    'amount_in_lc': safe_decimal(row[col_alc]) if col_alc else None,
                    'local_currency': safe_str(row[col_lc]) if col_lc else None,
                    'amount': safe_decimal(row[col_amt]) if col_amt else None,
                    'currency': safe_str(row[col_curr]) if col_curr else None,
                    'gr_ir_clearing_value_lc': safe_decimal(row[col_grir]) if col_grir else None,
                    'invoice_value': safe_decimal(row[col_inv]) if col_inv else None,
                    'numero_material': safe_str(row[col_mat]) if col_mat else None,
                    'plant': safe_str(row[col_plant]) if col_plant else None,
                    'descripcion_material': safe_str(row[col_desc]) if col_desc else None,
                    'nombre_proveedor': safe_str(row[col_nom_prov]) if col_nom_prov else None,
                    'codigo_proveedor': safe_str(row[col_cod_prov]) if col_cod_prov and pd.notna(row[col_cod_prov]) else None,
                    'price': safe_decimal(row[col_price]) if col_price else None,
                }
                # Solo agregar si tiene al menos purchasing_document o numero_material
                if compra_dict.get('purchasing_document') or compra_dict.get('numero_material'):
                    compras_data.append(compra_dict)

            # Insertar o actualizar en la base de datos
            if compras_data:
                resultado = await crud.bulk_create_or_update_compras(db, compras_data)
                return JSONResponse(
                    status_code=200,
                    content={
                        "success": True,
                        "message": f"Procesamiento completado: {resultado['insertados']} registros insertados, {resultado['actualizados']} registros actualizados",
                        "insertados": resultado['insertados'],
                        "actualizados": resultado['actualizados']
                    }
                )
            else:
                return JSONResponse(
                    status_code=400,
                    content={"error": "No se encontraron datos válidos para insertar"}
                )

    except Exception as e:
        import traceback
        return JSONResponse(
            status_code=500,
            content={"error": f"Error al procesar: {str(e)}\n{traceback.format_exc()}"}
        )


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
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Procesa un archivo Excel (Reporte de ventas) y sube los datos a la base de datos."""
    import tempfile
    import shutil
    import os
    from pathlib import Path
    import pandas as pd
    import traceback
    
    execution = None
    ahora_utc = None
    try:
        # Validar que sea un archivo Excel
        extensiones_permitidas = ['.xlsx', '.xls']
        
        nombre_archivo_ventas = archivo_ventas.filename or ''
        
        extension_ventas = Path(nombre_archivo_ventas).suffix.lower()
        
        if extension_ventas not in extensiones_permitidas:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": f"El archivo de Reporte de ventas debe ser un archivo Excel (.xlsx o .xls). El archivo proporcionado tiene la extensión: {extension_ventas}"
                }
            )
        
        
        # Crear directorio temporal para el archivo
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Guardar archivo temporalmente
            archivo_ventas_path = temp_path / f"ventas_{nombre_archivo_ventas}"
            
            with open(archivo_ventas_path, "wb") as f:
                shutil.copyfileobj(archivo_ventas.file, f)
            
            # Leer archivo Excel
            try:
                if extension_ventas == '.xlsx':
                    df = pd.read_excel(archivo_ventas_path, engine='openpyxl', header=None)
                else:
                    df = pd.read_excel(archivo_ventas_path, engine='xlrd', header=None)
            except Exception as e:
                import traceback
                error_msg = str(e)
                error_traceback = traceback.format_exc()
                mensaje = f"Error al leer el archivo Excel: {error_msg}"
                
                if "No such file" in error_msg or "not found" in error_msg.lower():
                    mensaje = "Error: No se pudo encontrar o acceder al archivo Excel. Verifica que el archivo exista y no esté corrupto."
                elif "password" in error_msg.lower() or "encrypted" in error_msg.lower():
                    mensaje = "Error: El archivo Excel está protegido con contraseña. Por favor, elimina la protección y vuelve a intentar."
                elif "format" in error_msg.lower() or "invalid" in error_msg.lower():
                    mensaje = "Error: El formato del archivo Excel no es válido. Asegúrate de que sea un archivo .xlsx o .xls válido."
                elif "openpyxl" in error_msg.lower() or "xlrd" in error_msg.lower():
                    mensaje = "Error: No se pudo leer el archivo Excel. Verifica que el archivo no esté corrupto y que tenga el formato correcto."
                
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": mensaje,
                        "detalle_tecnico": error_msg,
                        "tipo_error": type(e).__name__,
                        "traceback": error_traceback
                    }
                )
            
            # Validar que el archivo tenga al menos 3 filas (para poder copiar del renglón 1 al 3)
            if len(df) < 3:
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": f"El archivo Excel debe tener al menos 3 filas para poder procesarse correctamente. El archivo actual tiene {len(df)} fila(s)."
                    }
                )
            
            # Registrar evento en historial de ejecuciones
            from datetime import timezone as tz
            ahora_utc = datetime.now(tz.utc)
            execution = await crud.create_sales_execution(
                db=db,
                user_id=current_user.id,
                fecha_inicio_periodo=ahora_utc,
                fecha_fin_periodo=ahora_utc,
                sistema_sap="Carga manual",
                transaccion="Procesar archivo",
                maquina=socket.gethostname() if hasattr(socket, 'gethostname') else (platform.node() or "unknown")
            )
            await crud.update_sales_execution_status(
                db=db,
                execution_id=execution.id,
                estado=ExecutionStatus.RUNNING,
                archivo_nombre=nombre_archivo_ventas
            )
            
            # Valores a buscar en el segundo renglón (índice 1)
            valores_a_buscar = ['OE', 'Budget', 'Act-Budget', 'incl. srap', '2005246']
            
            # Obtener el segundo renglón (índice 1)
            segundo_renglon = df.iloc[1]
            
            # Identificar columnas a eliminar
            columnas_a_eliminar = []
            for idx, valor in enumerate(segundo_renglon):
                # Convertir a string y limpiar espacios
                valor_str = str(valor).strip() if pd.notna(valor) else ''
                # Comparar con los valores a buscar (case-insensitive)
                for valor_buscar in valores_a_buscar:
                    if valor_str.lower() == valor_buscar.lower():
                        # Usar el índice de la columna
                        columnas_a_eliminar.append(idx)
                        break
            
            # Eliminar las columnas encontradas
            if columnas_a_eliminar:
                # Eliminar columnas por índice
                df = df.drop(df.columns[columnas_a_eliminar], axis=1)
            
            # Buscar columnas donde el segundo renglón (índice 1) tenga "Actual"
            # Obtener el segundo renglón actualizado después de eliminar columnas
            segundo_renglon_actualizado = df.iloc[1]
            columnas_actual = []
            
            for idx, valor in enumerate(segundo_renglon_actualizado):
                # Convertir a string y limpiar espacios
                valor_str = str(valor).strip() if pd.notna(valor) else ''
                # Comparar con "Actual" (case-insensitive)
                if valor_str.lower() == 'actual':
                    columnas_actual.append(idx)
            
            # Copiar el valor del renglón 1 (índice 0) al renglón 3 (índice 2) para las columnas "Actual"
            columnas_modificadas = 0
            for col_idx in columnas_actual:
                # Obtener el valor del renglón 1 (índice 0)
                valor_renglon_1 = df.iloc[0, col_idx]
                # Copiar al renglón 3 (índice 2)
                df.iloc[2, col_idx] = valor_renglon_1
                columnas_modificadas += 1
            
            # Eliminar los renglones 1 y 2 (índices 0 y 1)
            df = df.drop(df.index[0:2])
            # Resetear el índice para que quede secuencial
            df = df.reset_index(drop=True)
            
            # Guardar los valores del renglón 1 (índice 0) para usarlos como encabezado
            valores_renglon_1 = df.iloc[0].tolist()
            
            # Verificar si hay un renglón 2 y si tiene los mismos valores que el renglón 1
            if len(df) > 1:
                valores_renglon_2 = df.iloc[1].tolist()
                # Comparar los valores (considerando NaN como iguales)
                son_iguales = True
                for i in range(len(valores_renglon_1)):
                    val1 = valores_renglon_1[i]
                    val2 = valores_renglon_2[i]
                    # Comparar considerando NaN
                    if pd.isna(val1) and pd.isna(val2):
                        continue
                    elif pd.isna(val1) or pd.isna(val2):
                        son_iguales = False
                        break
                    elif str(val1).strip() != str(val2).strip():
                        son_iguales = False
                        break
                
                # Si son iguales, eliminar el renglón 2
                if son_iguales:
                    df = df.drop(df.index[1])
                    df = df.reset_index(drop=True)
            
            # Eliminar el renglón 1 (que ahora usaremos como encabezado)
            df = df.drop(df.index[0])
            df = df.reset_index(drop=True)
            
            # Buscar las columnas necesarias en los encabezados
            # Buscar "Customer"
            columna_customer = None
            # Buscar "Quantity OE/TO FT"
            columna_quantity_ft = None
            # Buscar "Quantity OE/TO M"
            columna_quantity_m = None
            # Buscar "Turnover w/o metal"
            columna_turnover_wo_metal_original = None
            # Buscar "OE/Turnover like FI"
            columna_turnover_like_fi_original = None
            
            for idx, encabezado in enumerate(valores_renglon_1):
                encabezado_str = str(encabezado).strip() if pd.notna(encabezado) else ''
                encabezado_lower = encabezado_str.lower()
                # Buscar "Customer"
                if columna_customer is None and 'customer' in encabezado_lower:
                    columna_customer = idx
                # Buscar "Quantity OE/TO FT"
                if columna_quantity_ft is None and ('quantity' in encabezado_lower and 'ft' in encabezado_lower and 
                    ('oe' in encabezado_lower or 'to' in encabezado_lower)):
                    columna_quantity_ft = idx
                # Buscar "Quantity OE/TO M" (sin FT, solo M)
                if columna_quantity_m is None and ('quantity' in encabezado_lower and 'm' in encabezado_lower and 
                    ('oe' in encabezado_lower or 'to' in encabezado_lower) and 'ft' not in encabezado_lower):
                    columna_quantity_m = idx
                # Buscar "Turnover w/o metal"
                if columna_turnover_wo_metal_original is None and ('turnover' in encabezado_lower and 'metal' in encabezado_lower and 'w/o' in encabezado_lower):
                    columna_turnover_wo_metal_original = idx
                # Buscar "OE/Turnover like FI"
                if columna_turnover_like_fi_original is None and ('turnover' in encabezado_lower and 'like' in encabezado_lower and 'fi' in encabezado_lower):
                    columna_turnover_like_fi_original = idx
            
            # Calcular el índice actual de "Turnover w/o metal" después de eliminar columnas
            columna_turnover_wo_metal = None
            if columna_turnover_wo_metal_original is not None:
                # Crear un mapeo de índices originales a índices actuales
                # Las columnas eliminadas están en columnas_a_eliminar
                # Necesitamos contar cuántas columnas antes de la columna objetivo fueron eliminadas
                columnas_eliminadas_antes = sum(1 for col_idx in columnas_a_eliminar if col_idx < columna_turnover_wo_metal_original)
                # El índice actual es el índice original menos las columnas eliminadas antes
                columna_turnover_wo_metal = columna_turnover_wo_metal_original - columnas_eliminadas_antes
                # Verificar que el índice esté dentro del rango del DataFrame
                if columna_turnover_wo_metal < 0 or columna_turnover_wo_metal >= len(df.columns):
                    columna_turnover_wo_metal = None
            
            # Calcular el índice actual de "OE/Turnover like FI" después de eliminar columnas
            columna_turnover_like_fi = None
            if columna_turnover_like_fi_original is not None:
                # Crear un mapeo de índices originales a índices actuales
                # Las columnas eliminadas están en columnas_a_eliminar
                # Necesitamos contar cuántas columnas antes de la columna objetivo fueron eliminadas
                columnas_eliminadas_antes = sum(1 for col_idx in columnas_a_eliminar if col_idx < columna_turnover_like_fi_original)
                # El índice actual es el índice original menos las columnas eliminadas antes
                columna_turnover_like_fi = columna_turnover_like_fi_original - columnas_eliminadas_antes
                # Verificar que el índice esté dentro del rango del DataFrame
                if columna_turnover_like_fi < 0 or columna_turnover_like_fi >= len(df.columns):
                    columna_turnover_like_fi = None
            
            # Obtener todos los grupos de clientes de la base de datos
            grupos_clientes = await crud.list_cliente_grupos(db, limit=100000)
            # Crear un diccionario que mapee codigo_cliente -> grupo
            dict_grupos = {}
            for grupo_cliente in grupos_clientes:
                if grupo_cliente.codigo_cliente is not None:
                    dict_grupos[grupo_cliente.codigo_cliente] = grupo_cliente.grupo or ''
            
            # Agregar la columna "Customer number" a la derecha de "Customer"
            if columna_customer is not None:
                # Obtener los valores de la columna Customer
                valores_customer = df.iloc[:, columna_customer]
                # Extraer los primeros 7 caracteres de cada valor
                customer_number = valores_customer.astype(str).str[:7]
                # Reemplazar 'nan' con string vacío
                customer_number = customer_number.replace('nan', '')
                # Insertar la columna justo después de Customer (posición columna_customer + 1)
                df.insert(columna_customer + 1, 'Customer number', customer_number.tolist())
                # Actualizar los índices de las otras columnas si Customer estaba antes de ellas
                if columna_quantity_ft is not None and columna_quantity_ft > columna_customer:
                    columna_quantity_ft += 1
                if columna_quantity_m is not None and columna_quantity_m > columna_customer:
                    columna_quantity_m += 1
                if columna_turnover_wo_metal is not None and columna_turnover_wo_metal > columna_customer:
                    columna_turnover_wo_metal += 1
                if columna_turnover_like_fi is not None and columna_turnover_like_fi > columna_customer:
                    columna_turnover_like_fi += 1
                # Actualizar valores_renglon_1 para incluir el nuevo encabezado
                valores_renglon_1.insert(columna_customer + 1, 'Customer number')
                
                # Agregar la columna "grupo" a la derecha de "Customer number"
                # Obtener los valores de la columna Customer number (ahora en posición columna_customer + 1)
                valores_customer_number = df.iloc[:, columna_customer + 1]
                # Convertir a entero para buscar en el diccionario, manejando errores
                grupos = []
                for valor in valores_customer_number:
                    try:
                        # Convertir a string y limpiar
                        valor_str = str(valor).strip()
                        # Verificar que no esté vacío ni sea 'nan'
                        if not valor_str or valor_str.lower() == 'nan' or valor_str == '':
                            grupos.append('')
                            continue
                        # Intentar convertir a entero
                        codigo = int(float(valor_str))
                        # Buscar el grupo en el diccionario
                        if codigo in dict_grupos:
                            grupos.append(dict_grupos[codigo])
                        else:
                            grupos.append('')
                    except (ValueError, TypeError):
                        grupos.append('')
                
                # Insertar la columna "grupo" justo después de Customer number (posición columna_customer + 2)
                df.insert(columna_customer + 2, 'grupo', grupos)
                # Actualizar los índices de las otras columnas si Customer estaba antes de ellas
                if columna_quantity_ft is not None and columna_quantity_ft > columna_customer:
                    columna_quantity_ft += 1
                if columna_quantity_m is not None and columna_quantity_m > columna_customer:
                    columna_quantity_m += 1
                if columna_turnover_wo_metal is not None and columna_turnover_wo_metal > columna_customer:
                    columna_turnover_wo_metal += 1
                if columna_turnover_like_fi is not None and columna_turnover_like_fi > columna_customer:
                    columna_turnover_like_fi += 1
                # Actualizar valores_renglon_1 para incluir el nuevo encabezado
                valores_renglon_1.insert(columna_customer + 2, 'grupo')
            
            # Agregar las 3 nuevas columnas
            num_filas = len(df)
            
            # Calcular "Conversion de FT a M" = Quantity OE/TO FT / 3.2808
            if columna_quantity_ft is not None:
                # Obtener los valores de la columna Quantity OE/TO FT
                valores_quantity_ft = df.iloc[:, columna_quantity_ft]
                # Convertir a numérico y dividir entre 3.2808
                valores_numericos_ft = pd.to_numeric(valores_quantity_ft, errors='coerce')
                conversion_ft_m = valores_numericos_ft / 3.2808
                df['Conversion de FT a M'] = conversion_ft_m
            else:
                # Si no se encuentra la columna, dejar vacío
                df['Conversion de FT a M'] = [None] * num_filas
            
            # Calcular "Sales total MTS" = Quantity OE/TO M + Conversion de FT a M
            if columna_quantity_m is not None:
                # Obtener los valores de la columna Quantity OE/TO M
                valores_quantity_m = df.iloc[:, columna_quantity_m]
                # Convertir a numérico
                valores_numericos_m = pd.to_numeric(valores_quantity_m, errors='coerce')
                # Obtener los valores de Conversion de FT a M (ya calculados)
                valores_conversion = pd.to_numeric(df['Conversion de FT a M'], errors='coerce')
                # Sumar ambos valores
                sales_total_mts = valores_numericos_m + valores_conversion
                df['Sales total MTS'] = sales_total_mts
            else:
                # Si no se encuentra la columna Quantity OE/TO M, usar solo Conversion de FT a M
                valores_conversion = pd.to_numeric(df['Conversion de FT a M'], errors='coerce')
                df['Sales total MTS'] = valores_conversion
            
            # Calcular "Sales KM" = Sales total MTS / 1000
            valores_sales_total = pd.to_numeric(df['Sales total MTS'], errors='coerce')
            sales_km = valores_sales_total / 1000
            df['Sales KM'] = sales_km
            
            # Buscar las columnas "Turnover w/o metal" y "OE/Turnover like FI" directamente en el DataFrame
            # después de todas las modificaciones, usando valores_renglon_1 que ya tiene las columnas insertadas
            columna_turnover_wo_metal_final = None
            columna_turnover_like_fi_final = None
            
            # Verificar que valores_renglon_1 tenga la misma longitud que las columnas del DataFrame
            num_columnas_df = len(df.columns)
            num_valores_renglon = len(valores_renglon_1)
            
            # Usar el mínimo para evitar errores de índice
            num_columnas_a_buscar = min(num_columnas_df, num_valores_renglon)
            
            for idx in range(num_columnas_a_buscar):
                encabezado = valores_renglon_1[idx] if idx < len(valores_renglon_1) else ''
                encabezado_str = str(encabezado).strip() if pd.notna(encabezado) else ''
                encabezado_lower = encabezado_str.lower()
                
                # Buscar "Turnover w/o metal" - debe tener "turnover", "metal" y "w/o" o "without", pero NO "like" ni "fi"
                # Patrón más específico: buscar "w/o" o "without" junto con "metal" pero sin "like" ni "fi"
                if columna_turnover_wo_metal_final is None:
                    has_turnover = 'turnover' in encabezado_lower
                    has_metal = 'metal' in encabezado_lower
                    has_wo = 'w/o' in encabezado_lower or 'without' in encabezado_lower
                    has_like = 'like' in encabezado_lower
                    has_fi = 'fi' in encabezado_lower
                    
                    if has_turnover and has_metal and has_wo and not has_like and not has_fi:
                        columna_turnover_wo_metal_final = idx
                
                # Buscar "OE/Turnover like FI" - debe tener "turnover", "like" y "fi"
                # Patrón más específico: buscar "like" y "fi" juntos, con "turnover"
                if columna_turnover_like_fi_final is None:
                    has_turnover = 'turnover' in encabezado_lower
                    has_like = 'like' in encabezado_lower
                    has_fi = 'fi' in encabezado_lower
                    
                    if has_turnover and has_like and has_fi:
                        columna_turnover_like_fi_final = idx
            
            # Calcular "Precio Exmetal KM" = Turnover w/o metal / Sales KM
            if columna_turnover_wo_metal_final is not None and 0 <= columna_turnover_wo_metal_final < len(df.columns):
                # Obtener los valores de la columna Turnover w/o metal
                valores_turnover = df.iloc[:, columna_turnover_wo_metal_final]
                # Convertir a numérico
                valores_numericos_turnover = pd.to_numeric(valores_turnover, errors='coerce')
                # Obtener los valores de Sales KM (ya calculados)
                valores_sales_km = pd.to_numeric(df['Sales KM'], errors='coerce')
                # Dividir: Precio Exmetal KM = Turnover w/o metal / Sales KM
                # Evitar división por cero
                precio_exmetal = valores_numericos_turnover / valores_sales_km.replace(0, pd.NA)
                df['Precio Exmetal KM'] = precio_exmetal
            else:
                # Si no se encuentra la columna, dejar vacío
                df['Precio Exmetal KM'] = [None] * num_filas
            
            # Calcular "Precio Full Metal KM" = OE/Turnover like FI / Sales KM
            if columna_turnover_like_fi_final is not None and 0 <= columna_turnover_like_fi_final < len(df.columns):
                # Verificar que no sea la misma columna que Turnover w/o metal
                if columna_turnover_like_fi_final != columna_turnover_wo_metal_final:
                    # Obtener los valores de la columna OE/Turnover like FI
                    valores_turnover_fi = df.iloc[:, columna_turnover_like_fi_final]
                    # Convertir a numérico
                    valores_numericos_turnover_fi = pd.to_numeric(valores_turnover_fi, errors='coerce')
                    # Obtener los valores de Sales KM (ya calculados)
                    valores_sales_km = pd.to_numeric(df['Sales KM'], errors='coerce')
                    # Dividir: Precio Full Metal KM = OE/Turnover like FI / Sales KM
                    # Evitar división por cero
                    precio_full_metal = valores_numericos_turnover_fi / valores_sales_km.replace(0, pd.NA)
                    df['Precio Full Metal KM'] = precio_full_metal
                else:
                    # Si es la misma columna, dejar vacío
                    df['Precio Full Metal KM'] = [None] * num_filas
            else:
                # Si no se encuentra la columna, dejar vacío
                df['Precio Full Metal KM'] = [None] * num_filas
            
            # Calcular "Precio Exmetal M" = Precio Exmetal KM / 1000
            valores_precio_exmetal_km = pd.to_numeric(df['Precio Exmetal KM'], errors='coerce')
            precio_exmetal_m = valores_precio_exmetal_km / 1000
            df['Precio Exmetal M'] = precio_exmetal_m
            
            # Calcular "Precio Full Metal M" = Precio Full Metal KM / 1000
            valores_precio_full_metal_km = pd.to_numeric(df['Precio Full Metal KM'], errors='coerce')
            precio_full_metal_m = valores_precio_full_metal_km / 1000
            df['Precio Full Metal M'] = precio_full_metal_m
            
            # Reemplazar NaN con string vacío en las columnas calculadas
            df['Conversion de FT a M'] = df['Conversion de FT a M'].fillna('')
            df['Sales total MTS'] = df['Sales total MTS'].fillna('')
            df['Sales KM'] = df['Sales KM'].fillna('')
            df['Precio Exmetal KM'] = df['Precio Exmetal KM'].fillna('')
            df['Precio Full Metal KM'] = df['Precio Full Metal KM'].fillna('')
            df['Precio Exmetal M'] = df['Precio Exmetal M'].fillna('')
            df['Precio Full Metal M'] = df['Precio Full Metal M'].fillna('')
            
            # Crear una fila de encabezados usando:
            # - Los valores del renglón 1 original para las columnas originales
            # - Los nombres de las nuevas columnas para las nuevas columnas
            nombres_encabezados = valores_renglon_1.copy()
            nombres_encabezados.extend(['Conversion de FT a M', 'Sales total MTS', 'Sales KM', 'Precio Exmetal KM', 'Precio Full Metal KM', 'Precio Exmetal M', 'Precio Full Metal M'])
            
            # Convertir los valores a strings para evitar problemas
            nombres_encabezados = [str(val) if pd.notna(val) else '' for val in nombres_encabezados]
            
            # Crear un DataFrame con una sola fila (los encabezados)
            fila_encabezados = pd.DataFrame([nombres_encabezados], columns=df.columns)
            # Concatenar la fila de encabezados al inicio del DataFrame
            df = pd.concat([fila_encabezados, df], ignore_index=True)
            
            # Actualizar y guardar los índices de las columnas según su nombre
            # Crear un diccionario que mapee el nombre de cada columna a su índice actualizado
            indices_columnas = {}
            for idx, nombre_columna in enumerate(df.columns):
                # Obtener el nombre real de la columna desde los encabezados (primera fila)
                nombre_real = str(df.iloc[0, idx]).strip() if pd.notna(df.iloc[0, idx]) else ''
                if nombre_real:
                    # Guardar el índice con el nombre de la columna (case-insensitive para búsqueda)
                    indices_columnas[nombre_real.lower()] = idx
                    # También guardar con el nombre exacto
                    indices_columnas[nombre_real] = idx
            
            # Actualizar los índices de las columnas conocidas después de todas las modificaciones
            # Buscar nuevamente las columnas en el DataFrame final para obtener sus índices actualizados
            columna_customer_actualizado = None
            columna_quantity_ft_actualizado = None
            columna_quantity_m_actualizado = None
            columna_customer_number_actualizado = None
            columna_grupo_actualizado = None
            columna_conversion_ft_m_actualizado = None
            columna_sales_total_mts_actualizado = None
            columna_sales_km_actualizado = None
            columna_turnover_wo_metal_actualizado = None
            columna_precio_exmetal_actualizado = None
            columna_turnover_like_fi_actualizado = None
            columna_precio_full_metal_actualizado = None
            columna_precio_exmetal_m_actualizado = None
            columna_precio_full_metal_m_actualizado = None
            
            # Buscar en la primera fila (encabezados) del DataFrame final
            primera_fila = df.iloc[0]
            for idx, encabezado in enumerate(primera_fila):
                encabezado_str = str(encabezado).strip() if pd.notna(encabezado) else ''
                encabezado_lower = encabezado_str.lower()
                
                # Buscar cada columna por nombre
                if columna_customer_actualizado is None and 'customer' in encabezado_lower and 'number' not in encabezado_lower:
                    columna_customer_actualizado = idx
                if columna_customer_number_actualizado is None and 'customer number' in encabezado_lower:
                    columna_customer_number_actualizado = idx
                if columna_grupo_actualizado is None and encabezado_lower == 'grupo':
                    columna_grupo_actualizado = idx
                if columna_quantity_ft_actualizado is None and ('quantity' in encabezado_lower and 'ft' in encabezado_lower and 
                    ('oe' in encabezado_lower or 'to' in encabezado_lower)):
                    columna_quantity_ft_actualizado = idx
                if columna_quantity_m_actualizado is None and ('quantity' in encabezado_lower and 'm' in encabezado_lower and 
                    ('oe' in encabezado_lower or 'to' in encabezado_lower) and 'ft' not in encabezado_lower):
                    columna_quantity_m_actualizado = idx
                if columna_conversion_ft_m_actualizado is None and 'conversion de ft a m' in encabezado_lower:
                    columna_conversion_ft_m_actualizado = idx
                if columna_sales_total_mts_actualizado is None and 'sales total mts' in encabezado_lower:
                    columna_sales_total_mts_actualizado = idx
                if columna_sales_km_actualizado is None and 'sales km' in encabezado_lower:
                    columna_sales_km_actualizado = idx
                if columna_turnover_wo_metal_actualizado is None and ('turnover' in encabezado_lower and 'metal' in encabezado_lower and 'w/o' in encabezado_lower):
                    columna_turnover_wo_metal_actualizado = idx
                if columna_precio_exmetal_actualizado is None and 'precio exmetal km' in encabezado_lower:
                    columna_precio_exmetal_actualizado = idx
                if columna_turnover_like_fi_actualizado is None and ('turnover' in encabezado_lower and 'like' in encabezado_lower and 'fi' in encabezado_lower):
                    columna_turnover_like_fi_actualizado = idx
                if columna_precio_full_metal_actualizado is None and 'precio full metal km' in encabezado_lower:
                    columna_precio_full_metal_actualizado = idx
                if columna_precio_exmetal_m_actualizado is None and 'precio exmetal m' in encabezado_lower:
                    columna_precio_exmetal_m_actualizado = idx
                if columna_precio_full_metal_m_actualizado is None and 'precio full metal m' in encabezado_lower:
                    columna_precio_full_metal_m_actualizado = idx
            
            # Crear diccionario con los índices actualizados de las columnas principales
            indices_columnas_principales = {
                'customer': columna_customer_actualizado,
                'customer_number': columna_customer_number_actualizado,
                'grupo': columna_grupo_actualizado,
                'quantity_ft': columna_quantity_ft_actualizado,
                'quantity_m': columna_quantity_m_actualizado,
                'conversion_ft_m': columna_conversion_ft_m_actualizado,
                'sales_total_mts': columna_sales_total_mts_actualizado,
                'sales_km': columna_sales_km_actualizado,
                'turnover_wo_metal': columna_turnover_wo_metal_actualizado,
                'precio_exmetal': columna_precio_exmetal_actualizado,
                'turnover_like_fi': columna_turnover_like_fi_actualizado,
                'precio_full_metal': columna_precio_full_metal_actualizado,
                'precio_exmetal_m': columna_precio_exmetal_m_actualizado,
                'precio_full_metal_m': columna_precio_full_metal_m_actualizado
            }
            
            # Modificar la columna Customer al final: eliminar los primeros 7 caracteres y espacios en blanco
            # Buscar la columna Customer en el DataFrame final
            columna_customer_final = None
            primera_fila_final = df.iloc[0]
            for idx, encabezado in enumerate(primera_fila_final):
                encabezado_str = str(encabezado).strip() if pd.notna(encabezado) else ''
                encabezado_lower = encabezado_str.lower()
                if columna_customer_final is None and 'customer' in encabezado_lower and 'number' not in encabezado_lower:
                    columna_customer_final = idx
                    break
            
            if columna_customer_final is not None:
                # Obtener todos los valores de la columna Customer (excluyendo la primera fila que es el encabezado)
                # Modificar desde la fila 1 en adelante (índice 1)
                for idx_fila in range(1, len(df)):
                    valor_original = df.iloc[idx_fila, columna_customer_final]
                    if pd.notna(valor_original):
                        # Convertir a string
                        valor_str = str(valor_original)
                        # Eliminar los primeros 7 caracteres
                        if len(valor_str) > 7:
                            valor_modificado = valor_str[7:]
                        else:
                            valor_modificado = ''
                        # Eliminar espacios en blanco al inicio y al final
                        valor_modificado = valor_modificado.strip()
                        # Actualizar el valor en el DataFrame
                        df.iloc[idx_fila, columna_customer_final] = valor_modificado
            
            # Descomponer la columna "Product" en "Producto" y "Product description"
            # Buscar la columna Product en el DataFrame final
            columna_product_final = None
            primera_fila_final = df.iloc[0]
            for idx, encabezado in enumerate(primera_fila_final):
                encabezado_str = str(encabezado).strip() if pd.notna(encabezado) else ''
                encabezado_lower = encabezado_str.lower()
                if columna_product_final is None and encabezado_lower == 'product':
                    columna_product_final = idx
                    break
            
            if columna_product_final is not None:
                # Crear listas para almacenar los valores descompuestos
                valores_producto = []
                valores_product_description = []
                
                # Procesar cada fila (incluyendo el encabezado)
                for idx_fila in range(len(df)):
                    valor_original = df.iloc[idx_fila, columna_product_final]
                    if pd.notna(valor_original):
                        # Convertir a string
                        valor_str = str(valor_original).strip()
                        # Dividir por el primer espacio
                        partes = valor_str.split(' ', 1)  # split con maxsplit=1 divide solo en el primer espacio
                        if len(partes) == 2:
                            # Hay dos partes: antes y después del primer espacio
                            valores_producto.append(partes[0])
                            # Eliminar espacios en blanco al inicio y al final de la descripción
                            descripcion_limpia = partes[1].strip()
                            valores_product_description.append(descripcion_limpia)
                        elif len(partes) == 1:
                            # Solo hay una parte (no hay espacio)
                            valores_producto.append(partes[0])
                            valores_product_description.append('')
                        else:
                            # Caso vacío
                            valores_producto.append('')
                            valores_product_description.append('')
                    else:
                        # Valor NaN
                        valores_producto.append('')
                        valores_product_description.append('')
                
                # Actualizar la columna Product con los valores de Producto
                df.iloc[:, columna_product_final] = valores_producto
                
                # Cambiar el nombre del encabezado de "Product" a "Producto" en la primera fila
                df.iloc[0, columna_product_final] = 'Producto'
                
                # Insertar la nueva columna "Product Description" justo después de Producto
                # Necesitamos insertar como una serie con el mismo índice que el DataFrame
                df.insert(columna_product_final + 1, 'Product Description', valores_product_description)
                
                # Asegurarse de que el encabezado en la primera fila sea "Product Description"
                df.iloc[0, columna_product_final + 1] = 'Product Description'
            
            # Mapear columnas del Excel a columnas de la BD
            # Primero, obtener los índices de todas las columnas necesarias desde los encabezados
            primera_fila = df.iloc[0]
            columnas_map = {}
            
            # Debug: imprimir todos los encabezados para ver qué hay disponible
            # print("Encabezados encontrados:", [str(h).strip() for h in primera_fila])
            
            # Buscar todas las columnas necesarias
            for idx, encabezado in enumerate(primera_fila):
                encabezado_str = str(encabezado).strip() if pd.notna(encabezado) else ''
                encabezado_lower = encabezado_str.lower()
                
                # Mapear según los nombres proporcionados
                if 'customer' in encabezado_lower and 'number' not in encabezado_lower:
                    columnas_map['Customer'] = idx
                elif 'customer number' in encabezado_lower:
                    columnas_map['Customer Number'] = idx
                elif encabezado_lower == 'grupo':
                    columnas_map['grupo'] = idx
                elif 'business unit' in encabezado_lower:
                    columnas_map['Business Unit'] = idx
                elif 'period' in encabezado_lower:
                    if 'Period/Year' not in columnas_map:  # Solo tomar la primera coincidencia
                        columnas_map['Period/Year'] = idx
                elif 'artnr condensed' in encabezado_lower:
                    columnas_map['Artnr condensed'] = idx
                elif 'region asc' in encabezado_lower:
                    columnas_map['Region ASC'] = idx
                elif encabezado_lower == 'plant':
                    columnas_map['Plant'] = idx
                elif 'ship' in encabezado_lower and 'party' in encabezado_lower:
                    if 'Shipt-to-party' not in columnas_map:  # Solo tomar la primera coincidencia
                        columnas_map['Shipt-to-party'] = idx
                elif encabezado_lower == 'producto':
                    columnas_map['Producto'] = idx
                elif 'product description' in encabezado_lower:
                    columnas_map['Product Description'] = idx
                elif 'turnover' in encabezado_lower and 'metal' in encabezado_lower and 'w/o' in encabezado_lower:
                    columnas_map['Turnover w/o metal'] = idx
                elif 'turnover' in encabezado_lower and 'like' in encabezado_lower and 'fi' in encabezado_lower:
                    columnas_map['OE/Turnover like FI'] = idx
                elif 'copper sales' in encabezado_lower and 'cuv' in encabezado_lower:
                    columnas_map['Copper Sales (CUV)'] = idx
                elif 'cu-sales effect' in encabezado_lower:
                    columnas_map['CU-Sales effect'] = idx
                elif 'cu result' in encabezado_lower:
                    columnas_map['CU result'] = idx
                elif 'quantity' in encabezado_lower and 'm' in encabezado_lower and 'ft' not in encabezado_lower:
                    columnas_map['Quantity OE/TO M'] = idx
                elif 'quantity' in encabezado_lower and 'ft' in encabezado_lower:
                    columnas_map['Quantity OE/TO FT'] = idx
                elif 'cu weight techn' in encabezado_lower and 'cut' in encabezado_lower:
                    columnas_map['CU Weight techn. CUT'] = idx
                elif 'cu weight sales' in encabezado_lower and 'cuv' in encabezado_lower:
                    columnas_map['CU weight Sales  CUV'] = idx
                elif 'conversion de ft a m' in encabezado_lower:
                    columnas_map['Conversion de FT a M'] = idx
                elif 'sales total mts' in encabezado_lower:
                    columnas_map['Sales total MTS'] = idx
                elif 'sales km' in encabezado_lower:
                    columnas_map['Sales KM'] = idx
                elif 'precio exmetal km' in encabezado_lower:
                    columnas_map['Precio Exmetal KM'] = idx
                elif 'precio full metal km' in encabezado_lower:
                    columnas_map['Precio Full Metal KM'] = idx
                elif 'precio exmetal m' in encabezado_lower:
                    columnas_map['Precio Exmetal M'] = idx
                elif 'precio full metal m' in encabezado_lower:
                    columnas_map['Precio Full Metal M'] = idx
            
            # Debug: verificar qué columnas se encontraron
            # print(f"Columnas encontradas: {list(columnas_map.keys())}")
            # if 'Period/Year' not in columnas_map:
            #     print("ADVERTENCIA: Columna Period/Year no encontrada")
            # if 'Shipt-to-party' not in columnas_map:
            #     print("ADVERTENCIA: Columna Shipt-to-party no encontrada")
            
            # Obtener todos los grupos de clientes para mapear grupo_id
            grupos_clientes = await crud.list_cliente_grupos(db, limit=100000)
            dict_grupos_cliente = {}
            dict_grupos_id = {}
            for grupo_cliente in grupos_clientes:
                if grupo_cliente.codigo_cliente is not None:
                    dict_grupos_cliente[grupo_cliente.codigo_cliente] = grupo_cliente.grupo or ''
                    # Crear un diccionario para buscar grupo_id por codigo_cliente y grupo
                    key = (grupo_cliente.codigo_cliente, grupo_cliente.grupo or '')
                    dict_grupos_id[key] = grupo_cliente.id
            
            # Procesar cada fila del DataFrame (excluyendo el encabezado)
            ventas_data = []
            from datetime import date as date_class
            from decimal import Decimal
            
            for idx_fila in range(1, len(df)):
                venta_dict = {}
                # Inicializar valores por defecto
                venta_dict['periodo'] = None
                venta_dict['ship_to_party'] = None
                
                # Cliente - Customer
                if 'Customer' in columnas_map:
                    cliente_val = df.iloc[idx_fila, columnas_map['Customer']]
                    venta_dict['cliente'] = str(cliente_val).strip() if pd.notna(cliente_val) else None
                
                # Codigo_cliente - Customer Number
                if 'Customer Number' in columnas_map:
                    codigo_val = df.iloc[idx_fila, columnas_map['Customer Number']]
                    try:
                        if pd.notna(codigo_val):
                            codigo_str = str(codigo_val).strip()
                            if codigo_str and codigo_str.lower() != 'nan':
                                venta_dict['codigo_cliente'] = int(float(codigo_str))
                            else:
                                venta_dict['codigo_cliente'] = None
                        else:
                            venta_dict['codigo_cliente'] = None
                    except (ValueError, TypeError):
                        venta_dict['codigo_cliente'] = None
                
                # Grupo - buscar grupo_id basado en codigo_cliente y grupo
                grupo_id = None
                if 'grupo' in columnas_map and venta_dict.get('codigo_cliente'):
                    grupo_val = df.iloc[idx_fila, columnas_map['grupo']]
                    grupo_str = str(grupo_val).strip() if pd.notna(grupo_val) else ''
                    if grupo_str and grupo_str.lower() != 'nan':
                        key = (venta_dict['codigo_cliente'], grupo_str)
                        grupo_id = dict_grupos_id.get(key)
                venta_dict['grupo_id'] = grupo_id
                
                # Unidad_negocio - Business Unit
                if 'Business Unit' in columnas_map:
                    unidad_val = df.iloc[idx_fila, columnas_map['Business Unit']]
                    venta_dict['unidad_negocio'] = str(unidad_val).strip() if pd.notna(unidad_val) else None
                
                # Periodo - Period/Year (convertir a Date)
                if 'Period/Year' in columnas_map:
                    periodo_val = df.iloc[idx_fila, columnas_map['Period/Year']]
                    venta_dict['periodo'] = None  # Valor por defecto
                    if pd.notna(periodo_val):
                        periodo_str = str(periodo_val).strip()
                        if periodo_str and periodo_str.lower() not in ['nan', 'none', '']:
                            try:
                                import re
                                
                                # Buscar año de 4 dígitos (2020-2099)
                                año_match = re.search(r'\b(20\d{2})\b', periodo_str)
                                año = None
                                if año_match:
                                    año = int(año_match.group(1))
                                
                                # Buscar mes (1-12) - puede estar antes o después del año
                                # Buscar números de 1-2 dígitos que representen meses válidos
                                mes_match = None
                                
                                # Patrón 1: Buscar "Period YYYY" seguido de un número que podría ser el mes
                                # Ejemplo: "Period 2025 4" o "4. Period 2025"
                                period_pattern = re.search(r'period\s+(\d{4})\s*[.\s]*(\d{1,2})', periodo_str, re.IGNORECASE)
                                if period_pattern:
                                    año = int(period_pattern.group(1))
                                    mes_candidato = int(period_pattern.group(2))
                                    if 1 <= mes_candidato <= 12:
                                        mes_match = mes_candidato
                                
                                # Patrón 2: Buscar número seguido de punto y año, luego otro número
                                # Ejemplo: "004.2025 4" -> mes=4, año=2025
                                dot_pattern = re.search(r'(\d{1,2})\.\s*(\d{4})\s+(\d{1,2})', periodo_str)
                                if dot_pattern and not mes_match:
                                    año_candidato = int(dot_pattern.group(2))
                                    mes_candidato = int(dot_pattern.group(3))
                                    if 1 <= mes_candidato <= 12:
                                        año = año_candidato
                                        mes_match = mes_candidato
                                
                                # Patrón 3: Buscar número antes de "Period YYYY"
                                # Ejemplo: "4. Period 2025"
                                before_period = re.search(r'(\d{1,2})\s*\.\s*period\s+(\d{4})', periodo_str, re.IGNORECASE)
                                if before_period and not mes_match:
                                    mes_candidato = int(before_period.group(1))
                                    año_candidato = int(before_period.group(2))
                                    if 1 <= mes_candidato <= 12:
                                        año = año_candidato
                                        mes_match = mes_candidato
                                
                                # Patrón 4: Buscar formato estándar MM/YYYY o YYYY-MM
                                if '/' in periodo_str:
                                    partes = periodo_str.split('/')
                                    if len(partes) == 2:
                                        try:
                                            mes_candidato = int(partes[0])
                                            año_candidato = int(partes[1])
                                            if año_candidato < 100:
                                                año_candidato += 2000
                                            if 1 <= mes_candidato <= 12 and 2000 <= año_candidato <= 2099:
                                                año = año_candidato
                                                mes_match = mes_candidato
                                        except (ValueError, TypeError):
                                            pass
                                
                                if '-' in periodo_str and not mes_match:
                                    partes = periodo_str.split('-')
                                    if len(partes) == 2:
                                        try:
                                            año_candidato = int(partes[0])
                                            mes_candidato = int(partes[1])
                                            if año_candidato < 100:
                                                año_candidato += 2000
                                            if 1 <= mes_candidato <= 12 and 2000 <= año_candidato <= 2099:
                                                año = año_candidato
                                                mes_match = mes_candidato
                                        except (ValueError, TypeError):
                                            pass
                                
                                # Patrón 5: Buscar todos los números y encontrar mes y año
                                if not mes_match and año:
                                    numeros = re.findall(r'\d+', periodo_str)
                                    for num_str in numeros:
                                        num = int(num_str)
                                        # Si es un número de 1-12, podría ser el mes
                                        if 1 <= num <= 12 and not mes_match:
                                            mes_match = num
                                        # Si es un número de 4 dígitos entre 2000-2099, es el año
                                        elif 2000 <= num <= 2099 and not año:
                                            año = num
                                
                                # Patrón 6: Formato YYYYMM o MMYYYY (6 dígitos)
                                if not mes_match and len(periodo_str.replace(' ', '').replace('.', '').replace('-', '')) == 6:
                                    periodo_limpio = re.sub(r'[^\d]', '', periodo_str)
                                    if len(periodo_limpio) == 6:
                                        try:
                                            # Intentar YYYYMM primero
                                            año_candidato = int(periodo_limpio[:4])
                                            mes_candidato = int(periodo_limpio[4:])
                                            if 1 <= mes_candidato <= 12 and 2000 <= año_candidato <= 2099:
                                                año = año_candidato
                                                mes_match = mes_candidato
                                        except (ValueError, TypeError):
                                            try:
                                                # Intentar MMYYYY
                                                mes_candidato = int(periodo_limpio[:2])
                                                año_candidato = int(periodo_limpio[2:])
                                                if año_candidato < 100:
                                                    año_candidato += 2000
                                                if 1 <= mes_candidato <= 12 and 2000 <= año_candidato <= 2099:
                                                    año = año_candidato
                                                    mes_match = mes_candidato
                                            except (ValueError, TypeError):
                                                pass
                                
                                # Patrón 7: Intentar parsear como fecha completa usando pandas
                                if not mes_match or not año:
                                    try:
                                        fecha = pd.to_datetime(periodo_str, errors='coerce')
                                        if pd.notna(fecha):
                                            año = fecha.year
                                            mes_match = fecha.month
                                    except:
                                        pass
                                
                                # Si tenemos mes y año válidos, crear la fecha
                                if año and mes_match and 1 <= mes_match <= 12 and 2000 <= año <= 2099:
                                    venta_dict['periodo'] = date_class(año, mes_match, 1)
                                    
                            except Exception as e:
                                # Si hay algún error, dejar como None
                                pass
                else:
                    venta_dict['periodo'] = None
                
                # Producto_condensado - Artnr condensed
                if 'Artnr condensed' in columnas_map:
                    producto_cond_val = df.iloc[idx_fila, columnas_map['Artnr condensed']]
                    venta_dict['producto_condensado'] = str(producto_cond_val).strip() if pd.notna(producto_cond_val) else None
                
                # Region_asc - Region ASC
                if 'Region ASC' in columnas_map:
                    region_val = df.iloc[idx_fila, columnas_map['Region ASC']]
                    venta_dict['region_asc'] = str(region_val).strip() if pd.notna(region_val) else None
                
                # Planta - Plant
                if 'Plant' in columnas_map:
                    planta_val = df.iloc[idx_fila, columnas_map['Plant']]
                    venta_dict['planta'] = str(planta_val).strip() if pd.notna(planta_val) else None
                
                # ship_to_party - Shipt-to-party
                if 'Shipt-to-party' in columnas_map:
                    ship_val = df.iloc[idx_fila, columnas_map['Shipt-to-party']]
                    if pd.notna(ship_val):
                        ship_str = str(ship_val).strip()
                        venta_dict['ship_to_party'] = ship_str if ship_str and ship_str.lower() != 'nan' else None
                    else:
                        venta_dict['ship_to_party'] = None
                else:
                    venta_dict['ship_to_party'] = None
                
                # Producto - Producto
                if 'Producto' in columnas_map:
                    producto_val = df.iloc[idx_fila, columnas_map['Producto']]
                    venta_dict['producto'] = str(producto_val).strip() if pd.notna(producto_val) else None
                
                # descripcion_producto - Product Description
                if 'Product Description' in columnas_map:
                    desc_val = df.iloc[idx_fila, columnas_map['Product Description']]
                    venta_dict['descripcion_producto'] = str(desc_val).strip() if pd.notna(desc_val) else None
                
                # Campos numéricos - convertir a Decimal o None
                campos_numericos = {
                    'Turnover w/o metal': 'turnover_wo_metal',
                    'OE/Turnover like FI': 'oe_turnover_like_fi',
                    'Copper Sales (CUV)': 'copper_sales_cuv',
                    'CU-Sales effect': 'cu_sales_effect',
                    'CU result': 'cu_result',
                    'Quantity OE/TO M': 'quantity_oe_to_m',
                    'Quantity OE/TO FT': 'quantity_oe_to_ft',
                    'CU Weight techn. CUT': 'cu_weight_techn_cut',
                    'CU weight Sales  CUV': 'cu_weight_sales_cuv',
                    'Conversion de FT a M': 'conversion_ft_a_m',
                    'Sales total MTS': 'sales_total_mts',
                    'Sales KM': 'sales_km',
                    'Precio Exmetal KM': 'precio_exmetal_km',
                    'Precio Full Metal KM': 'precio_full_metal_km',
                    'Precio Exmetal M': 'precio_exmetal_m',
                    'Precio Full Metal M': 'precio_full_metal_m'
                }
                
                for col_excel, col_bd in campos_numericos.items():
                    if col_excel in columnas_map:
                        val = df.iloc[idx_fila, columnas_map[col_excel]]
                        if pd.notna(val):
                            try:
                                val_str = str(val).strip()
                                if val_str and val_str.lower() != 'nan' and val_str != '':
                                    venta_dict[col_bd] = Decimal(str(val))
                                else:
                                    venta_dict[col_bd] = None
                            except (ValueError, TypeError):
                                venta_dict[col_bd] = None
                        else:
                            venta_dict[col_bd] = None
                    else:
                        venta_dict[col_bd] = None
                
                ventas_data.append(venta_dict)
            
            # Insertar los datos en la base de datos (verificando duplicados)
            resultado = await crud.bulk_create_ventas(db, ventas_data)
            registros_insertados = resultado.get("insertados", 0)
            registros_duplicados = resultado.get("duplicados", 0)
            errores = resultado.get("errores", [])
            
            mensaje = f"Archivo procesado exitosamente. {registros_insertados} registros insertados."
            if registros_duplicados > 0:
                mensaje += f" {registros_duplicados} registros duplicados fueron omitidos (ya existían con la misma combinación de código cliente, producto y período)."
            if errores:
                mensaje += f" Se encontraron {len(errores)} error(es) al procesar algunos registros."
            
            # Determinar si fue exitoso (al menos algunos registros se insertaron o había duplicados)
            success = registros_insertados > 0 or (registros_duplicados > 0 and len(errores) == 0)
            
            # Actualizar historial con resultado exitoso
            fin_utc = datetime.now(tz.utc)
            duracion = int((fin_utc - ahora_utc).total_seconds())
            await crud.update_sales_execution_status(
                db=db,
                execution_id=execution.id,
                estado=ExecutionStatus.SUCCESS,
                fecha_inicio_ejecucion=ahora_utc,
                fecha_fin_ejecucion=fin_utc,
                duracion_segundos=duracion
            )
            
            return JSONResponse(
                status_code=200 if success else 207,  # 207 = Multi-Status si hay errores pero también éxitos
                content={
                    "success": success,
                    "message": mensaje,
                    "registros_insertados": registros_insertados,
                    "registros_duplicados": registros_duplicados,
                    "total_registros": len(ventas_data),
                    "errores": errores
                }
            )
        
    except Exception as e:
        import traceback
        error_traceback = traceback.format_exc()
        error_message = str(e)
        
        # Si se creó el registro de ejecución, actualizarlo a FAILED
        try:
            if execution is not None:
                await crud.update_sales_execution_status(
                    db=db,
                    execution_id=execution.id,
                    estado=ExecutionStatus.FAILED,
                    mensaje_error=str(e),
                    stack_trace=error_traceback
                )
        except Exception:
            pass  # No fallar si no se puede actualizar el historial
        
        # Mensaje de error más descriptivo en español
        mensaje_error = f"Error al procesar el archivo de ventas: {error_message}"
        
        # Mensajes de error más descriptivos en español según el tipo de error
        error_lower = error_message.lower()
        
        if "duplicate" in error_lower or "unique" in error_lower or "ya existe" in error_lower:
            mensaje_error = "Error: Se intentó insertar un registro duplicado. El sistema ya detectó y omitió los duplicados, pero puede haber un problema con la verificación. Por favor, verifica que no existan registros con la misma combinación de código cliente, producto y período."
        elif "foreign key" in error_lower or "constraint" in error_lower or "violates foreign key" in error_lower:
            mensaje_error = "Error: Violación de restricción de base de datos. Verifica que los datos de referencia (grupo de cliente) existan en la base de datos antes de insertar las ventas."
        elif ("null" in error_lower and "not null" in error_lower) or "required" in error_lower:
            mensaje_error = "Error: Se intentó insertar un registro con campos requeridos vacíos. Por favor, verifica que todos los campos obligatorios tengan valores válidos en el archivo Excel."
        elif "connection" in error_lower or "timeout" in error_lower or "database" in error_lower:
            mensaje_error = "Error: No se pudo conectar con la base de datos. Por favor, verifica la conexión e intenta nuevamente."
        elif "value" in error_lower and "type" in error_lower:
            mensaje_error = "Error: Tipo de dato incorrecto. Verifica que los valores en el archivo Excel sean del tipo correcto (números, fechas, texto)."
        elif "periodo" in error_lower or "date" in error_lower:
            mensaje_error = "Error al procesar el período. Verifica que el formato del período en el archivo Excel sea correcto (mes y año)."
        else:
            mensaje_error = f"Error al procesar el archivo de ventas: {error_message}"
        
        # Asegurar que siempre haya un mensaje de error en español
        if not mensaje_error or mensaje_error.strip() == "":
            mensaje_error = f"Error inesperado al procesar el archivo de ventas. Tipo de error: {type(e).__name__}"
        
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": mensaje_error,
                "detalle_tecnico": error_message,
                "tipo_error": type(e).__name__,
                "traceback": error_traceback
            }
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
