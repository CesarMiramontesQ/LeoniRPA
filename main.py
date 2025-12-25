from fastapi import FastAPI, Request, Depends
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import RedirectResponse
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from app.auth.router import router as auth_router, get_current_user, AuthenticationError
from app.db.init_db import init_db
from app.db.models import User

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
    print(f"[DASHBOARD] Usuario accediendo: {current_user.email} (ID: {current_user.id})")
    print(f"[DASHBOARD] Cookie recibida: {request.cookies.get('access_token', 'NO HAY COOKIE')[:50]}...")
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "current_user": current_user,
            "active_page": "dashboard"
        }
    )


@app.get("/ventas")
async def ventas(request: Request, current_user: User = Depends(get_current_user)):
    """Página de ventas - requiere autenticación."""
    # Datos de ejemplo de ventas
    ventas_data = [
        {
            "id": 1,
            "cliente": "Empresa ABC S.A.",
            "producto": "Cable UTP Cat6",
            "cantidad": 100,
            "precio_unitario": 2.50,
            "total": 250.00,
            "fecha": (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        },
        {
            "id": 2,
            "cliente": "Tech Solutions Ltda.",
            "producto": "Conector RJ45",
            "cantidad": 500,
            "precio_unitario": 0.75,
            "total": 375.00,
            "fecha": (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        },
        {
            "id": 3,
            "cliente": "Redes Industriales",
            "producto": "Switch 24 Puertos",
            "cantidad": 5,
            "precio_unitario": 150.00,
            "total": 750.00,
            "fecha": (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        },
        {
            "id": 4,
            "cliente": "Comunicaciones XYZ",
            "producto": "Cable Coaxial RG6",
            "cantidad": 200,
            "precio_unitario": 1.25,
            "total": 250.00,
            "fecha": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        },
        {
            "id": 5,
            "cliente": "Infraestructura Digital",
            "producto": "Router WiFi 6",
            "cantidad": 10,
            "precio_unitario": 89.99,
            "total": 899.90,
            "fecha": datetime.now().strftime("%Y-%m-%d")
        }
    ]
    
    total_general = sum(venta["total"] for venta in ventas_data)
    
    return templates.TemplateResponse(
        "ventas.html",
        {
            "request": request,
            "ventas": ventas_data,
            "total_general": total_general,
            "active_page": "ventas",
            "current_user": current_user
        }
    )


@app.get("/admin")
async def admin(request: Request, current_user: User = Depends(get_current_user)):
    """Panel de administración - requiere rol admin."""
    if current_user.role != "admin":
        return RedirectResponse(url="/dashboard", status_code=302)
    
    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "current_user": current_user,
            "active_page": "admin"
        }
    )


@app.get("/hello/{name}")
async def say_hello(name: str):
    """Endpoint de ejemplo - no requiere autenticación."""
    return {"message": f"Hello {name}"}
