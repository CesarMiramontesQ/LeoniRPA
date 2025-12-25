from fastapi import FastAPI, Request, Depends, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import RedirectResponse
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.auth.router import router as auth_router, get_current_user, AuthenticationError
from app.db.init_db import init_db
from app.db.base import get_db
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


@app.get("/admin/users")
async def admin_users(request: Request, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Gestión de usuarios - solo admin."""
    if current_user.role != "admin":
        return RedirectResponse(url="/dashboard", status_code=302)
    
    from sqlalchemy import select
    from app.db.models import User
    
    # Obtener todos los usuarios
    result = await db.execute(select(User).order_by(User.created_at.desc()))
    users = result.scalars().all()
    
    return templates.TemplateResponse(
        "admin_users.html",
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
    full_name: Optional[str] = Form(None),
    role: str = Form("user"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Crear nuevo usuario - solo admin."""
    if current_user.role != "admin":
        return RedirectResponse(url="/dashboard", status_code=302)
    
    from app.db import crud
    
    # Validaciones
    if len(password) < 8:
        return RedirectResponse(
            url=f"/admin/users?error=El password debe tener al menos 8 caracteres",
            status_code=302
        )
    
    if len(password.encode('utf-8')) > 72:
        return RedirectResponse(
            url=f"/admin/users?error=El password es demasiado largo (máximo 72 bytes)",
            status_code=302
        )
    
    # Validar rol
    if role not in ["user", "admin", "auditor"]:
        role = "user"
    
    # Verificar si el email ya existe
    existing_user = await crud.get_user_by_email(db, email)
    if existing_user:
        return RedirectResponse(
            url=f"/admin/users?error=El email ya está registrado",
            status_code=302
        )
    
    try:
        new_user = await crud.create_user(
            db=db,
            email=email,
            password=password,
            full_name=full_name,
            role=role
        )
        return RedirectResponse(url="/admin/users?success=Usuario creado exitosamente", status_code=302)
    except Exception as e:
        return RedirectResponse(
            url=f"/admin/users?error=Error al crear usuario: {str(e)}",
            status_code=302
        )


@app.post("/admin/users/{user_id}/update-role")
async def admin_update_user_role(
    user_id: int,
    request: Request,
    role: str = Form(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Actualizar rol de usuario - solo admin."""
    if current_user.role != "admin":
        return RedirectResponse(url="/dashboard", status_code=302)
    
    from app.db import crud
    
    # Validar rol
    if role not in ["user", "admin", "auditor"]:
        return RedirectResponse(url="/admin/users?error=Rol inválido", status_code=302)
    
    try:
        await crud.update_user_role(db, user_id, role)
        return RedirectResponse(url="/admin/users?success=Rol actualizado exitosamente", status_code=302)
    except Exception as e:
        return RedirectResponse(
            url=f"/admin/users?error=Error al actualizar rol: {str(e)}",
            status_code=302
        )


@app.get("/hello/{name}")
async def say_hello(name: str):
    """Endpoint de ejemplo - no requiere autenticación."""
    return {"message": f"Hello {name}"}
