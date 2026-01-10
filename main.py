from fastapi import FastAPI, Request, Depends, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import RedirectResponse, JSONResponse
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.auth.router import router as auth_router, get_current_user, AuthenticationError
from app.db.init_db import init_db
from app.db.base import get_db
from app.db.models import User
import threading
import sys

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


@app.get("/compras")
async def compras(request: Request, current_user: User = Depends(get_current_user)):
    """Página de compras - requiere autenticación."""
    return templates.TemplateResponse(
        "compras.html",
        {
            "request": request,
            "active_page": "compras",
            "current_user": current_user
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
