"""Rutas de autenticación."""
from datetime import timedelta
from typing import Annotated, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status, Form
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.security import HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.base import get_db
from app.db.models import User
from app.db import crud
from app.core.config import settings
from app.core.security import verify_password, create_access_token, decode_access_token
from app.auth.schemas import UserRegister, UserLogin, UserResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(prefix="/auth", tags=["auth"])
templates = Jinja2Templates(directory="templates")
security = HTTPBearer(auto_error=False)


class AuthenticationError(Exception):
    """Excepción para errores de autenticación que requieren redirección."""
    pass


async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(get_db)
) -> User:
    """Dependencia para obtener el usuario actual desde el token JWT en cookie."""
    # Debug: ver todas las cookies recibidas
    all_cookies = request.cookies
    print(f"[AUTH] Cookies recibidas: {list(all_cookies.keys())}")
    print(f"[AUTH] Buscando cookie: {settings.COOKIE_NAME}")
    
    token = request.cookies.get(settings.COOKIE_NAME)
    
    if not token:
        print(f"[AUTH] No se encontró token en cookies")
        print(f"[AUTH] Path solicitado: {request.url.path}")
        # Si no hay token y es una petición HTML, redirigir a login
        if request.headers.get("accept", "").startswith("text/html"):
            raise AuthenticationError("No autenticado")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No autenticado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    print(f"[AUTH] Token encontrado, longitud: {len(token)}")
    payload = decode_access_token(token)
    if payload is None:
        print(f"[AUTH] Token inválido o expirado")
        if request.headers.get("accept", "").startswith("text/html"):
            raise AuthenticationError("Token inválido o expirado")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado",
        )
    
    user_id_str = payload.get("sub")
    if user_id_str is None:
        print(f"[AUTH] No se encontró user_id en el payload")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido",
        )
    
    # Convertir string a int
    try:
        user_id = int(user_id_str)
    except (ValueError, TypeError):
        print(f"[AUTH] user_id no es un número válido: {user_id_str}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido",
        )
    
    print(f"[AUTH] User ID del token: {user_id}")
    user = await crud.get_user_by_id(db, user_id)
    if user is None or not user.is_active:
        print(f"[AUTH] Usuario no encontrado o inactivo: {user_id}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario no encontrado o inactivo",
        )
    
    print(f"[AUTH] Usuario autenticado correctamente: {user.email}")
    return user


def require_roles(allowed_roles: List[str]):
    """Dependencia para verificar roles."""
    async def role_checker(
        current_user: User = Depends(get_current_user)
    ) -> User:
        if current_user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Se requiere uno de los siguientes roles: {', '.join(allowed_roles)}",
            )
        return current_user
    return role_checker


@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    """Página de registro."""
    return templates.TemplateResponse("register.html", {"request": request})


@router.post("/register")
async def register(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    full_name: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db)
):
    """Registro de nuevo usuario."""
    # Validaciones
    if len(password) < 8:
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "error": "El password debe tener al menos 8 caracteres"},
            status_code=400
        )
    
    # Validar longitud máxima (bcrypt limita a 72 bytes)
    if len(password.encode('utf-8')) > 72:
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "error": "El password es demasiado largo (máximo 72 bytes)"},
            status_code=400
        )
    
    if password != confirm_password:
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "error": "Los passwords no coinciden"},
            status_code=400
        )
    
    # Verificar si el email ya existe
    existing_user = await crud.get_user_by_email(db, email)
    if existing_user:
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "error": "El email ya está registrado"},
            status_code=400
        )
    
    # Crear usuario con rol "user" por defecto
    try:
        new_user = await crud.create_user(
            db=db,
            email=email,
            password=password,
            full_name=full_name,
            role="user"
        )
        
        # Iniciar sesión automáticamente después del registro
        access_token = create_access_token(
            data={"sub": str(new_user.id), "email": new_user.email},
            expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        
        # Crear respuesta de redirección con cookie
        response = RedirectResponse(url="/dashboard", status_code=302)
        
        # Asegurarse de que SameSite sea un valor válido (lowercase)
        samesite_value = settings.COOKIE_SAMESITE.lower()
        if samesite_value not in ["strict", "lax", "none"]:
            samesite_value = "lax"
        
        response.set_cookie(
            key=settings.COOKIE_NAME,
            value=access_token,
            httponly=settings.COOKIE_HTTPONLY,
            secure=settings.COOKIE_SECURE,
            samesite=samesite_value,
            max_age=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            path="/"
        )
        return response
    except Exception as e:
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "error": f"Error al crear usuario: {str(e)}"},
            status_code=500
        )


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Página de login."""
    return templates.TemplateResponse("login.html", {"request": request})


@router.post("/login")
async def login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    db: AsyncSession = Depends(get_db)
):
    """Login de usuario."""
    user = await crud.get_user_by_email(db, email)
    
    if not user or not verify_password(password, user.hashed_password):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Email o password incorrecto"},
            status_code=401
        )
    
    if not user.is_active:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Usuario inactivo"},
            status_code=403
        )
    
    # Crear token (sub debe ser string según estándar JWT)
    access_token = create_access_token(
        data={"sub": str(user.id), "email": user.email},
        expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    
    # Debug: imprimir información (remover en producción)
    print(f"[LOGIN] Usuario autenticado: {user.email} (ID: {user.id})")
    print(f"[LOGIN] Token creado, longitud: {len(access_token)}")
    
    # Crear respuesta de redirección con cookie
    response = RedirectResponse(url="/dashboard", status_code=302)
    
    # Asegurarse de que SameSite sea un valor válido (lowercase)
    samesite_value = settings.COOKIE_SAMESITE.lower()
    if samesite_value not in ["strict", "lax", "none"]:
        samesite_value = "lax"
    
    response.set_cookie(
        key=settings.COOKIE_NAME,
        value=access_token,
        httponly=settings.COOKIE_HTTPONLY,
        secure=settings.COOKIE_SECURE,
        samesite=samesite_value,
        max_age=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        path="/"
    )
    
    print(f"[LOGIN] Cookie configurada: {settings.COOKIE_NAME}, redirigiendo a /dashboard")
    return response


@router.get("/logout")
async def logout(request: Request):
    """Logout y borrar cookie."""
    response = RedirectResponse(url="/auth/login", status_code=302)
    response.delete_cookie(key=settings.COOKIE_NAME, httponly=True, samesite=settings.COOKIE_SAMESITE, path="/")
    return response


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    """Obtiene información del usuario actual."""
    return UserResponse.model_validate(current_user)

