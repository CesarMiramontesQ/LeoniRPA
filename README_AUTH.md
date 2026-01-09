# Módulo de Autenticación - Leoni RPA

Sistema completo de autenticación y gestión de usuarios implementado en FastAPI.

## Características

- ✅ Registro de usuarios
- ✅ Inicio de sesión
- ✅ Cierre de sesión
- ✅ Autenticación JWT con cookies HttpOnly
- ✅ Sistema de roles (user, admin, auditor)
- ✅ Protección de rutas por roles
- ✅ Hash de passwords con bcrypt
- ✅ Base de datos PostgreSQL con SQLAlchemy 2.0 async

## Estructura del Proyecto

```
LeoniRPA/
├── app/
│   ├── core/
│   │   ├── config.py       # Configuración y variables de entorno
│   │   └── security.py     # Hash de passwords y JWT
│   ├── db/
│   │   ├── base.py         # Configuración de base de datos
│   │   ├── models.py       # Modelos SQLAlchemy
│   │   ├── crud.py         # Operaciones CRUD
│   │   └── init_db.py      # Inicialización de BD
│   └── auth/
│       ├── router.py       # Rutas de autenticación
│       └── schemas.py      # Schemas Pydantic
├── templates/
│   ├── login.html          # Página de login
│   ├── register.html       # Página de registro
│   ├── dashboard.html      # Dashboard principal
│   └── admin.html          # Panel de administración
├── main.py                 # Aplicación principal
├── create_admin.py         # Script para crear admin
└── requirements.txt        # Dependencias
```

## Instalación

### 1. Instalar dependencias

```bash
# Activar entorno virtual (si no está activado)
source venv/bin/activate  # Linux/Mac
# o
venv\Scripts\activate     # Windows

# Instalar dependencias
pip install -r requirements.txt
```

### 2. Configurar PostgreSQL

Asegúrate de tener PostgreSQL instalado y ejecutándose. Luego crea la base de datos:

```bash
# Conectar a PostgreSQL
psql -U postgres

# Crear la base de datos
CREATE DATABASE leoni_rpa;

# Salir de psql
\q
```

### 3. Configurar variables de entorno

Crea un archivo `.env` en la raíz del proyecto con las siguientes variables:

```env
# JWT Settings
SECRET_KEY=dev-secret-key-change-in-production-12345678901234567890
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=1440

# Database Configuration (PostgreSQL)
# Formato: postgresql+asyncpg://usuario:contraseña@host:puerto/nombre_base_datos
DB_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/leoni_rpa

# Cookie Settings
COOKIE_SECURE=False
COOKIE_SAMESITE=lax

# Admin User Creation (opcional)
ADMIN_EMAIL=admin@example.com
ADMIN_PASSWORD=admin123456
ADMIN_NAME=Administrador
```

**Importante**: Ajusta los valores de `DB_URL` según tu configuración de PostgreSQL (usuario, contraseña, host, puerto y nombre de base de datos).

### 4. Inicializar la base de datos

La base de datos se inicializa automáticamente al iniciar la aplicación, pero puedes hacerlo manualmente:

```bash
python -m app.db.init_db
```

### 5. Crear usuario administrador

**📖 Para instrucciones detalladas paso a paso, consulta: [INSTRUCCIONES_ADMIN.md](INSTRUCCIONES_ADMIN.md)**

**Resumen rápido:**

Opción 1: Usar el script (recomendado)

```bash
python create_admin.py
```

El script te pedirá:
- Email del administrador
- Contraseña (mínimo 8 caracteres, máximo 72 bytes)
- Nombre completo (opcional)

Opción 2: Usar variables de entorno

En tu archivo `.env`, configura:
```env
ADMIN_EMAIL=admin@example.com
ADMIN_PASSWORD=tu_contraseña_segura
ADMIN_NAME=Administrador
```

Luego ejecuta:
```bash
python create_admin.py
```

**⚠️ Importante**: El registro público está deshabilitado. Solo los administradores pueden crear nuevos usuarios desde el panel `/admin/users`.

## Ejecutar la aplicación

```bash
uvicorn main:app --reload
```

La aplicación estará disponible en: http://localhost:8000

## Rutas Disponibles

### Públicas (sin autenticación)
- `GET /auth/login` - Página de login
- `POST /auth/login` - Procesar login
- `GET /auth/register` - Página de registro
- `POST /auth/register` - Procesar registro

### Protegidas (requieren autenticación)
- `GET /dashboard` - Dashboard principal (cualquier usuario)
- `GET /ventas` - Página de ventas (cualquier usuario)
- `GET /admin` - Panel de administración (solo admin)
- `GET /auth/me` - Información del usuario actual (API JSON)
- `GET /auth/logout` - Cerrar sesión

## Uso

### Registro de usuarios

1. Visita `http://localhost:8000/auth/register`
2. Completa el formulario:
   - Email (debe ser único)
   - Nombre completo (opcional)
   - Contraseña (mínimo 8 caracteres)
   - Confirmar contraseña
3. Al registrar, se iniciará sesión automáticamente

### Iniciar sesión

1. Visita `http://localhost:8000/auth/login`
2. Ingresa email y contraseña
3. Serás redirigido al dashboard

### Roles

- **user**: Usuario estándar (rol por defecto)
- **admin**: Administrador (acceso a `/admin`)
- **auditor**: Auditor (rol disponible, sin funcionalidad específica aún)

### Protección de rutas

Las rutas se protegen usando la dependencia `get_current_user`:

```python
from app.auth.router import get_current_user
from app.db.models import User

@app.get("/mi-ruta")
async def mi_ruta(current_user: User = Depends(get_current_user)):
    return {"usuario": current_user.email}
```

Para proteger por rol:

```python
from app.auth.router import require_roles

@app.get("/admin-only")
async def admin_only(current_user: User = Depends(require_roles(["admin"]))):
    return {"mensaje": "Solo admin"}
```

## Seguridad

- **Passwords**: Se hashean con bcrypt usando passlib
- **JWT**: Tokens firmados con HS256 almacenados en cookies HttpOnly
- **Cookies**: Configuradas con SameSite=Lax para desarrollo local
- **Validaciones**: 
  - Email único
  - Password mínimo 8 caracteres
  - Validación de email con Pydantic

## Desarrollo

### Estructura de la base de datos

La tabla `users` tiene los siguientes campos:
- `id`: Integer (PK)
- `email`: String (único, indexado)
- `nombre`: String (opcional)
- `password_hash`: String
- `rol`: String (default: "operador", valores: "admin", "operador", "auditor")
- `activo`: Boolean (default: True)
- `created_at`: DateTime
- `last_login`: DateTime (nullable)

### Cambiar el rol de un usuario

Por ahora, solo un administrador puede cambiar roles. Esto se puede hacer directamente en la base de datos o creando un endpoint protegido.

Ejemplo de cambio en la BD:
```python
from app.db.base import AsyncSessionLocal
from app.db import crud

async with AsyncSessionLocal() as db:
    await crud.update_user_role(db, user_id=1, role="admin")
```

## Troubleshooting

### Error: "No module named 'app'"
Asegúrate de estar en el directorio raíz del proyecto y que Python pueda encontrar el módulo `app`.

### Error: "Table 'users' already exists"
La tabla ya existe. Si necesitas resetear la BD, puedes eliminar y recrear las tablas desde PostgreSQL o reiniciar la app.

### Error de autenticación en desarrollo
Verifica que:
- Las cookies estén habilitadas en tu navegador
- No estés bloqueando cookies de terceros
- El `SECRET_KEY` sea consistente

## Notas

- En producción, cambiar `COOKIE_SECURE=True` en `.env`
- Cambiar `SECRET_KEY` por una clave segura y aleatoria
- Implementar rate limiting para login/registro
- Agregar verificación de email (opcional)

