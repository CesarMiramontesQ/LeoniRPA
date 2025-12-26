# Instrucciones para Crear Usuario Administrador

Este documento explica cómo crear el primer usuario administrador en Leoni RPA desde cero.

## Requisitos Previos

1. **Python 3.8+** instalado
2. **Entorno virtual activado** (opcional pero recomendado)
3. **Dependencias instaladas**: `pip install -r requirements.txt`
4. **Base de datos inicializada** (se crea automáticamente al iniciar la app)

## Método 1: Usar el Script create_admin.py (Recomendado)

### Paso 1: Activar el entorno virtual (si usas uno)

```bash
# En Mac/Linux:
source venv/bin/activate

# En Windows:
venv\Scripts\activate
```

### Paso 2: Ejecutar el script

```bash
python create_admin.py
```

### Paso 3: Seguir las indicaciones

El script te pedirá:

1. **Email del administrador**: Ingresa el email que usará el admin (ej: `admin@leoni.com`)
2. **Contraseña del administrador**: Ingresa una contraseña segura (mínimo 8 caracteres, máximo 72 bytes)
3. **Nombre completo** (opcional): Puedes ingresar el nombre o presionar Enter para omitir

**Ejemplo de ejecución:**

```
$ python create_admin.py
Email del administrador: admin@leoni.com
Contraseña del administrador (mínimo 8 caracteres, máximo 72 bytes): Admin123456
Nombre completo (opcional): Administrador Principal
✅ Usuario administrador creado exitosamente:
   Email: admin@leoni.com
   Rol: admin
   ID: 1
```

### Paso 4: Verificar que funciona

1. Inicia la aplicación:

   ```bash
   uvicorn main:app --reload
   ```

2. Ve a `http://localhost:8000/auth/login`

3. Inicia sesión con las credenciales del admin creado

4. Deberías poder acceder al panel de administración en `/admin`

## Método 2: Usar Variables de Entorno

Si prefieres no introducir datos manualmente, puedes usar variables de entorno.

### Paso 1: Crear archivo .env

Crea un archivo `.env` en la raíz del proyecto (si no existe):

```bash
touch .env
```

### Paso 2: Configurar variables en .env

Abre el archivo `.env` y agrega:

```env
ADMIN_EMAIL=admin@leoni.com
ADMIN_PASSWORD=TuContraseñaSegura123
ADMIN_NAME=Administrador Principal
```

**⚠️ Importante**:

- Asegúrate de que la contraseña tenga al menos 8 caracteres
- No uses espacios en las variables de entorno
- Nunca subas el archivo `.env` a repositorios públicos

### Paso 3: Ejecutar el script

```bash
python create_admin.py
```

El script leerá las variables de entorno y creará el usuario automáticamente sin pedirte datos.

## Método 3: Crear Manualmente con Python Interactivo

Si necesitas más control, puedes crear el usuario directamente desde Python:

### Paso 1: Abrir Python en el directorio del proyecto

```bash
python
```

### Paso 2: Ejecutar el código

```python
import asyncio
from app.db.base import AsyncSessionLocal
from app.db import crud

async def create_admin():
    async with AsyncSessionLocal() as db:
        admin = await crud.create_user(
            db=db,
            email="admin@leoni.com",
            password="TuContraseñaSegura123",
            full_name="Administrador",
            role="admin"
        )
        print(f"✅ Usuario creado: {admin.email} (ID: {admin.id})")

asyncio.run(create_admin())
```

## Verificación y Uso

### Verificar que el usuario admin existe

1. Inicia sesión en la aplicación con las credenciales del admin
2. Ve a `/admin` - deberías ver el panel de administración
3. Ve a `/admin/users` - deberías ver la lista de usuarios (incluyendo el admin)

### Funcionalidades del Admin

Una vez creado el usuario administrador, puedes:

1. **Crear nuevos usuarios**:

   - Ve a `/admin/users`
   - Usa el formulario para crear usuarios con diferentes roles (user, admin, auditor)

2. **Gestionar usuarios existentes**:

   - Ver lista de todos los usuarios
   - Cambiar roles de usuarios
   - Ver información de usuarios

3. **Acceder a todas las funcionalidades**:
   - Acceso completo a `/admin` y todas sus rutas
   - Puede crear usuarios con cualquier rol

## Solución de Problemas

### Error: "Ya existe un usuario con email..."

Si intentas crear un admin y ya existe un usuario con ese email:

- **Opción 1**: El script te preguntará si deseas cambiar la contraseña
- **Opción 2**: Usa un email diferente
- **Opción 3**: Elimina el usuario existente de la base de datos primero

### Error: "password cannot be longer than 72 bytes"

- Asegúrate de que la contraseña no sea demasiado larga
- 72 bytes es aproximadamente 72 caracteres ASCII
- Si usas caracteres especiales o Unicode, el límite puede ser menor

### Error: "ModuleNotFoundError"

Asegúrate de:

1. Tener el entorno virtual activado (si usas uno)
2. Haber instalado todas las dependencias: `pip install -r requirements.txt`
3. Estar en el directorio raíz del proyecto

### La base de datos no existe

La base de datos se crea automáticamente la primera vez que inicias la aplicación. Si necesitas crearla manualmente:

```bash
python -m app.db.init_db
```

## Notas Importantes

1. **Seguridad**:

   - Cambia la contraseña por defecto después de la primera sesión
   - No compartas las credenciales del administrador
   - Usa contraseñas seguras (mínimo 8 caracteres, combinar mayúsculas, minúsculas, números)

2. **Primer Admin**:

   - El primer usuario admin debe crearse usando uno de estos métodos
   - Después, los admins pueden crear más usuarios desde la interfaz web

3. **Base de Datos**:

   - La base de datos SQLite se guarda en `leoni_rpa.db` en la raíz del proyecto
   - Para resetear todo, puedes eliminar este archivo (⚠️ esto borrará todos los usuarios)

4. **Múltiples Admins**:
   - Puedes crear múltiples usuarios con rol "admin"
   - Todos tendrán los mismos privilegios

## Estructura de Roles

El sistema soporta tres roles:

- **user**: Usuario estándar (acceso básico)
- **admin**: Administrador (puede crear usuarios y gestionar el sistema)
- **auditor**: Auditor (rol disponible para futuras funcionalidades)

## Próximos Pasos

Después de crear el usuario administrador:

1. Inicia sesión en la aplicación
2. Explora el panel de administración en `/admin`
3. Crea usuarios adicionales según sea necesario
4. Configura las funcionalidades específicas de tu aplicación

---

**¿Necesitas ayuda?** Revisa los logs de la aplicación o consulta el archivo `README_AUTH.md` para más información sobre el sistema de autenticación.
