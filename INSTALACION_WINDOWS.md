# Manual de Instalación - Leoni RPA (Windows)

Este manual te guiará paso a paso para instalar Leoni RPA desde cero en una computadora con Windows.

## 📋 Requisitos Previos

Antes de comenzar, asegúrate de tener:

- Windows 10 o superior
- Acceso de administrador a la computadora
- Conexión a internet
- Al menos 2 GB de espacio libre en disco

## 📦 Paso 1: Instalar Python

1. **Descargar Python**:
   - Visita: https://www.python.org/downloads/
   - Descarga la versión **Python 3.11** o superior (recomendado: Python 3.11 o 3.12)
   - Descarga el instalador para Windows (archivo `.exe`)

2. **Ejecutar el instalador**:
   - Haz doble clic en el archivo descargado
   - **⚠️ IMPORTANTE**: Marca la casilla **"Add Python to PATH"** en la primera pantalla
   - Haz clic en **"Install Now"**
   - Espera a que termine la instalación
   - Haz clic en **"Close"**

3. **Verificar la instalación**:
   - Abre el **Símbolo del sistema (CMD)** o **PowerShell**
   - Ejecuta:
     ```cmd
     python --version
     ```
   - Deberías ver algo como: `Python 3.11.x` o `Python 3.12.x`
   - También verifica pip:
     ```cmd
     pip --version
     ```

## 🗄️ Paso 2: Verificar y Configurar Conexión a PostgreSQL

**Nota**: Este manual asume que ya tienes PostgreSQL instalado en un servidor. Si no tienes PostgreSQL instalado localmente, consulta con tu administrador de sistemas para obtener acceso.

### 2.1: Obtener Información del Servidor PostgreSQL

Necesitas obtener la siguiente información de tu administrador de sistemas o del equipo de TI:

- ✅ **Host/IP del servidor**: Por ejemplo: `192.168.1.100`, `servidor.local`, o `localhost` si está en la misma máquina
- ✅ **Puerto**: Generalmente es `5432` (puerto por defecto de PostgreSQL)
- ✅ **Nombre de la base de datos**: Puede ser `leoni_rpa` o el que te hayan asignado
- ✅ **Usuario de la base de datos**: Por ejemplo: `postgres`, `leoni_user`, o el usuario que te hayan proporcionado
- ✅ **Contraseña**: La contraseña del usuario de la base de datos
- ✅ **Nombre de la base de datos a usar**: Puede que ya exista o necesites crear `leoni_rpa`

**⚠️ IMPORTANTE**: Guarda esta información de forma segura, la necesitarás en los siguientes pasos.

### 2.2: Verificar Conectividad con el Servidor

1. **Verificar que puedes alcanzar el servidor**:
   ```cmd
   ping [IP_O_HOST_DEL_SERVIDOR]
   ```
   Ejemplo: `ping 192.168.1.100` o `ping servidor.local`
   
   - Si el ping funciona, verás respuestas del servidor
   - Si no funciona, verifica:
     - Que estés en la misma red
     - Que el firewall no esté bloqueando la conexión
     - Que el host/IP sea correcto

2. **Verificar que el puerto de PostgreSQL esté abierto**:
   ```cmd
   telnet [IP_O_HOST] [PUERTO]
   ```
   Ejemplo: `telnet 192.168.1.100 5432`
   
   - Si `telnet` no está instalado, puedes usar PowerShell:
     ```powershell
     Test-NetConnection -ComputerName [IP_O_HOST] -Port [PUERTO]
     ```
   - Si la conexión es exitosa, el puerto está abierto
   - Si falla, verifica con tu administrador de sistemas

3. **Verificar conexión con PostgreSQL (si tienes cliente instalado)**:
   ```cmd
   psql -h [IP_O_HOST] -p [PUERTO] -U [USUARIO] -d postgres
   ```
   Ejemplo: `psql -h 192.168.1.100 -p 5432 -U postgres -d postgres`
   
   - Te pedirá la contraseña
   - Si conecta exitosamente, puedes escribir `\q` para salir

### 2.3: Instalar Cliente PostgreSQL (Opcional pero Recomendado)

Si necesitas herramientas para gestionar la base de datos, puedes instalar:

**Opción A: pgAdmin 4** (Interfaz gráfica - Recomendado):
- Descarga desde: https://www.pgadmin.org/download/pgadmin-4-windows/
- Instala normalmente
- Úsalo para conectarte al servidor y gestionar bases de datos

**Opción B: Solo línea de comandos**:
- Descarga solo el cliente desde: https://www.postgresql.org/download/windows/
- Durante la instalación, selecciona solo "Command Line Tools"

## 📥 Paso 3: Obtener el Proyecto

### Opción A: Clonar desde Git (Recomendado si tienes acceso al repositorio)

1. **Instalar Git para Windows** (si no lo tienes):
   - Descarga desde: https://git-scm.com/download/win
   - Ejecuta el instalador con opciones por defecto

2. **Clonar el repositorio**:
   ```cmd
   git clone <URL_DEL_REPOSITORIO>
   cd LeoniRPA
   ```

### Opción B: Descargar como ZIP

1. Descarga el proyecto como archivo ZIP
2. Extrae el contenido en una carpeta (por ejemplo: `C:\Proyectos\LeoniRPA`)
3. Abre CMD o PowerShell y navega a la carpeta:
   ```cmd
   cd C:\Proyectos\LeoniRPA
   ```

## 🐍 Paso 4: Crear Entorno Virtual

1. **Navegar a la carpeta del proyecto**:
   ```cmd
   cd C:\Ruta\Al\Proyecto\LeoniRPA
   ```

2. **Crear el entorno virtual**:
   ```cmd
   python -m venv venv
   ```

3. **Activar el entorno virtual**:
   ```cmd
   venv\Scripts\activate
   ```
   - Verás que el prompt cambia y muestra `(venv)` al inicio
   - Si esto no funciona, intenta:
     ```cmd
     .\venv\Scripts\activate
     ```

## 📚 Paso 5: Instalar Dependencias

Con el entorno virtual activado (deberías ver `(venv)` en tu prompt):

```cmd
pip install --upgrade pip
pip install -r requirements.txt
```

Esto instalará todas las dependencias necesarias:
- FastAPI
- Uvicorn
- SQLAlchemy
- PostgreSQL driver (asyncpg)
- JWT y autenticación
- Y otras dependencias

**Tiempo estimado**: 2-5 minutos dependiendo de tu conexión a internet.

## ⚙️ Paso 6: Configurar Variables de Entorno

1. **Crear archivo `.env`**:
   - En la raíz del proyecto (donde está `main.py`)
   - Crea un archivo llamado `.env` (sin extensión)
   - Puedes usar el Bloc de notas o cualquier editor de texto

2. **Contenido del archivo `.env`**:
   ```env
   # JWT Settings
   SECRET_KEY=dev-secret-key-change-in-production-12345678901234567890
   ALGORITHM=HS256
   ACCESS_TOKEN_EXPIRE_MINUTES=1440

   # Database Configuration (PostgreSQL)
   # Formato: postgresql+asyncpg://usuario:contraseña@host:puerto/nombre_base_datos
   # IMPORTANTE: Reemplaza los valores con la información de tu servidor PostgreSQL
   DB_URL=postgresql+asyncpg://USUARIO:CONTRASEÑA@HOST:PUERTO/NOMBRE_BASE_DATOS

   # Cookie Settings
   COOKIE_SECURE=False
   COOKIE_SAMESITE=lax
   ```

3. **⚠️ IMPORTANTE - Reemplazar valores**:
   
   Usa la información que obtuviste en el Paso 2.1:
   
   - **USUARIO**: El usuario de la base de datos (ejemplo: `postgres`, `leoni_user`)
   - **CONTRASEÑA**: La contraseña del usuario (⚠️ Si contiene caracteres especiales, pueden necesitar codificación URL)
   - **HOST**: La IP o nombre del servidor (ejemplo: `192.168.1.100`, `servidor.local`, `localhost`)
   - **PUERTO**: El puerto de PostgreSQL (generalmente `5432`)
   - **NOMBRE_BASE_DATOS**: El nombre de la base de datos (ejemplo: `leoni_rpa`)

**Ejemplos de URLs de conexión**:

**Ejemplo 1** - Servidor local (misma máquina):
```env
DB_URL=postgresql+asyncpg://postgres:MiPassword123@localhost:5432/leoni_rpa
```

**Ejemplo 2** - Servidor en la red local:
```env
DB_URL=postgresql+asyncpg://leoni_user:Password123!@192.168.1.100:5432/leoni_rpa
```

**Ejemplo 3** - Servidor con nombre de dominio:
```env
DB_URL=postgresql+asyncpg://postgres:MiPassword123@servidor.local:5432/leoni_rpa
```

**Ejemplo 4** - Con contraseña que contiene caracteres especiales:
Si tu contraseña es `P@ssw0rd!2024`, algunos caracteres necesitan codificación:
- `@` se codifica como `%40`
- `!` se codifica como `%21`
- `#` se codifica como `%23`
- etc.

Ejemplo con codificación:
```env
DB_URL=postgresql+asyncpg://postgres:P%40ssw0rd%212024@192.168.1.100:5432/leoni_rpa
```

**Nota sobre caracteres especiales**: Si tienes problemas con caracteres especiales en la contraseña, puedes:
1. Escapar los caracteres especiales (como en el ejemplo 4)
2. O pedir a tu administrador que cambie la contraseña por una sin caracteres especiales (solo letras, números y algunos caracteres como `-` o `_`)

## 🗃️ Paso 7: Crear la Base de Datos (si no existe)

**Nota**: Verifica primero con tu administrador si la base de datos ya existe. Si ya existe, puedes saltar este paso.

### 7.1: Verificar si la Base de Datos Existe

**Opción A: Usar pgAdmin 4** (Recomendado):

1. Abre **pgAdmin 4** (si lo tienes instalado)
2. Haz clic derecho en **"Servers"** → **"Register"** → **"Server..."**
3. En la pestaña **"General"**:
   - **Name**: `Leoni RPA Server` (o el nombre que prefieras)
4. En la pestaña **"Connection"**:
   - **Host name/address**: La IP o host del servidor (ejemplo: `192.168.1.100`)
   - **Port**: El puerto (generalmente `5432`)
   - **Maintenance database**: `postgres`
   - **Username**: El usuario de la base de datos
   - **Password**: La contraseña del usuario
   - Marca **"Save password"** si quieres que se guarde
5. Haz clic en **"Save"**
6. Expande el servidor y luego **"Databases"**
7. Verifica si existe una base de datos llamada `leoni_rpa` (o el nombre que te hayan asignado)

**Opción B: Usar línea de comandos**:
```cmd
psql -h [HOST] -p [PUERTO] -U [USUARIO] -d postgres -l
```
Ejemplo: `psql -h 192.168.1.100 -p 5432 -U postgres -d postgres -l`
- Te pedirá la contraseña
- Verás una lista de todas las bases de datos

### 7.2: Crear la Base de Datos (si no existe)

Si la base de datos **NO existe**, créala usando una de estas opciones:

**Opción A: Usar pgAdmin 4**:

1. Conéctate al servidor (siguiendo los pasos del 7.1)
2. Haz clic derecho en **"Databases"** → **"Create"** → **"Database..."**
3. En la pestaña **"General"**:
   - **Database**: `leoni_rpa` (o el nombre que te hayan indicado)
4. En la pestaña **"Definition"**:
   - **Owner**: Selecciona el usuario que te proporcionaron
   - **Encoding**: `UTF8` (recomendado)
5. Haz clic en **"Save"**

**Opción B: Usar línea de comandos**:
```cmd
psql -h [HOST] -p [PUERTO] -U [USUARIO] -d postgres
```
Ejemplo: `psql -h 192.168.1.100 -p 5432 -U postgres -d postgres`
- Te pedirá la contraseña
- Luego ejecuta:
```sql
CREATE DATABASE leoni_rpa OWNER [USUARIO];
\q
```
Ejemplo: `CREATE DATABASE leoni_rpa OWNER postgres;`

**Opción C: Pedir al administrador que la cree**:

Si no tienes permisos para crear bases de datos, solicita a tu administrador que cree la base de datos `leoni_rpa` (o el nombre asignado) y te otorgue permisos sobre ella.

### 7.3: Verificar Permisos

Asegúrate de que tu usuario tenga los permisos necesarios sobre la base de datos:

1. **Conectarse a la base de datos**: `CONNECT`
2. **Crear tablas**: `CREATE`
3. **Modificar datos**: `INSERT`, `UPDATE`, `DELETE`, `SELECT`

Si tienes problemas de permisos, contacta a tu administrador de base de datos.

### 7.4: Verificar que la Conexión Funciona

Puedes verificar que todo esté configurado correctamente intentando conectarte a la base de datos:

```cmd
psql -h [HOST] -p [PUERTO] -U [USUARIO] -d leoni_rpa
```
Ejemplo: `psql -h 192.168.1.100 -p 5432 -U postgres -d leoni_rpa`

Si la conexión es exitosa, podrás escribir comandos SQL. Escribe `\q` para salir.

## 🚀 Paso 8: Inicializar la Base de Datos

Con el entorno virtual activado (deberías ver `(venv)` en tu prompt):

```cmd
python -m app.db.init_db
```

**Opcional**: La base de datos también se inicializará automáticamente la primera vez que ejecutes la aplicación.

## 👤 Paso 9: Crear Usuario Administrador

Con el entorno virtual activado:

```cmd
python create_admin.py
```

El script te pedirá:
- **Email del administrador**: Por ejemplo: `admin@leoni.com`
- **Contraseña**: Mínimo 8 caracteres (ejemplo: `Admin123!`)
- **Nombre completo** (opcional): Por ejemplo: `Administrador`

**Ejemplo de ejecución**:
```
Email: admin@leoni.com
Password: Admin123!
Nombre completo (opcional): Administrador
Usuario administrador creado exitosamente.
```

## ▶️ Paso 10: Ejecutar la Aplicación

Con el entorno virtual activado (deberías ver `(venv)` en tu prompt):

```cmd
uvicorn main:app --reload
```

Deberías ver algo como:
```
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

## 🌐 Paso 11: Acceder a la Aplicación

1. Abre tu navegador web (Chrome, Firefox, Edge, etc.)
2. Ve a: **http://localhost:8000**
3. Serás redirigido a la página de login
4. Inicia sesión con las credenciales del administrador que creaste:
   - **Email**: El que ingresaste en el Paso 9
   - **Contraseña**: La que ingresaste en el Paso 9

## ✅ Verificación de Instalación

1. **Página de login**: Deberías poder iniciar sesión con el usuario administrador
2. **Dashboard**: Después del login, deberías ver el dashboard principal
3. **Menú de navegación**: Deberías ver opciones como Dashboard, Ventas, Compras, Administración
4. **Panel de administración**: Si eres administrador, deberías poder acceder a `/admin`

## 🔧 Solución de Problemas Comunes

### Error: "python no se reconoce como comando"

**Solución**:
- Asegúrate de haber marcado "Add Python to PATH" durante la instalación
- O reinstala Python marcando esa opción
- También puedes agregar Python manualmente al PATH

### Error: "pip no se reconoce como comando"

**Solución**:
- Python 3.11+ incluye pip por defecto
- Reinstala Python asegurándote de marcar "Add Python to PATH"
- O ejecuta: `python -m pip --version`

### Error: "psql no se reconoce como comando"

**Solución**:
- Si instalaste el cliente PostgreSQL localmente:
  - Agrega PostgreSQL al PATH:
    1. Busca "Variables de entorno" en Windows
    2. Edita la variable "Path"
    3. Agrega: `C:\Program Files\PostgreSQL\15\bin` (ajusta la versión si es diferente)
- O usa pgAdmin 4 en su lugar (no requiere línea de comandos)
- O contacta a tu administrador para obtener las herramientas necesarias

### Error: "password authentication failed"

**Solución**:
- Verifica que la contraseña en el archivo `.env` sea correcta
- Verifica que el usuario sea correcto
- Si la contraseña contiene caracteres especiales, asegúrate de codificarlos correctamente en la URL
- Intenta conectarte manualmente con pgAdmin 4 o `psql` para verificar las credenciales
- Verifica que el usuario tenga permisos para conectarse al servidor desde tu IP (puede ser un tema de configuración en `pg_hba.conf` del servidor)

### Error: "database does not exist"

**Solución**:
- Verifica que la base de datos exista en el servidor (Paso 7.1)
- Verifica que el nombre de la base de datos en `.env` sea correcto (puede que tenga un nombre diferente al esperado)
- Si no existe, créala siguiendo el Paso 7.2
- Verifica que tengas permisos para conectarte a esa base de datos
- Contacta a tu administrador si necesitas que se cree la base de datos

### Error: "module 'asyncpg' not found"

**Solución**:
- Asegúrate de que el entorno virtual esté activado (deberías ver `(venv)`)
- Reinstala las dependencias:
  ```cmd
  pip install -r requirements.txt
  ```

### Error: "Address already in use" al ejecutar uvicorn

**Solución**:
- Otro proceso está usando el puerto 8000
- Puedes cambiar el puerto:
  ```cmd
  uvicorn main:app --reload --port 8001
  ```
- O cierra el proceso que está usando el puerto 8000

### Error al activar el entorno virtual

**Solución**:
- Si recibes un error de "execution policy" en PowerShell:
  1. Abre PowerShell como Administrador
  2. Ejecuta: `Set-ExecutionPolicy RemoteSigned`
  3. Selecciona "Sí" cuando te pregunte
- O usa CMD en lugar de PowerShell

### Error: "connection refused" o "could not connect to server"

**Solución**:
- Verifica que puedas alcanzar el servidor PostgreSQL (Paso 2.2):
  - Prueba con `ping [IP_DEL_SERVIDOR]`
  - Verifica que el puerto esté abierto
- Verifica que el host y puerto en `.env` sean correctos
- Verifica que el firewall no esté bloqueando la conexión:
  - Firewall de Windows
  - Firewall del servidor PostgreSQL
- Verifica que PostgreSQL esté corriendo en el servidor (contacta al administrador)
- Si el servidor está en otra red, verifica la conectividad de red (VPN, etc.)

### Error: "permission denied for database" o "permission denied for schema"

**Solución**:
- Verifica que tu usuario tenga permisos sobre la base de datos
- Contacta a tu administrador para que otorgue los permisos necesarios:
  - `CONNECT` en la base de datos
  - `CREATE` para crear tablas
  - `USAGE` y `CREATE` en el schema `public`

### La aplicación no inicia

**Solución**:
- Verifica que puedas conectarte al servidor PostgreSQL (Paso 2.2)
- Verifica que el archivo `.env` esté en la raíz del proyecto
- Verifica que el entorno virtual esté activado
- Revisa los mensajes de error en la consola para identificar el problema específico
- Verifica que todos los valores en `.env` sean correctos (especialmente la URL de conexión)

## 📝 Notas Importantes

1. **Entorno Virtual**: Siempre activa el entorno virtual antes de ejecutar comandos:
   ```cmd
   venv\Scripts\activate
   ```

2. **Información del Servidor**: Guarda de forma segura:
   - Host/IP del servidor PostgreSQL
   - Puerto de PostgreSQL
   - Usuario y contraseña de la base de datos
   - Nombre de la base de datos
   - Contraseña del usuario administrador de la aplicación

3. **Archivo .env**: 
   - Nunca subas el archivo `.env` a un repositorio público (ya está en .gitignore)
   - Este archivo contiene información sensible de conexión

4. **Conectividad de Red**:
   - Asegúrate de tener conectividad con el servidor PostgreSQL
   - Si estás en una red corporativa, puede que necesites VPN
   - Verifica con tu administrador de TI sobre restricciones de firewall

5. **Permisos**:
   - Si encuentras errores de permisos, contacta a tu administrador de base de datos
   - El usuario debe tener permisos para: `CONNECT`, `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `SELECT`

6. **Producción**: Para producción, cambia:
   - `SECRET_KEY` por una clave secreta fuerte y única
   - `COOKIE_SECURE=True` si usas HTTPS
   - Verifica que la conexión a la base de datos use credenciales seguras

## 📞 Soporte y Contacto

Si encuentras problemas que no están cubiertos en esta guía:

1. **Verifica los pasos básicos**:
   - Verifica que todos los pasos se hayan completado correctamente
   - Revisa los mensajes de error completos en la consola
   - Verifica los logs de la aplicación

2. **Problemas de conectividad con el servidor**:
   - Contacta a tu administrador de sistemas o equipo de TI
   - Verifica que tengas acceso a la red donde está el servidor PostgreSQL
   - Verifica configuración de firewall y VPN si aplica

3. **Problemas de base de datos**:
   - Contacta a tu administrador de base de datos
   - Verifica permisos y credenciales
   - Verifica que la base de datos exista y esté accesible

4. **Documentación adicional**:
   - `README_AUTH.md` - Documentación de autenticación
   - `CONFIGURACION_POSTGRESQL.md` - Configuración detallada de PostgreSQL
   - `INSTRUCCIONES_ADMIN.md` - Instrucciones para administradores

## 📋 Checklist de Información Necesaria

Antes de comenzar la instalación, asegúrate de tener esta información:

- [ ] Host/IP del servidor PostgreSQL
- [ ] Puerto de PostgreSQL (generalmente 5432)
- [ ] Usuario de la base de datos
- [ ] Contraseña del usuario
- [ ] Nombre de la base de datos (o confirmación de que se creará)
- [ ] Confirmación de permisos del usuario
- [ ] Acceso de red al servidor (verificado)

## 🎉 ¡Listo!

Si has completado todos los pasos, deberías tener Leoni RPA funcionando correctamente en tu computadora con Windows.

**Próximos pasos**:
- Explora el dashboard
- Crea usuarios adicionales desde el panel de administración
- Configura las funcionalidades de Ventas y Compras
- Personaliza la aplicación según tus necesidades
