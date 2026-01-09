# Configuración de PostgreSQL para Leoni RPA

Este proyecto utiliza PostgreSQL como base de datos. Sigue estos pasos para configurarlo correctamente.

## Requisitos Previos

- PostgreSQL instalado (versión 12 o superior)
- Acceso de administrador a PostgreSQL

## Pasos de Configuración

### 1. Instalar PostgreSQL

#### macOS (usando Homebrew)
```bash
brew install postgresql@15
brew services start postgresql@15
```

#### Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

#### Windows
Descarga e instala desde: https://www.postgresql.org/download/windows/

### 2. Crear la Base de Datos

Conéctate a PostgreSQL y crea la base de datos:

```bash
# Conectar a PostgreSQL (como usuario postgres)
psql -U postgres

# Crear la base de datos
CREATE DATABASE leoni_rpa;

# (Opcional) Crear un usuario específico para la aplicación
CREATE USER leoni_user WITH PASSWORD 'tu_contraseña_segura';
GRANT ALL PRIVILEGES ON DATABASE leoni_rpa TO leoni_user;

# Salir de psql
\q
```

### 3. Configurar Variables de Entorno

Crea un archivo `.env` en la raíz del proyecto con la siguiente configuración:

```env
# Database Configuration
# Formato: postgresql+asyncpg://usuario:contraseña@host:puerto/nombre_base_datos
DB_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/leoni_rpa
```

**Ejemplos de URLs de conexión:**

- Usuario por defecto (postgres):
  ```
  DB_URL=postgresql+asyncpg://postgres:tu_contraseña@localhost:5432/leoni_rpa
  ```

- Usuario personalizado:
  ```
  DB_URL=postgresql+asyncpg://leoni_user:tu_contraseña@localhost:5432/leoni_rpa
  ```

- Base de datos remota:
  ```
  DB_URL=postgresql+asyncpg://usuario:contraseña@host_remoto:5432/leoni_rpa
  ```

### 4. Instalar Dependencias

Asegúrate de tener el driver de PostgreSQL instalado:

```bash
# Activar entorno virtual
source venv/bin/activate

# Instalar/actualizar dependencias
pip install -r requirements.txt
```

El archivo `requirements.txt` incluye `asyncpg`, que es el driver asíncrono para PostgreSQL.

### 5. Inicializar la Base de Datos

Ejecuta el script de inicialización para crear las tablas:

```bash
python -m app.db.init_db
```

O la base de datos se inicializará automáticamente al iniciar la aplicación.

## Verificar la Conexión

Puedes verificar que la conexión funciona correctamente ejecutando:

```bash
python -c "from app.core.config import settings; print(f'DB URL: {settings.DB_URL}')"
```

## Solución de Problemas

### Error: "password authentication failed"
- Verifica que el usuario y contraseña en `DB_URL` sean correctos
- Asegúrate de que el usuario tenga permisos en la base de datos

### Error: "database does not exist"
- Verifica que la base de datos `leoni_rpa` haya sido creada
- Revisa el nombre de la base de datos en `DB_URL`

### Error: "connection refused"
- Verifica que PostgreSQL esté ejecutándose: `pg_isready` o `sudo systemctl status postgresql`
- Verifica que el puerto (por defecto 5432) sea correcto
- Si PostgreSQL está en otro host, verifica la conectividad de red

### Error: "module 'asyncpg' not found"
- Instala las dependencias: `pip install -r requirements.txt`
- Asegúrate de estar usando el entorno virtual correcto

## Migración desde SQLite

Si tenías datos en SQLite y necesitas migrarlos a PostgreSQL:

1. Exporta los datos de SQLite a SQL
2. Ajusta el SQL para que sea compatible con PostgreSQL
3. Importa los datos en PostgreSQL

**Nota**: Las estructuras de datos pueden variar entre SQLite y PostgreSQL. Revisa los tipos de datos y ajusta según sea necesario.

## Conexión desde DataGrip u otros clientes

Para conectar desde DataGrip u otros clientes de base de datos:

- **Host**: localhost (o la IP del servidor)
- **Puerto**: 5432 (por defecto)
- **Base de datos**: leoni_rpa
- **Usuario**: postgres (o el usuario que hayas configurado)
- **Contraseña**: la contraseña que configuraste

