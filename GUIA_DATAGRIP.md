# Guía para Conectar DataGrip a PostgreSQL - Leoni RPA

Esta guía te ayudará a conectar DataGrip a tu base de datos PostgreSQL.

## Requisitos Previos

- DataGrip instalado
- PostgreSQL corriendo (Postgres.app o servicio de PostgreSQL)
- Base de datos `leoni_rpa` creada

## Pasos para Conectar

### 1. Abrir DataGrip

Abre DataGrip desde tus aplicaciones.

### 2. Crear Nueva Conexión

1. En la ventana principal de DataGrip, haz clic en el botón **"+"** (o **"New"** → **"Data Source"**)
2. Selecciona **"PostgreSQL"** de la lista de bases de datos

### 3. Configurar la Conexión

En la ventana de configuración, completa los siguientes campos:

#### Pestaña "General"

- **Name**: `Leoni RPA PostgreSQL` (o el nombre que prefieras)
- **Host**: `localhost`
- **Port**: `5432`
- **Database**: `leoni_rpa`
- **User**: `postgres`
- **Password**: 
  - Si usas **Postgres.app**: Deja vacío o usa la contraseña que configuraste
  - Si usas **PostgreSQL de Homebrew**: Usa la contraseña que configuraste durante la instalación
  - Si no recuerdas la contraseña, puedes resetearla o crear un nuevo usuario

#### Pestaña "SSH/SSL" (Opcional)

- Generalmente no necesitas configurar nada aquí para conexiones locales

### 4. Descargar el Driver (si es necesario)

Si es la primera vez que usas PostgreSQL en DataGrip:

1. DataGrip te mostrará un mensaje indicando que necesita descargar el driver
2. Haz clic en **"Download"** o **"Download missing driver files"**
3. Espera a que se complete la descarga

### 5. Probar la Conexión

1. Haz clic en el botón **"Test Connection"** (o **"Test"**)
2. Deberías ver un mensaje verde: **"Connection successful"** o **"Successful"**
3. Si hay un error, revisa la sección de "Solución de Problemas" más abajo

### 6. Guardar y Conectar

1. Haz clic en **"OK"** para guardar la configuración
2. La conexión aparecerá en el panel izquierdo bajo **"Database"**
3. Expande la conexión para ver:
   - **Schemas** → **public** → **Tables** → **users**

## Explorar la Base de Datos

### Ver Tablas

1. Expande la conexión en el panel izquierdo
2. Navega a: `leoni_rpa` → `Schemas` → `public` → `Tables`
3. Verás la tabla `users`

### Ver Datos de una Tabla

1. Haz clic derecho en la tabla `users`
2. Selecciona **"Open Table"** o **"Jump to Data"**
3. Verás los datos en formato de tabla

### Ejecutar Consultas SQL

1. Haz clic derecho en la conexión o en la base de datos
2. Selecciona **"New"** → **"Query Console"** (o presiona `Ctrl+Enter` / `Cmd+Enter`)
3. Escribe tu consulta SQL, por ejemplo:
   ```sql
   SELECT * FROM users;
   ```
4. Presiona `Ctrl+Enter` (Windows/Linux) o `Cmd+Enter` (Mac) para ejecutar

### Ver Estructura de una Tabla

1. Haz clic derecho en la tabla `users`
2. Selecciona **"Modify Table"** o **"Properties"**
3. Verás las columnas, tipos de datos, restricciones, etc.

## Configuraciones Adicionales Útiles

### Auto-completado

DataGrip tiene auto-completado inteligente. Mientras escribes SQL, presiona `Ctrl+Space` para ver sugerencias.

### Formato de Código

- Selecciona tu código SQL
- Presiona `Ctrl+Alt+L` (Windows/Linux) o `Cmd+Option+L` (Mac) para formatear

### Exportar Datos

1. Selecciona los datos en una tabla
2. Haz clic derecho → **"Export Data"**
3. Elige el formato (CSV, JSON, Excel, etc.)

### Importar Datos

1. Haz clic derecho en una tabla
2. Selecciona **"Import Data from File"**
3. Sigue el asistente de importación

## Solución de Problemas

### Error: "Connection refused"

**Causa**: PostgreSQL no está corriendo

**Solución**:
- Si usas **Postgres.app**: Abre la aplicación Postgres.app
- Si usas **Homebrew**: Ejecuta `brew services start postgresql@17`
- Verifica que el puerto 5432 esté en uso: `lsof -i :5432`

### Error: "password authentication failed"

**Causa**: Contraseña incorrecta o usuario no existe

**Solución**:
- Verifica que el usuario `postgres` exista
- Si usas Postgres.app, generalmente no requiere contraseña para conexiones locales
- Intenta dejar el campo de contraseña vacío
- Si necesitas crear/resetear contraseña:
  ```bash
  psql -h localhost -U postgres
  ALTER USER postgres PASSWORD 'nueva_contraseña';
  ```

### Error: "database does not exist"

**Causa**: La base de datos `leoni_rpa` no existe

**Solución**:
```bash
psql -h localhost -U postgres
CREATE DATABASE leoni_rpa;
```

### Error: "Driver not found"

**Causa**: El driver de PostgreSQL no está descargado

**Solución**:
1. En la ventana de configuración de conexión
2. Haz clic en **"Download"** junto al campo del driver
3. Espera a que se complete la descarga

### La conexión se cierra automáticamente

**Solución**:
1. Ve a la configuración de la conexión
2. En la pestaña **"Options"**
3. Aumenta el valor de **"Connection timeout"** (por ejemplo, a 30 segundos)

## Consultas Útiles para Leoni RPA

### Ver todos los usuarios

```sql
SELECT id, email, nombre, rol, created_at, activo, last_login 
FROM users 
ORDER BY created_at DESC;
```

### Contar usuarios por rol

```sql
SELECT rol, COUNT(*) as cantidad 
FROM users 
GROUP BY rol;
```

### Ver usuarios activos

```sql
SELECT * FROM users WHERE activo = true;
```

### Ver estructura de la tabla users

```sql
\d users
```

O en DataGrip: Haz clic derecho en la tabla → **"Modify Table"**

## Consejos Adicionales

1. **Guardar consultas frecuentes**: Crea archivos `.sql` en DataGrip para consultas que uses frecuentemente

2. **Usar Bookmarks**: Marca tablas o consultas importantes como favoritos

3. **Historial de consultas**: DataGrip guarda el historial de todas las consultas ejecutadas

4. **Sincronización**: DataGrip puede sincronizar la estructura de la base de datos automáticamente

5. **Comparar esquemas**: Puedes comparar esquemas entre diferentes bases de datos

## Configuración Recomendada para Desarrollo

En la pestaña **"Options"** de la conexión:

- ✅ **Auto-sync**: Activar para mantener la estructura actualizada
- ✅ **Show all databases**: Si quieres ver otras bases de datos
- ⚠️ **Read-only**: Desactivar para poder hacer cambios

## Próximos Pasos

Una vez conectado, puedes:
- Explorar la estructura de las tablas
- Ver y editar datos
- Ejecutar consultas SQL
- Crear nuevas tablas o modificar existentes
- Exportar/importar datos

---

**Nota**: Si tienes problemas con la conexión, verifica que PostgreSQL esté corriendo y que la base de datos `leoni_rpa` exista usando:

```bash
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
psql -h localhost -U postgres -l | grep leoni_rpa
```

