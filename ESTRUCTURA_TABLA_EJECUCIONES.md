# Estructura de la Tabla `purcharsing_execution_history`

Este documento describe la estructura de la tabla de base de datos para registrar el historial de ejecuciones del proceso "Descargar Compras del Mes" desde SAP GUI.

## Descripción

La tabla `purcharsing_execution_history` almacena un registro completo por cada ejecución del proceso RPA, permitiendo auditoría completa de todas las operaciones realizadas.

## Estructura de la Tabla

### SQL de Creación (Referencia)

```sql
-- Crear el tipo ENUM para los estados
CREATE TYPE execution_status AS ENUM ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED');

-- Crear la tabla
CREATE TABLE purcharsing_execution_history (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Usuario que ejecutó el proceso
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    
    -- Periodo de fechas
    fecha_inicio_periodo TIMESTAMP WITH TIME ZONE NOT NULL,
    fecha_fin_periodo TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Archivo generado
    archivo_ruta VARCHAR NULL,
    archivo_nombre VARCHAR NULL,
    
    -- Estado del proceso
    estado execution_status NOT NULL DEFAULT 'PENDING',
    
    -- Fechas de ejecución
    fecha_inicio_ejecucion TIMESTAMP WITH TIME ZONE NULL,
    fecha_fin_ejecucion TIMESTAMP WITH TIME ZONE NULL,
    
    -- Duración en segundos
    duracion_segundos INTEGER NULL,
    
    -- Información técnica
    sistema_sap VARCHAR NULL,
    transaccion VARCHAR NULL,
    maquina VARCHAR NULL,
    
    -- Información de errores
    mensaje_error TEXT NULL,
    stack_trace TEXT NULL
);

-- Crear índices
CREATE INDEX idx_purcharsing_execution_history_user_id ON purcharsing_execution_history(user_id);
CREATE INDEX idx_purcharsing_execution_history_estado ON purcharsing_execution_history(estado);
CREATE INDEX idx_purcharsing_execution_history_created_at ON purcharsing_execution_history(created_at DESC);
```

## Campos de la Tabla

| Campo | Tipo | Descripción | Nullable |
|-------|------|-------------|----------|
| `id` | INTEGER | ID único de la ejecución (auto-incremental) | No |
| `created_at` | TIMESTAMP WITH TIME ZONE | Fecha y hora de creación del registro | No |
| `user_id` | INTEGER | ID del usuario que ejecutó el proceso (FK a `users.id`) | No |
| `fecha_inicio_periodo` | TIMESTAMP WITH TIME ZONE | Fecha de inicio del periodo de compras | No |
| `fecha_fin_periodo` | TIMESTAMP WITH TIME ZONE | Fecha de fin del periodo de compras | No |
| `archivo_ruta` | VARCHAR | Ruta completa del archivo generado | Sí |
| `archivo_nombre` | VARCHAR | Nombre del archivo generado | Sí |
| `estado` | execution_status | Estado actual del proceso (ENUM) | No |
| `fecha_inicio_ejecucion` | TIMESTAMP WITH TIME ZONE | Fecha y hora de inicio de la ejecución | Sí |
| `fecha_fin_ejecucion` | TIMESTAMP WITH TIME ZONE | Fecha y hora de fin de la ejecución | Sí |
| `duracion_segundos` | INTEGER | Duración total del proceso en segundos | Sí |
| `sistema_sap` | VARCHAR | Sistema SAP utilizado | Sí |
| `transaccion` | VARCHAR | Transacción SAP ejecutada | Sí |
| `maquina` | VARCHAR | Máquina/host donde se ejecutó el proceso | Sí |
| `mensaje_error` | TEXT | Mensaje de error descriptivo (si aplica) | Sí |
| `stack_trace` | TEXT | Stack trace completo del error (si aplica) | Sí |

## Estados del Proceso

El campo `estado` puede tener los siguientes valores:

- **PENDING**: La ejecución ha sido creada pero aún no ha comenzado
- **RUNNING**: El proceso está en ejecución
- **SUCCESS**: El proceso se completó exitosamente
- **FAILED**: El proceso falló durante la ejecución
- **CANCELLED**: El proceso fue cancelado antes de completarse

## Relaciones

- **users**: La tabla tiene una relación de clave foránea con la tabla `users` a través del campo `user_id`. Esto permite obtener información del usuario que ejecutó el proceso.

## Índices

La tabla incluye los siguientes índices para optimizar las consultas:

1. `idx_purcharsing_execution_history_user_id`: Para búsquedas rápidas por usuario
2. `idx_purcharsing_execution_history_estado`: Para filtrar por estado del proceso
3. `idx_purcharsing_execution_history_created_at`: Para ordenar por fecha de creación (últimas primero)

## Uso con SQLAlchemy

La tabla se crea automáticamente al ejecutar el script de inicialización:

```bash
python -m app.db.init_db
```

O desde Python:

```python
from app.db.init_db import init_db
import asyncio

asyncio.run(init_db())
```

## Ejemplos de Consultas

### Obtener todas las ejecuciones de un usuario

```sql
SELECT e.*, u.email, u.nombre
FROM purcharsing_execution_history e
JOIN users u ON e.user_id = u.id
WHERE e.user_id = 1
ORDER BY e.created_at DESC;
```

### Obtener ejecuciones fallidas

```sql
SELECT e.*, u.email
FROM purcharsing_execution_history e
JOIN users u ON e.user_id = u.id
WHERE e.estado = 'FAILED'
ORDER BY e.created_at DESC;
```

### Obtener ejecuciones del último mes

```sql
SELECT e.*, u.email
FROM purcharsing_execution_history e
JOIN users u ON e.user_id = u.id
WHERE e.created_at >= NOW() - INTERVAL '1 month'
ORDER BY e.created_at DESC;
```

### Estadísticas de ejecuciones por estado

```sql
SELECT 
    estado,
    COUNT(*) as total,
    AVG(duracion_segundos) as duracion_promedio_segundos
FROM purcharsing_execution_history
GROUP BY estado;
```

## Notas de Implementación

- La tabla utiliza `TIMESTAMP WITH TIME ZONE` para manejar correctamente las zonas horarias
- El campo `stack_trace` puede ser muy largo, por lo que se usa el tipo `TEXT`
- La relación con `users` tiene `ON DELETE RESTRICT` para prevenir la eliminación de usuarios con ejecuciones asociadas
- Los campos de información técnica (`sistema_sap`, `transaccion`, `maquina`) son opcionales para permitir flexibilidad

