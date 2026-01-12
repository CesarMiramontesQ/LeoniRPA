"""
Ejemplo de uso de las funciones CRUD para ExecutionHistory.

Este script muestra cómo crear, actualizar y consultar ejecuciones del proceso
"Descargar Compras del Mes".
"""
import asyncio
from datetime import datetime, timedelta
from app.db.base import get_db
from app.db import crud
from app.db.models import ExecutionStatus


async def ejemplo_crear_ejecucion():
    """Ejemplo de cómo crear una nueva ejecución."""
    async for db in get_db():
        # Crear una nueva ejecución
        execution = await crud.create_execution(
            db=db,
            user_id=1,  # ID del usuario que ejecuta
            fecha_inicio_periodo=datetime(2024, 1, 1),
            fecha_fin_periodo=datetime(2024, 1, 31),
            sistema_sap="SAP ECC 6.0",
            transaccion="ME23N",
            maquina="DESKTOP-ABC123"
        )
        print(f"✓ Ejecución creada con ID: {execution.id}")
        print(f"  Estado inicial: {execution.estado.value}")
        return execution.id


async def ejemplo_actualizar_ejecucion_en_proceso(execution_id: int):
    """Ejemplo de cómo actualizar una ejecución cuando comienza."""
    async for db in get_db():
        fecha_inicio = datetime.now()
        
        execution = await crud.update_execution_status(
            db=db,
            execution_id=execution_id,
            estado=ExecutionStatus.RUNNING,
            fecha_inicio_ejecucion=fecha_inicio
        )
        print(f"✓ Ejecución {execution_id} actualizada a RUNNING")
        return execution


async def ejemplo_actualizar_ejecucion_exitosa(execution_id: int):
    """Ejemplo de cómo actualizar una ejecución cuando termina exitosamente."""
    async for db in get_db():
        fecha_fin = datetime.now()
        
        # Obtener la ejecución para calcular la duración
        execution = await crud.get_execution_by_id(db, execution_id)
        if execution and execution.fecha_inicio_ejecucion:
            duracion = int((fecha_fin - execution.fecha_inicio_ejecucion).total_seconds())
        else:
            duracion = None
        
        execution = await crud.update_execution_status(
            db=db,
            execution_id=execution_id,
            estado=ExecutionStatus.SUCCESS,
            fecha_fin_ejecucion=fecha_fin,
            duracion_segundos=duracion,
            archivo_ruta="/ruta/archivos/compras_2024_01.xlsx",
            archivo_nombre="compras_2024_01.xlsx"
        )
        print(f"✓ Ejecución {execution_id} completada exitosamente")
        print(f"  Duración: {duracion} segundos")
        print(f"  Archivo: {execution.archivo_nombre}")


async def ejemplo_actualizar_ejecucion_fallida(execution_id: int, error: Exception):
    """Ejemplo de cómo actualizar una ejecución cuando falla."""
    import traceback
    
    async for db in get_db():
        fecha_fin = datetime.now()
        
        # Obtener la ejecución para calcular la duración
        execution = await crud.get_execution_by_id(db, execution_id)
        if execution and execution.fecha_inicio_ejecucion:
            duracion = int((fecha_fin - execution.fecha_inicio_ejecucion).total_seconds())
        else:
            duracion = None
        
        execution = await crud.update_execution_status(
            db=db,
            execution_id=execution_id,
            estado=ExecutionStatus.FAILED,
            fecha_fin_ejecucion=fecha_fin,
            duracion_segundos=duracion,
            mensaje_error=str(error),
            stack_trace=traceback.format_exc()
        )
        print(f"✗ Ejecución {execution_id} falló")
        print(f"  Error: {execution.mensaje_error}")


async def ejemplo_listar_ejecuciones():
    """Ejemplo de cómo listar ejecuciones."""
    async for db in get_db():
        # Listar todas las ejecuciones
        executions = await crud.list_executions(db, limit=10)
        print(f"\n✓ Total de ejecuciones encontradas: {len(executions)}")
        
        for exec in executions:
            print(f"\n  ID: {exec.id}")
            print(f"  Usuario: {exec.user.email if exec.user else 'N/A'}")
            print(f"  Estado: {exec.estado.value}")
            print(f"  Periodo: {exec.fecha_inicio_periodo.date()} - {exec.fecha_fin_periodo.date()}")
            print(f"  Creada: {exec.created_at}")
            if exec.duracion_segundos:
                print(f"  Duración: {exec.duracion_segundos} segundos")


async def ejemplo_listar_ejecuciones_por_usuario(user_id: int):
    """Ejemplo de cómo listar ejecuciones de un usuario específico."""
    async for db in get_db():
        executions = await crud.list_executions(db, user_id=user_id, limit=20)
        print(f"\n✓ Ejecuciones del usuario {user_id}: {len(executions)}")
        
        for exec in executions:
            print(f"  - {exec.id}: {exec.estado.value} ({exec.created_at})")


async def ejemplo_listar_ejecuciones_por_estado(estado: ExecutionStatus):
    """Ejemplo de cómo listar ejecuciones por estado."""
    async for db in get_db():
        executions = await crud.list_executions(db, estado=estado, limit=50)
        print(f"\n✓ Ejecuciones con estado {estado.value}: {len(executions)}")
        
        for exec in executions:
            print(f"  - ID {exec.id}: Usuario {exec.user_id} - {exec.created_at}")


async def ejemplo_obtener_ejecucion(execution_id: int):
    """Ejemplo de cómo obtener una ejecución específica."""
    async for db in get_db():
        execution = await crud.get_execution_by_id(db, execution_id)
        
        if execution:
            print(f"\n✓ Ejecución {execution_id}:")
            print(f"  Usuario: {execution.user.email if execution.user else 'N/A'}")
            print(f"  Estado: {execution.estado.value}")
            print(f"  Periodo: {execution.fecha_inicio_periodo} - {execution.fecha_fin_periodo}")
            print(f"  Archivo: {execution.archivo_nombre or 'N/A'}")
            if execution.mensaje_error:
                print(f"  Error: {execution.mensaje_error}")
        else:
            print(f"✗ Ejecución {execution_id} no encontrada")


async def ejemplo_flujo_completo():
    """Ejemplo de un flujo completo de ejecución."""
    print("=== Ejemplo de Flujo Completo ===\n")
    
    # 1. Crear ejecución
    print("1. Creando nueva ejecución...")
    execution_id = await ejemplo_crear_ejecucion()
    
    # 2. Iniciar ejecución
    print("\n2. Iniciando ejecución...")
    await ejemplo_actualizar_ejecucion_en_proceso(execution_id)
    
    # Simular procesamiento
    print("\n3. Procesando... (simulando 2 segundos)")
    await asyncio.sleep(2)
    
    # 3. Completar ejecución exitosamente
    print("\n4. Completando ejecución...")
    await ejemplo_actualizar_ejecucion_exitosa(execution_id)
    
    # 4. Obtener detalles
    print("\n5. Obteniendo detalles de la ejecución...")
    await ejemplo_obtener_ejecucion(execution_id)


if __name__ == "__main__":
    print("Ejemplos de uso de ExecutionHistory CRUD")
    print("=" * 50)
    
    # Descomentar el ejemplo que quieras ejecutar:
    
    # Ejemplo completo
    # asyncio.run(ejemplo_flujo_completo())
    
    # Ejemplos individuales
    # asyncio.run(ejemplo_crear_ejecucion())
    # asyncio.run(ejemplo_listar_ejecuciones())
    # asyncio.run(ejemplo_listar_ejecuciones_por_usuario(1))
    # asyncio.run(ejemplo_listar_ejecuciones_por_estado(ExecutionStatus.SUCCESS))
    # asyncio.run(ejemplo_obtener_ejecucion(1))

