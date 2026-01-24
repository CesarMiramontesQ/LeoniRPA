"""
Script para corregir la secuencia de IDs de la tabla materiales.

Este script sincroniza la secuencia de PostgreSQL con el máximo ID existente
en la tabla materiales para evitar errores de "duplicate key value".
"""
import asyncio
import sys
from sqlalchemy import text
from app.db.base import AsyncSessionLocal


async def fix_materiales_sequence():
    """Corrige la secuencia de IDs de la tabla materiales."""
    async with AsyncSessionLocal() as session:
        try:
            print("="*50)
            print("CORRECCIÓN: Secuencia de IDs de materiales")
            print("="*50)
            print()
            
            # 1. Obtener el máximo ID actual en la tabla
            result = await session.execute(
                text("SELECT COALESCE(MAX(id), 0) FROM materiales")
            )
            max_id = result.scalar() or 0
            
            print(f"1. ID máximo encontrado en la tabla: {max_id}")
            
            # 2. Obtener el valor actual de la secuencia
            result = await session.execute(
                text("SELECT last_value FROM materiales_id_seq")
            )
            current_seq_value = result.scalar()
            
            print(f"2. Valor actual de la secuencia: {current_seq_value}")
            
            # 3. Si la secuencia está desincronizada, corregirla
            if current_seq_value <= max_id:
                new_value = max_id + 1
                print(f"3. Corrigiendo secuencia a: {new_value}")
                
                await session.execute(
                    text(f"SELECT setval('materiales_id_seq', {new_value}, false)")
                )
                await session.commit()
                
                print("   ✓ Secuencia corregida exitosamente")
            else:
                print("3. ✓ La secuencia está correcta, no se requiere corrección")
            
            # 4. Verificar el nuevo valor
            result = await session.execute(
                text("SELECT last_value FROM materiales_id_seq")
            )
            new_seq_value = result.scalar()
            
            print(f"4. Nuevo valor de la secuencia: {new_seq_value}")
            
            print("\n" + "="*50)
            print("✓ CORRECCIÓN COMPLETADA EXITOSAMENTE")
            print("="*50)
            return True
            
        except Exception as e:
            await session.rollback()
            print(f"\n✗ Error durante la corrección: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


async def main():
    """Función principal."""
    success = await fix_materiales_sequence()
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
