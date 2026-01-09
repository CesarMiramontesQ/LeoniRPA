"""Script de migración para actualizar la tabla users con los nuevos campos."""
import asyncio
from sqlalchemy import text
from app.db.base import engine


async def migrate_users_table():
    """Migra la tabla users a la nueva estructura."""
    async with engine.begin() as conn:
        print("🔄 Iniciando migración de la tabla users...")
        
        try:
            # Verificar si la columna password_hash existe
            result = await conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'password_hash'
            """))
            has_password_hash = result.scalar() is not None
            
            if not has_password_hash:
                print("  → Renombrando hashed_password a password_hash...")
                await conn.execute(text("ALTER TABLE users RENAME COLUMN hashed_password TO password_hash"))
            
            # Verificar si la columna nombre existe
            result = await conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'nombre'
            """))
            has_nombre = result.scalar() is not None
            
            if not has_nombre:
                print("  → Renombrando full_name a nombre...")
                await conn.execute(text("ALTER TABLE users RENAME COLUMN full_name TO nombre"))
            
            # Verificar si la columna rol existe
            result = await conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'rol'
            """))
            has_rol = result.scalar() is not None
            
            if not has_rol:
                print("  → Renombrando role a rol...")
                await conn.execute(text("ALTER TABLE users RENAME COLUMN role TO rol"))
                # Actualizar valores de rol
                print("  → Actualizando valores de rol...")
                await conn.execute(text("UPDATE users SET rol = 'operador' WHERE rol = 'user'"))
            
            # Verificar si la columna activo existe
            result = await conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'activo'
            """))
            has_activo = result.scalar() is not None
            
            if not has_activo:
                print("  → Renombrando is_active a activo...")
                await conn.execute(text("ALTER TABLE users RENAME COLUMN is_active TO activo"))
            
            # Verificar si la columna last_login existe
            result = await conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'last_login'
            """))
            has_last_login = result.scalar() is not None
            
            if not has_last_login:
                print("  → Agregando columna last_login...")
                await conn.execute(text("""
                    ALTER TABLE users 
                    ADD COLUMN last_login TIMESTAMP WITH TIME ZONE
                """))
            
            # Eliminar columna updated_at si existe
            result = await conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'updated_at'
            """))
            has_updated_at = result.scalar() is not None
            
            if has_updated_at:
                print("  → Eliminando columna updated_at...")
                await conn.execute(text("ALTER TABLE users DROP COLUMN updated_at"))
            
            print("✅ Migración completada exitosamente!")
            print("\n📋 Resumen de cambios:")
            print("   - hashed_password → password_hash")
            print("   - full_name → nombre")
            print("   - role → rol (valores: admin, operador, auditor)")
            print("   - is_active → activo")
            print("   - Agregado: last_login")
            print("   - Eliminado: updated_at")
            
        except Exception as e:
            print(f"❌ Error durante la migración: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(migrate_users_table())
