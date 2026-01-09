"""Script para crear usuario administrador."""
import asyncio
import sys
from app.db.base import AsyncSessionLocal
from app.db import crud
from app.core.config import settings
from app.core.security import hash_password


async def create_admin():
    """Crea un usuario administrador."""
    async with AsyncSessionLocal() as db:
        # Verificar si ya existe un admin
        admin_user = await crud.get_user_by_email(db, settings.ADMIN_EMAIL or "admin@example.com")
        
        if admin_user:
            print(f"⚠️  Ya existe un usuario con email: {admin_user.email}")
            response = input("¿Deseas cambiar su contraseña? (s/n): ")
            if response.lower() == 's':
                new_password = input("Nueva contraseña (mínimo 8 caracteres, máximo 72 bytes): ").strip()
                if len(new_password) < 8:
                    print("❌ La contraseña debe tener al menos 8 caracteres")
                    return
                if len(new_password.encode('utf-8')) > 72:
                    print("❌ La contraseña es demasiado larga (máximo 72 bytes)")
                    return
                admin_user.password_hash = hash_password(new_password)
                admin_user.rol = "admin"
                await db.commit()
                print(f"✅ Contraseña actualizada para {admin_user.email}")
            return
        
        # Obtener datos desde variables de entorno o input
        email = settings.ADMIN_EMAIL
        password = settings.ADMIN_PASSWORD
        nombre = settings.ADMIN_NAME
        
        if not email:
            email = input("Email del administrador: ")
        
        if not password:
            password = input("Contraseña del administrador (mínimo 8 caracteres, máximo 72 bytes): ").strip()
            if len(password) < 8:
                print("❌ La contraseña debe tener al menos 8 caracteres")
                return
            if len(password.encode('utf-8')) > 72:
                print("❌ La contraseña es demasiado larga (máximo 72 bytes)")
                return
        
        if not nombre:
            nombre = input("Nombre completo (opcional): ") or None
        
        try:
            admin = await crud.create_user(
                db=db,
                email=email,
                password=password,
                nombre=nombre,
                rol="admin"
            )
            print(f"✅ Usuario administrador creado exitosamente:")
            print(f"   Email: {admin.email}")
            print(f"   Rol: {admin.rol}")
            print(f"   ID: {admin.id}")
        except Exception as e:
            print(f"❌ Error al crear usuario: {e}")
            sys.exit(1)


if __name__ == "__main__":
    asyncio.run(create_admin())

