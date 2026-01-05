"""Operaciones CRUD para usuarios."""
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.db.models import User
from app.core.security import hash_password


async def get_user_by_email(db: AsyncSession, email: str) -> Optional[User]:
    """Obtiene un usuario por email."""
    result = await db.execute(select(User).where(User.email == email))
    return result.scalar_one_or_none()


async def get_user_by_id(db: AsyncSession, user_id: int) -> Optional[User]:
    """Obtiene un usuario por ID."""
    result = await db.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()


async def create_user(
    db: AsyncSession,
    email: str,
    password: str,
    full_name: Optional[str] = None,
    role: str = "user"
) -> User:
    """Crea un nuevo usuario."""
    hashed_pwd = hash_password(password)
    db_user = User(
        email=email,
        hashed_password=hashed_pwd,
        full_name=full_name,
        role=role,
        is_active=True
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user


async def update_user_role(
    db: AsyncSession,
    user_id: int,
    role: str
) -> Optional[User]:
    """Actualiza el rol de un usuario."""
    user = await get_user_by_id(db, user_id)
    if user:
        user.role = role
        await db.commit()
        await db.refresh(user)
    return user


async def update_user(
    db: AsyncSession,
    user_id: int,
    email: str,
    full_name: Optional[str] = None,
    role: str = "user",
    password: Optional[str] = None
) -> Optional[User]:
    """Actualiza un usuario."""
    user = await get_user_by_id(db, user_id)
    if not user:
        return None
    
    # Verificar si el email ya existe en otro usuario
    existing_user = await get_user_by_email(db, email)
    if existing_user and existing_user.id != user_id:
        raise ValueError("El email ya está registrado")
    
    user.email = email
    user.full_name = full_name
    user.role = role
    
    # Solo actualizar contraseña si se proporciona
    if password:
        user.hashed_password = hash_password(password)
    
    await db.commit()
    await db.refresh(user)
    return user

