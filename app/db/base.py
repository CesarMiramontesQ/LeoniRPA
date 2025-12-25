"""Configuración de la base de datos SQLAlchemy async."""
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from app.core.config import settings

# Crear engine async
engine = create_async_engine(
    settings.DB_URL,
    echo=False,  # Cambiar a True para debug SQL
    future=True,
)

# Session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

# Base para modelos
Base = declarative_base()


async def get_db() -> AsyncSession:
    """Dependencia para obtener sesión de DB."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

