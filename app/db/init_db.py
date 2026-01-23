"""Inicializa la base de datos creando las tablas."""
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from app.db.base import Base
from app.core.config import settings
# Importar modelos para que SQLAlchemy los registre
from app.db.models import User, ExecutionHistory, SalesExecutionHistory, Material, PrecioMaterial, Compra, PaisOrigenMaterial


async def init_db():
    """Crea todas las tablas en la base de datos."""
    engine = create_async_engine(settings.DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
    print("✓ Base de datos inicializada correctamente")


if __name__ == "__main__":
    asyncio.run(init_db())

