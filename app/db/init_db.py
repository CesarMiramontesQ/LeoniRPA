"""Utilidades de inicialización y verificación de base de datos."""
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from app.db.base import Base
from app.core.config import settings
# Importar modelos para que SQLAlchemy los registre
from app.db.models import (
    Parte,
    TradingGood,
    TradingGoodHistorial,
    Kathoden,
    KathodenHistorial,
    Bom,
    BomRevision,
    BomItem,
    User,
    ExecutionHistory,
    SalesExecutionHistory,
    FraccionArancelariaHistorial,
    Material,
    Semiterminado,
    SemiterminadoHistorial,
    PrecioMaterial,
    Compra,
    PaisOrigenMaterial,
    ClienteGrupo,
    Venta,
    CargaProveedor,
    CargaProveedoresNacional,
    CargaProveedoresNacionalHistorial,
    CargaCliente,
    PrecioVenta,
    MasterUnificadoVirtuales,
    MasterUnificadoVirtualHistorial,
)


async def init_db():
    """Crea todas las tablas en la base de datos (uso explícito)."""
    engine = create_async_engine(settings.DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
    print("✓ Base de datos inicializada correctamente")


async def verify_db_connection() -> None:
    """Valida conectividad de BD sin alterar esquema ni datos."""
    engine = create_async_engine(settings.DB_URL, echo=False)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
            if settings.DB_STRICT_SCHEMA_CHECK:
                result = await conn.execute(
                    text(
                        """
                        SELECT tablename
                        FROM pg_catalog.pg_tables
                        WHERE schemaname = 'public'
                        """
                    )
                )
                existing_tables = {row[0] for row in result.fetchall()}
                missing = [
                    table
                    for table in settings.DB_REQUIRED_TABLES
                    if table and table not in existing_tables
                ]
                if missing:
                    raise RuntimeError(
                        "Esquema incompleto. Faltan tablas requeridas: "
                        + ", ".join(sorted(missing))
                    )
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(init_db())

