"""Configuración de la aplicación."""
import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Configuración de la aplicación desde variables de entorno."""
    
    # JWT
    SECRET_KEY: str = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production-12345678901234567890")
    ALGORITHM: str = os.getenv("ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))  # 24 horas
    
    # Database
    # PostgreSQL connection string format: postgresql+asyncpg://user:password@host:port/database
    DB_URL: str = os.getenv(
        "DB_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:5432/leoni_rpa"
    )
    
    # Cookie settings
    COOKIE_NAME: str = "access_token"
    COOKIE_HTTPONLY: bool = True
    COOKIE_SECURE: bool = os.getenv("COOKIE_SECURE", "False").lower() == "true"  # False para desarrollo local
    COOKIE_SAMESITE: str = os.getenv("COOKIE_SAMESITE", "lax")  # lax permite cookies en redirecciones POST->GET
    
    # Admin user creation
    ADMIN_EMAIL: Optional[str] = os.getenv("ADMIN_EMAIL")
    ADMIN_PASSWORD: Optional[str] = os.getenv("ADMIN_PASSWORD")
    ADMIN_NAME: str = os.getenv("ADMIN_NAME", "Administrador")


settings = Settings()

