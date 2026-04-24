"""Configuración de la aplicación."""

import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

_DEFAULT_DEV_SECRET = "dev-secret-key-change-in-production-12345678901234567890"
_DEV_ENV_NAMES = {"dev", "development", "local", "test", "testing"}


def _to_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_origins(value: Optional[str]) -> list[str]:
    if value is None:
        return ["http://localhost:8000", "http://127.0.0.1:8000"]
    parsed = [item.strip() for item in value.split(",") if item.strip()]
    return parsed


class Settings:
    """Configuración de la aplicación desde variables de entorno."""

    def __init__(self) -> None:
        self.APP_ENV: str = os.getenv("APP_ENV", "development").strip().lower()

        # JWT
        self.SECRET_KEY: str = os.getenv("SECRET_KEY", _DEFAULT_DEV_SECRET)
        self.ALGORITHM: str = os.getenv("ALGORITHM", "HS256")
        self.ACCESS_TOKEN_EXPIRE_MINUTES: int = int(
            os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440")
        )  # 24 horas

        # Database
        self.DB_URL: str = os.getenv(
            "DB_URL",
            "postgresql+asyncpg://postgres:postgres@localhost:5432/leoni_rpa",
        )
        self.DB_STRICT_SCHEMA_CHECK: bool = _to_bool(
            os.getenv("DB_STRICT_SCHEMA_CHECK", "false"), default=False
        )
        self.DB_REQUIRED_TABLES: list[str] = _parse_origins(
            os.getenv("DB_REQUIRED_TABLES", "users")
        )

        # CORS
        self.ALLOWED_ORIGINS: list[str] = _parse_origins(os.getenv("ALLOWED_ORIGINS"))
        self.CORS_ALLOW_CREDENTIALS: bool = _to_bool(
            os.getenv("CORS_ALLOW_CREDENTIALS", "true"), default=True
        )

        # Cookie settings
        self.COOKIE_NAME: str = "access_token"
        self.COOKIE_HTTPONLY: bool = True
        self.COOKIE_SECURE: bool = _to_bool(
            os.getenv("COOKIE_SECURE", "False"), default=False
        )  # False para desarrollo local
        self.COOKIE_SAMESITE: str = os.getenv("COOKIE_SAMESITE", "lax")

        # Admin user creation
        self.ADMIN_EMAIL: Optional[str] = os.getenv("ADMIN_EMAIL")
        self.ADMIN_PASSWORD: Optional[str] = os.getenv("ADMIN_PASSWORD")
        self.ADMIN_NAME: str = os.getenv("ADMIN_NAME", "Administrador")

        # BOM actualización desde SAP (script VBS)
        self.BOM_VBS_PATH: Optional[str] = os.getenv("BOM_VBS_PATH")
        self.BOM_EXPORT_DIR: Optional[str] = os.getenv(
            "BOM_EXPORT_DIR",
            r"C:\Users\anad5004\Documents\Leoni_RPA\bom",
        )
        self.BOM_VBS_TIMEOUT_SEC: int = int(os.getenv("BOM_VBS_TIMEOUT_SEC", "90"))

        # Cross Reference actualización desde SAP (VD59)
        self.CROSS_REFERENCE_VBS_PATH: Optional[str] = os.getenv(
            "CROSS_REFERENCE_VBS_PATH"
        )
        self.CROSS_REFERENCE_EXPORT_DIR: Optional[str] = os.getenv(
            "CROSS_REFERENCE_EXPORT_DIR",
            r"C:\Users\anad5004\Documents\Leoni_RPA\cross",
        )
        self.CROSS_REFERENCE_VBS_TIMEOUT_SEC: int = int(
            os.getenv("CROSS_REFERENCE_VBS_TIMEOUT_SEC", "120")
        )

        # Pesos netos actualización desde SAP (MM17)
        self.PESO_NETO_VBS_PATH: Optional[str] = os.getenv("PESO_NETO_VBS_PATH")
        self.PESO_NETO_EXPORT_DIR: Optional[str] = os.getenv(
            "PESO_NETO_EXPORT_DIR",
            r"C:\Users\anad5004\Documents\Leoni_RPA\peso_neto",
        )
        self.PESO_NETO_EXPORT_FILENAME: str = os.getenv(
            "PESO_NETO_EXPORT_FILENAME", "pesos_netos.xls"
        )
        self.PESO_NETO_VBS_TIMEOUT_SEC: int = int(
            os.getenv("PESO_NETO_VBS_TIMEOUT_SEC", "180")
        )

        self._validate_security_settings()
        self._validate_cors_settings()

    def _validate_security_settings(self) -> None:
        if self.APP_ENV in _DEV_ENV_NAMES:
            return
        secret = (self.SECRET_KEY or "").strip()
        if not secret:
            raise RuntimeError("SECRET_KEY es obligatoria fuera de desarrollo.")
        if secret == _DEFAULT_DEV_SECRET:
            raise RuntimeError(
                "SECRET_KEY insegura: configure un valor real fuera de desarrollo."
            )
        if len(secret) < 32:
            raise RuntimeError(
                "SECRET_KEY insegura: use al menos 32 caracteres fuera de desarrollo."
            )

    def _validate_cors_settings(self) -> None:
        if not self.ALLOWED_ORIGINS:
            raise RuntimeError("ALLOWED_ORIGINS no puede estar vacío.")
        if self.CORS_ALLOW_CREDENTIALS and "*" in self.ALLOWED_ORIGINS:
            raise RuntimeError(
                "Configuración CORS inválida: no use '*' en ALLOWED_ORIGINS "
                "cuando CORS_ALLOW_CREDENTIALS=true."
            )


settings = Settings()

