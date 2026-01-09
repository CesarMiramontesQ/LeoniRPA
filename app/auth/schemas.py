"""Schemas Pydantic para autenticación."""
from pydantic import BaseModel, EmailStr, field_validator
from typing import Optional


class UserRegister(BaseModel):
    """Schema para registro de usuario."""
    email: EmailStr
    password: str
    confirm_password: str
    nombre: Optional[str] = None
    
    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("El password debe tener al menos 8 caracteres")
        if len(v.encode('utf-8')) > 72:
            raise ValueError("El password es demasiado largo (máximo 72 bytes)")
        return v
    
    @field_validator("confirm_password")
    @classmethod
    def validate_confirm_password(cls, v: str, info) -> str:
        if "password" in info.data and v != info.data["password"]:
            raise ValueError("Los passwords no coinciden")
        return v


class UserLogin(BaseModel):
    """Schema para login."""
    email: EmailStr
    password: str


class UserResponse(BaseModel):
    """Schema de respuesta con datos de usuario."""
    id: int
    email: str
    nombre: Optional[str]
    rol: str
    activo: bool
    created_at: Optional[str] = None
    last_login: Optional[str] = None
    
    class Config:
        from_attributes = True


class TokenData(BaseModel):
    """Datos del token."""
    user_id: Optional[int] = None
    email: Optional[str] = None

