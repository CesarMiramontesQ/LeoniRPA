"""Funciones de seguridad: hash de passwords y JWT."""
import bcrypt
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from app.core.config import settings


def hash_password(password: str) -> str:
    """Hashea un password usando bcrypt.
    
    Nota: bcrypt tiene una limitación de 72 bytes. Si el password es más largo,
    se truncará automáticamente a 72 bytes.
    """
    # Limpiar el password (eliminar espacios al inicio/final)
    password = password.strip()
    
    # Convertir a bytes y truncar a 72 bytes exactos si es necesario
    password_bytes = password.encode('utf-8')
    if len(password_bytes) > 72:
        password_bytes = password_bytes[:72]
    
    # Generar salt y hash usando bcrypt directamente
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password_bytes, salt)
    
    # Devolver como string (prefijo $2b$ para compatibilidad con passlib)
    return hashed.decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifica si un password coincide con el hash."""
    # Limpiar el password
    plain_password = plain_password.strip()
    
    # Convertir a bytes y truncar si es necesario
    password_bytes = plain_password.encode('utf-8')
    if len(password_bytes) > 72:
        password_bytes = password_bytes[:72]
    
    # Convertir hash a bytes si es string
    if isinstance(hashed_password, str):
        hashed_bytes = hashed_password.encode('utf-8')
    else:
        hashed_bytes = hashed_password
    
    # Verificar usando bcrypt
    return bcrypt.checkpw(password_bytes, hashed_bytes)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Crea un token JWT."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str) -> Optional[dict]:
    """Decodifica y valida un token JWT."""
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return payload
    except JWTError as e:
        # Debug: imprimir el error específico
        print(f"[DECODE_TOKEN] Error al decodificar token: {type(e).__name__}: {str(e)}")
        print(f"[DECODE_TOKEN] Token recibido (primeros 50 chars): {token[:50]}...")
        print(f"[DECODE_TOKEN] SECRET_KEY (primeros 20 chars): {settings.SECRET_KEY[:20]}...")
        print(f"[DECODE_TOKEN] ALGORITHM: {settings.ALGORITHM}")
        return None

