#!/usr/bin/env python
"""
Punto de entrada para arrancar la aplicación FastAPI con uvicorn.

Permite:
- Acceso por IP dentro de la red LAN: host 0.0.0.0 hace la API accesible desde
  otras máquinas (http://IP_DEL_SERVIDOR:PUERTO)
- Uso con NSSM o como servicio de Windows: este script puede ejecutarse de forma
  estable sin reload, ideal para run.bat y para registrar como servicio
- No usar reload=True: modo producción, sin recarga automática
- No agregar dependencias innecesarias: usa solo uvicorn (ya en requirements.txt)

Variables de entorno (ver .env.example):
  API_HOST: host de escucha (default 0.0.0.0)
  API_PORT: puerto (default 8000)
"""
import os

# Cargar .env si existe (python-dotenv ya está en requirements.txt)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import uvicorn

if __name__ == "__main__":
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))

    uvicorn.run(
        "main:app",  # app en main.py de la raíz
        host=host,
        port=port,
        reload=False,  # Desactivado para producción / servicio Windows
    )
