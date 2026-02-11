@echo off
REM Run.bat - Arranca la API FastAPI
REM Permite: acceso por IP en la red LAN (http://IP_DEL_SERVIDOR:PUERTO)
REM Compatible con NSSM para registrar como servicio de Windows
REM No usa reload - modo producción

cd /d "%~dp0"

REM Activar entorno virtual si existe
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

python start.py
