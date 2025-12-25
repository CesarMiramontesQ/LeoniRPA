from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from datetime import datetime, timedelta

app = FastAPI()
templates = Jinja2Templates(directory="templates")

app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(GZipMiddleware)

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("base.html", {"request": request, "active_page": "dashboard"})


@app.get("/ventas")
async def ventas(request: Request):
    # Datos de ejemplo de ventas
    ventas_data = [
        {
            "id": 1,
            "cliente": "Empresa ABC S.A.",
            "producto": "Cable UTP Cat6",
            "cantidad": 100,
            "precio_unitario": 2.50,
            "total": 250.00,
            "fecha": (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        },
        {
            "id": 2,
            "cliente": "Tech Solutions Ltda.",
            "producto": "Conector RJ45",
            "cantidad": 500,
            "precio_unitario": 0.75,
            "total": 375.00,
            "fecha": (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        },
        {
            "id": 3,
            "cliente": "Redes Industriales",
            "producto": "Switch 24 Puertos",
            "cantidad": 5,
            "precio_unitario": 150.00,
            "total": 750.00,
            "fecha": (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        },
        {
            "id": 4,
            "cliente": "Comunicaciones XYZ",
            "producto": "Cable Coaxial RG6",
            "cantidad": 200,
            "precio_unitario": 1.25,
            "total": 250.00,
            "fecha": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        },
        {
            "id": 5,
            "cliente": "Infraestructura Digital",
            "producto": "Router WiFi 6",
            "cantidad": 10,
            "precio_unitario": 89.99,
            "total": 899.90,
            "fecha": datetime.now().strftime("%Y-%m-%d")
        }
    ]
    
    total_general = sum(venta["total"] for venta in ventas_data)
    
    return templates.TemplateResponse(
        "ventas.html",
        {
            "request": request,
            "ventas": ventas_data,
            "total_general": total_general,
            "active_page": "ventas"
        }
    )


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
