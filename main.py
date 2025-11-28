from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from routers import data, agent 


app = FastAPI(
    title="GeoData Backend ISED",
    version="1.3.0",
    description="API + Visor Web Conectividad Educativa"
)


# ---------- CORS ----------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- RUTAS API ----------
app.include_router(data.router, prefix="/data")
app.include_router(agent.router, prefix="/agent")


# ---------- ARCHIVOS FRONT ----------
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def index():
    return FileResponse("static/index.html")
