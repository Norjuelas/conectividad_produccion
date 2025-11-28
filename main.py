from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from routers import data, agent

app = FastAPI(
    title="GeoData Backend ISED",
    version="1.3.0",
    description="API + Visor Web Conectividad Educativa"
)

# ========================= CORS GLOBAL =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========================= API ROUTERS =========================
app.include_router(data.router, prefix="/data")
app.include_router(agent.router, prefix="/agent")

# ========================= FRONT STATIC ========================
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def serve_frontend():
    return FileResponse("static/index.html")
