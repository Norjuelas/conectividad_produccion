from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import data, agent
from core.settings import settings  # Asegúrate de tener tu settings básico

app = FastAPI(
    title="GeoData Backend ISED",
    version="1.2.0",
    description="API Geoespacial con soporte de filtros espaciales y estilos dinámicos"
)

# ============================================================================
# CORS (Permite que el Frontend hable con el Backend)
# ============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción cambiar por el dominio real
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# ROUTERS
# ============================================================================
app.include_router(data.router, prefix="/data", tags=["Data"])
app.include_router(agent.router, prefix="/agent", tags=["Agent"])

@app.get("/")
def root():
    return {"status": "OK", "message": "Backend Geoespacial Activo"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)