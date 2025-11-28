"""
Este script:
1. Construye df_completo desde pipeline.py
2. Demuele y recarga la tabla sedes_data en PostgreSQL/PostGIS
"""

import os
from pathlib import Path

from cnc_mock.etl.pipeline import build_df_completo
from cnc_mock.etl.loader import inicializar_db_desde_dataframe

print("🚀 Iniciando REBUILD de la base de datos...")

# ------------------------------------
# 1. Construir df_completo
# ------------------------------------
df = build_df_completo()
print(f"📦 df_completo construido: {df.shape}")

# ------------------------------------
# 2. Cargar a PostgreSQL
# ------------------------------------
from sqlalchemy import create_engine

DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "sedesdb")

url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print("🔌 Conectando a:", url)

engine = create_engine(url)

# Reemplazar engine global usado por loader
import cnc_mock.etl.loader
cnc_mock.etl.loader.engine = engine

inicializar_db_desde_dataframe(df)

print("🎉 Base de datos reconstruida completamente.")
