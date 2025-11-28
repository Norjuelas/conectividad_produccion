import pandas as pd
import geopandas as gpd
from sqlalchemy import text
from cnc_mock.db.session import engine

def inicializar_db_desde_dataframe(df_completo: pd.DataFrame):
    """
    Carga un único DataFrame maestro (como el de 223 columnas) a una tabla
    'sedes_data' en PostgreSQL/PostGIS.
    La tabla se destruye y se crea cada vez (modo idempotente).
    """
    print("🚀 Iniciando carga a PostgreSQL/PostGIS...")

    # 1. Asegurar GeoDataFrame
    if not isinstance(df_completo, gpd.GeoDataFrame):
        print("🔄 Convirtiendo DataFrame a GeoDataFrame (POINT)...")
        gdf = gpd.GeoDataFrame(
            df_completo,
            geometry=gpd.points_from_xy(
                df_completo["longitud"],
                df_completo["latitud"]
            ),
            crs="EPSG:4326"
        )
    else:
        gdf = df_completo.copy()

    gdf = gdf.rename_geometry("geom")

    # 2. Demoler tabla previa
    from sqlalchemy import text
    with engine.begin() as conn:
        print("💣 Eliminando tabla previa sedes_data (si existe) y habilitando PostGIS...")
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        conn.execute(text("DROP TABLE IF EXISTS sedes_data CASCADE;"))

    # 3. Cargar datos
    print(f"📦 Cargando {len(gdf)} registros con {len(gdf.columns)} columnas...")
    gdf.to_postgis(
        name="sedes_data",
        con=engine,
        if_exists="replace",
        index=True,
        index_label="id",
        chunksize=5000,
        dtype={"geom": "Geometry(POINT, 4326)"}
    )

    # 4. Índices
    with engine.begin() as conn:
        print("⚡ Creando índices de rendimiento...")
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_sedes_geom
            ON sedes_data USING GIST(geom);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_sedes_year
            ON sedes_data (year_reporte);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_sedes_codigo
            ON sedes_data (sede_codigo);
        """))

    print("✅ ¡Carga completada correctamente!")
    print("🐘 Tabla sedes_data lista en PostgreSQL/PostGIS.")
