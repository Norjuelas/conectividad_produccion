import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, List, Dict, Any

# ============================================================================
# CONFIGURACIÓN (Esto podría venir de tu YAML, pero lo dejamos aquí por ahora)
# ============================================================================
DATASETS = {
    "sedes_mock": {
        "file": "db/sedes_mock.csv", 
        "lat_col": "latitud",
        "lon_col": "longitud",
        "filters": ["year_reporte", "zona", "DPTO_CNMBR", "MPIO_CNMBR"]
    },
    # --- NUEVO DATASET ---
    "sena_ised": {
        "file": "db/sena_ised.parquet",
        "lat_col": "latitud",
        "lon_col": "longitud",
        "filters": ["year_reporte","departamento","d_conectado","sector_atencion"]
    }
}

class QueryEngine:
    def __init__(self):
        self._cache = {}

    def _load_df(self, dataset_name: str):
        if dataset_name not in self._cache:
            config = DATASETS.get(dataset_name)
            if not config:
                raise ValueError(f"Dataset '{dataset_name}' no configurado.")

            file_path = Path(config["file"])

            # log.info(f"📄 Cargando dataset {dataset_name} desde {file_path}")

            if dataset_name == "sena_ised":
                df = pd.read_parquet(file_path)  # 🚀 ahora súper rápido
            else:
                df = pd.read_csv(file_path, low_memory=False)

            self._cache[dataset_name] = df

        return self._cache[dataset_name].copy()

    def _filter_spatial(self, df: pd.DataFrame, bbox: str, lat_col: str, lon_col: str) -> pd.DataFrame:
        """Filtra por Bounding Box (min_lon, min_lat, max_lon, max_lat)"""
        try:
            min_lon, min_lat, max_lon, max_lat = map(float, bbox.split(','))
            return df[
                (df[lon_col] >= min_lon) & (df[lon_col] <= max_lon) &
                (df[lat_col] >= min_lat) & (df[lat_col] <= max_lat)
            ]
        except (ValueError, AttributeError):
            return df
    def _df_to_geojson_optimized(self, df: pd.DataFrame, lat_col: str, lon_col: str) -> dict:
        # 1) Convertir NaN → None para evitar el error JSON
        df = df.replace({float('nan'): None}).dropna(subset=[lat_col, lon_col])

        # 2) Asegurar que lat/lon sean numéricos
        df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
        df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
        df = df.dropna(subset=[lat_col, lon_col])  # eliminar filas sin coordenadas reales

        # 3) Construcción del GeoJSON optimizado
        coords = df[[lon_col, lat_col]].values
        props = df.drop(columns=[lat_col, lon_col]).where(pd.notnull(df), None).to_dict(orient="records")

        return {
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": p
                }
                for (lon, lat), p in zip(coords, props)
            ]
        }


    def get_data(self, dataset_id: str, format: str, filters: Dict[str, Any], bbox: Optional[str] = None, elevation_col: Optional[str] = None):
        config = DATASETS.get(dataset_id)
        if not config:
            raise ValueError(f"Dataset desconocido: {dataset_id}")

        df = self._load_df(dataset_id)

        # 1. Aplicar Filtros de Atributos (Año, Zona, etc.)
        for col, val in filters.items():
            if val is not None and col in df.columns:
                # Filtro tolerante a tipos (str vs int)
                df = df[df[col].astype(str) == str(val)]

        # 2. Aplicar Filtro Espacial (BBOX)
        if bbox:
            df = self._filter_spatial(df, bbox, config["lat_col"], config["lon_col"])

        # 3. Retornar formato
        if format == "geojson":
            return self._df_to_geojson_optimized(df, config["lat_col"], config["lon_col"])

        elif format == "columnar":
            return self._df_to_columnar(df, config["lat_col"], config["lon_col"], elevation_col)
        
        else: # JSON normal
            return df.to_dict(orient="records") 

    def get_classification_breaks(self, dataset_id: str, field: str, method: str, bins: int):
        """Calcula cortes para leyendas dinámicas"""
        df = self._load_df(dataset_id)
        
        if field not in df.columns:
            raise ValueError(f"Columna '{field}' no existe")
            
        series = df[field].dropna()
        
        if method == "unique":
            counts = series.value_counts().to_dict()
            return {"type": "categorical", "stats": counts}
            
        # Lógica numérica con Numpy
        values = pd.to_numeric(series, errors='coerce').dropna().values
        
        if len(values) == 0:
            return {"error": "No hay datos numéricos válidos"}

        if method == "quantile":
            breaks = np.quantile(values, np.linspace(0, 1, bins + 1)).tolist()
        else: # equal_interval
            breaks = np.linspace(values.min(), values.max(), bins + 1).tolist()

        return {
            "type": "numerical",
            "min": float(values.min()),
            "max": float(values.max()),
            "breaks": breaks,
            "method": method
        }
    
    def _df_to_columnar(self, df: pd.DataFrame, lat_col: str, lon_col: str, elevation_col: str = None) -> List[dict]:
        """
        Formato optimizado para Deck.gl ColumnLayer.
        Retorna: [{position: [lon, lat], elevation: 100, color: [r,g,b]}, ...]
        """
        # 1. Limpiar
        df = df.dropna(subset=[lat_col, lon_col])
        
        # 2. Preparar elevación (Altura de la barra)
        # Si nos piden una columna numérica (ej: matricula), la usamos. Si no, altura fija.
        if elevation_col and elevation_col in df.columns:
            # Convertimos a numérico y reemplazamos NaN con 0
            elevations = pd.to_numeric(df[elevation_col], errors='coerce').fillna(10).values
        else:
            elevations = np.full(len(df), 50) # Altura por defecto si no hay columna

        # 3. Preparar Color (Lógica simple basada en Zona para este ejemplo)
        # Urbana = Azul [0, 150, 255], Rural = Naranja [255, 140, 0], Otro = Gris
        colors = []
        if "zona" in df.columns:
            # Vectorizar esto sería mejor, pero para legibilidad lo hacemos simple:
            zona_vals = df["zona"].astype(str).str.upper().values
            for z in zona_vals:
                if "URBANA" in z:
                    colors.append([0, 150, 255])
                elif "RURAL" in z:
                    colors.append([255, 140, 0])
                else:
                    colors.append([200, 200, 200])
        else:
            colors = [[0, 150, 255]] * len(df)

        # 4. Extraer coordenadas
        coords = df[[lon_col, lat_col]].values

        # 5. Construir lista final (Zip es rápido)
        data = []
        for pos, elev, col in zip(coords, elevations, colors):
            data.append({
                "position": [pos[0], pos[1]],
                "elevation": float(elev), # Asegurar float nativo
                "fillColor": col
            })
            
        return data
# Singleton
query_engine = QueryEngine()