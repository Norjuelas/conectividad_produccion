def dataframe_to_geojson(df: pd.DataFrame, lat_col: str, lon_col: str) -> dict:
    # 1. Limpiar datos inválidos
    df = df.dropna(subset=[lat_col, lon_col])
    
    if df.empty:
        return {"type": "FeatureCollection", "features": []}

    # 2. Vectorización (Mucho más rápido que iterrows)
    # Extraemos coordenadas aparte
    coords = df[[lon_col, lat_col]].values
    
    # Extraemos propiedades (todo menos lat/lon)
    # Orient 'records' devuelve una lista de diccionarios directamente
    props = df.drop(columns=[lat_col, lon_col]).to_dict(orient="records")
    
    # 3. Construcción rápida
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [c[0], c[1]] # lon, lat
            },
            "properties": p
        }
        for c, p in zip(coords, props)
    ]

    return {
        "type": "FeatureCollection",
        "features": features
    }