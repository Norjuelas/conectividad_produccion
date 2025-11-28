import pandas as pd
import geopandas as gpd
from pathlib import Path

# ============================================================================
# SCRIPT PARA GENERAR CSV MOCK COMPLETO
# Consolida: sedes + geografía + indicadores ISED + conectividad
# ============================================================================

# Carga de datos

ised = pd.read_excel(r"database/datos/Base_ISED_2022_2023.xlsx", sheet_name="Sheet1")
rectores = pd.read_excel(r"database/datos/Códigos_CC422701  MD-MINTIC Rectores 2025.xlsx")
conectividad = pd.read_csv(r"database/datos/Conectividad_2022_2025.txt", sep=",")
municipios = gpd.read_file(r"database/datos/MGN_MPIO_POLITICO/MGN_MPIO_POLITICO.shp")
departamentos = gpd.read_file(r"database/datos/MGN_DPTO_POLITICO/MGN_ADM_DPTO_POLITICO.shp")

def estandarizar_codigos(
    ised: pd.DataFrame,
    municipios: gpd.GeoDataFrame,
    departamentos: gpd.GeoDataFrame,
    establecimientos: pd.DataFrame
):
    """
    Estandariza los códigos DANE a formato texto con ceros a la izquierda:
      - DPTO_CCDGO: 2 dígitos
      - MPIO_CDPMP / cod_dane_municipio: 5 dígitos
    """

    # ised: cod_dane_municipio a 5 dígitos
    if "cod_dane_municipio" in ised.columns:
        ised["cod_dane_municipio"] = (
            ised["cod_dane_municipio"].astype(str).str.zfill(5)
        )

    # municipios: DPTO_CCDGO (2) y MPIO_CDPMP (5)
    if "DPTO_CCDGO" in municipios.columns:
        municipios["DPTO_CCDGO"] = (
            municipios["DPTO_CCDGO"].astype(str).str.zfill(2)
        )
    if "MPIO_CDPMP" in municipios.columns:
        municipios["MPIO_CDPMP"] = (
            municipios["MPIO_CDPMP"].astype(str).str.zfill(5)
        )

    # departamentos: DPTO_CCDGO (2)
    if "DPTO_CCDGO" in departamentos.columns:
        departamentos["DPTO_CCDGO"] = (
            departamentos["DPTO_CCDGO"].astype(str).str.zfill(2)
        )

    # establecimientos: MPIO_CDPMP (5)
    if "MPIO_CDPMP" in establecimientos.columns:
        establecimientos["MPIO_CDPMP"] = (
            establecimientos["MPIO_CDPMP"].astype(str).str.zfill(5)
        )

    return ised, municipios, departamentos, establecimientos


def homogenizar_est_sed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada sede, homogeneizar est_id usando el del último año disponible.
    Requiere columnas: 'sede_codigo', 'year_reporte', 'est_id'.
    """
    # 1. Para cada sede, tomar el registro del último año y su est_id
    ultimos_est = (
        df.sort_values(["sede_codigo", "year_reporte"])
        .groupby("sede_codigo")
        .tail(1)[["sede_codigo", "est_id"]]
        .rename(columns={"est_id": "est_id_final"})
    )

    # 2. Unir ese est_id_final al df original
    df2 = df.merge(ultimos_est, on="sede_codigo", how="left")

    # 3. Sobrescribir est_id con el est_id_final (el del último año)
    df2["est_id"] = df2["est_id_final"]
    sedes = df2.drop(columns="est_id_final")

    return sedes


def imputar_coord_nan(sedes: pd.DataFrame, municipios: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Imputa coordenadas faltantes (latitud / longitud) usando el centroide del municipio.
    Requiere en sedes: 'cod_dane_municipio', 'latitud', 'longitud'
    y en municipios: 'MPIO_CDPMP', 'geometry'.
    """
    # Copias para no modificar los originales
    df = sedes.copy()
    mun = municipios.copy()

    # Aseguramos que los códigos de municipio sean texto
    df["cod_dane_municipio"] = df["cod_dane_municipio"].astype(str).str.zfill(5)
    mun["MPIO_CDPMP"] = mun["MPIO_CDPMP"].astype(str).str.zfill(5)

    # Aseguramos CRS en WGS84 antes de sacar centroides
    if mun.crs is not None and mun.crs.to_epsg() != 4326:
        mun = mun.to_crs(epsg=4326)

    # Calcular centroides de municipios
    mun["centroid"] = mun.geometry.centroid
    mun["centroid_lat"] = mun["centroid"].y
    mun["centroid_lon"] = mun["centroid"].x

    # Nos quedamos con código y centroides
    mun_cent = mun[["MPIO_CDPMP", "centroid_lat", "centroid_lon"]].drop_duplicates("MPIO_CDPMP")

    # Unir centroides al df de sedes
    df = df.merge(
        mun_cent,
        left_on="cod_dane_municipio",
        right_on="MPIO_CDPMP",
        how="left"
    )

    # Filtrar las filas que NO tienen coordenadas completas
    mask_sin_coord = df["latitud"].isna() | df["longitud"].isna()

    # Imputar con centroides de municipio
    df.loc[mask_sin_coord, "latitud"] = df.loc[mask_sin_coord, "centroid_lat"]
    df.loc[mask_sin_coord, "longitud"] = df.loc[mask_sin_coord, "centroid_lon"]

    # Limpiar columnas auxiliares
    df = df.drop(columns=["MPIO_CDPMP", "centroid_lat", "centroid_lon", "centroid"], errors="ignore")

    return df


def validar_coordenadas_en_municipio(
    sedes: pd.DataFrame,
    municipios: gpd.GeoDataFrame
) -> pd.DataFrame:
    """
    Verifica que las coordenadas de cada sede caigan dentro del municipio declarado.
    Si no cae dentro, se ponen latitud/longitud en NaN para luego imputar con centroides.

    Además:
      - imprime cuántas sedes no tienen coordenada (NaN) originalmente
      - imprime cuántas sedes tienen coordenada pero caen fuera de su municipio

    Requiere:
      sedes: 'cod_dane_municipio', 'latitud', 'longitud'
      municipios: 'MPIO_CDPMP', 'geometry'
    """
    df = sedes.copy()
    mun = municipios.copy()

    # 1) Cuántas sedes no tienen coordenada originalmente
    mask_sin_coord_inicial = df["latitud"].isna() | df["longitud"].isna()
    n_sin_coord_inicial = int(mask_sin_coord_inicial.sum())

    # 2) Solo filas con coordenadas válidas para hacer la validación espacial
    mask_coord_ok = ~mask_sin_coord_inicial
    if not mask_coord_ok.any():
        print(f"Sedes sin coordenada original: {n_sin_coord_inicial}")
        print("No hay sedes con coordenadas para validar contra el municipio.")
        return df

    # GeoDataFrame de sedes con coords
    puntos = gpd.GeoDataFrame(
        df.loc[mask_coord_ok].copy(),
        geometry=gpd.points_from_xy(
            df.loc[mask_coord_ok, "longitud"],
            df.loc[mask_coord_ok, "latitud"]
        ),
        crs="EPSG:4326"
    )

    # Asegurar CRS de municipios
    if mun.crs is None:
        mun = mun.set_crs(epsg=4326)  # ajusta si tu shapefile está en otro CRS
    elif mun.crs.to_epsg() != 4326:
        mun = mun.to_crs(epsg=4326)

    mun = mun[["MPIO_CDPMP", "geometry"]].copy()
    mun["MPIO_CDPMP"] = mun["MPIO_CDPMP"].astype(str).str.zfill(5)

    # Join espacial: qué municipio contiene a cada punto
    puntos_join = gpd.sjoin(
        puntos,
        mun,
        how="left",
        predicate="within"
    )

    # Comparar código reportado vs código geométrico

    cod_rep = puntos_join["cod_dane_municipio"].astype(str).str.zfill(5)
    cod_geo = puntos_join["MPIO_CDPMP"].astype(str).str.zfill(5)

    # Mismatches (incluye cuando no se encontró polígono: NaN)
    mask_mismatch = (cod_rep != cod_geo) | cod_geo.isna()
    n_fuera_mpio = int(mask_mismatch.sum())

    # Índices de sedes con coordenadas incoherentes
    idx_mal = puntos_join.index[mask_mismatch]

    # Poner esas coordenadas en NaN en el df original
    df.loc[idx_mal, "latitud"] = pd.NA
    df.loc[idx_mal, "longitud"] = pd.NA

    # 3) Imprimir resumen
    print(f"Sedes sin coordenada original: {n_sin_coord_inicial}")
    print(f"Sedes con coordenada fuera de su municipio: {n_fuera_mpio}")

    return df

def validar_coordenadas_en_municipio(
    sedes: pd.DataFrame,
    municipios: gpd.GeoDataFrame
) -> pd.DataFrame:
    """
    Verifica que las coordenadas de cada sede caigan dentro del municipio declarado.
    Si no cae dentro, se ponen latitud/longitud en NaN para luego imputar con centroides.

    Además:
      - imprime cuántas sedes no tienen coordenada (NaN) originalmente
      - imprime cuántas sedes tienen coordenada pero caen fuera de su municipio

    Requiere:
      sedes: 'cod_dane_municipio', 'latitud', 'longitud'
      municipios: 'MPIO_CDPMP', 'geometry'
    """
    df = sedes.copy()
    mun = municipios.copy()

    # 1) Cuántas sedes no tienen coordenada originalmente
    mask_sin_coord_inicial = df["latitud"].isna() | df["longitud"].isna()
    n_sin_coord_inicial = int(mask_sin_coord_inicial.sum())

    # 2) Solo filas con coordenadas válidas para hacer la validación espacial
    mask_coord_ok = ~mask_sin_coord_inicial
    if not mask_coord_ok.any():
        print(f"Sedes sin coordenada original: {n_sin_coord_inicial}")
        print("No hay sedes con coordenadas para validar contra el municipio.")
        return df

    # GeoDataFrame de sedes con coords
    puntos = gpd.GeoDataFrame(
        df.loc[mask_coord_ok].copy(),
        geometry=gpd.points_from_xy(
            df.loc[mask_coord_ok, "longitud"],
            df.loc[mask_coord_ok, "latitud"]
        ),
        crs="EPSG:4326"
    )

    # Asegurar CRS de municipios
    if mun.crs is None:
        mun = mun.set_crs(epsg=4326)  # ajusta si tu shapefile está en otro CRS
    elif mun.crs.to_epsg() != 4326:
        mun = mun.to_crs(epsg=4326)

    mun = mun[["MPIO_CDPMP", "geometry"]].copy()
    mun["MPIO_CDPMP"] = mun["MPIO_CDPMP"].astype(str)

    # Join espacial: qué municipio contiene a cada punto
    puntos_join = gpd.sjoin(
        puntos,
        mun,
        how="left",
        predicate="within"
    )

    # Comparar código reportado vs código geométrico
    cod_rep = puntos_join["cod_dane_municipio"].astype(str)
    cod_geo = puntos_join["MPIO_CDPMP"].astype(str)

    # Mismatches (incluye cuando no se encontró polígono: NaN)
    mask_mismatch = (cod_rep != cod_geo) | cod_geo.isna()
    n_fuera_mpio = int(mask_mismatch.sum())

    # Índices de sedes con coordenadas incoherentes
    idx_mal = puntos_join.index[mask_mismatch]

    # Poner esas coordenadas en NaN en el df original
    df.loc[idx_mal, "latitud"] = pd.NA
    df.loc[idx_mal, "longitud"] = pd.NA

    # 3) Imprimir resumen
    print(f"Sedes sin coordenada original: {n_sin_coord_inicial}")
    print(f"Sedes con coordenada fuera de su municipio: {n_fuera_mpio}")

    return df


def normalizar_datos(
    ised: pd.DataFrame,
    rectores: pd.DataFrame,
    conectividad: pd.DataFrame,
    municipios: gpd.GeoDataFrame,
    departamentos: gpd.GeoDataFrame
):

    # Normalizamos nombres de columnas de departamentos (excepto geometría)
    departamentos = departamentos.copy()
    geom_col = departamentos.geometry.name
    departamentos.columns = [
        c.upper() if c != geom_col else c
        for c in departamentos.columns
    ]

    # Mantener columnas geométricas y llaves asociadas
    departamentos = departamentos[["DPTO_CCDGO", "DPTO_CNMBR", "geometry"]].copy()

    # Ajustamos municipios a las columnas clave
    municipios = municipios[["DPTO_CCDGO", "MPIO_CDPMP", "DPTO_CNMBR", "MPIO_CNMBR", "geometry"]].copy()

    # Extraer establecimientos únicos
    establecimientos = (
        ised[["cod_dane_municipio", "est_id", "nombre_establecimiento"]]
        .drop_duplicates(subset="est_id")
        .rename(columns={"cod_dane_municipio": "MPIO_CDPMP"})
    )

    # *** NUEVO: estandarizar códigos DANE ***
    ised, municipios, departamentos, establecimientos = estandarizar_codigos(
        ised, municipios, departamentos, establecimientos
    )

    # Homogeneizar est_id a nivel de sedes e imputamos coordenadas faltantes
    sedes = homogenizar_est_sed(ised)
    sedes = validar_coordenadas_en_municipio(sedes, municipios)
    sedes = imputar_coord_nan(sedes, municipios)

    # dropeamos sedes duplicadas
    sedes = (
        sedes[["est_id", "sede_codigo", "nombre_sede", 'zona', 'direccion', 'latitud','longitud']]
        .drop_duplicates(subset="sede_codigo")
    )

    # convertimos en gdf
    sedes = gpd.GeoDataFrame(
        sedes,
        geometry=gpd.points_from_xy(sedes["longitud"], sedes["latitud"], crs="EPSG:4326")
    )

    # normalizamos rectores
    rectores = rectores.drop_duplicates(subset="PRECAR_E").rename(columns={"PRECAR_E": "sede_codigo"})
    # Dropear rectores sin padre en sedes
    rectores = rectores[rectores["sede_codigo"].astype(str).isin(sedes["sede_codigo"].astype(str))]

    # trabajamos indicadores ised
    indicadores_ised = ised.drop(columns=['nombre_establecimiento', 'est_id','nombre_sede', 'direccion', 'zona', 'est_id', 'longitud', 'latitud'])
    # trabajamos indicadores conectividad
    indicadores_conectividad = conectividad.drop(columns=['codigo_sed', 'nombre_sed', 'codmpio_sede','sede_municipio', 'codigo_dane', 'sede_codigo_principal',
                                                          'nombre_institucion', 'codigo_dane_sede','nombre_sede','sede_zona', 'matricula_sede'])


    # dropeamos sedes de conectividad que no se ecnuentren en sdes
    indicadores_conectividad = indicadores_conectividad[indicadores_conectividad["sede_codigo"].astype(str).isin(sedes["sede_codigo"].astype(str))]


    return municipios, departamentos, establecimientos, sedes, rectores, indicadores_ised, indicadores_conectividad


def generar_csv_mock_completo(
    sedes: gpd.GeoDataFrame,
    establecimientos: pd.DataFrame,
    municipios: gpd.GeoDataFrame,
    departamentos: gpd.GeoDataFrame,
    indicadores_ised: pd.DataFrame,
    indicadores_conectividad: pd.DataFrame,
    rectores: pd.DataFrame = None,
    output_path: str = "datos/mock_sedes_completo.csv"
):
    """
    Genera un CSV completo con toda la información necesaria para el mock.
    
    Estructura final:
    - Identificación de sede (sede_codigo, nombre_sede, est_id, nombre_establecimiento)
    - Geografía (latitud, longitud, zona, direccion, municipio, departamento)
    - Indicadores ISED (por año)
    - Indicadores Conectividad (por año)
    - Info rectores (opcional)
    
    Parameters:
    -----------
    sedes : gpd.GeoDataFrame
        GeoDataFrame con las sedes y sus coordenadas
    establecimientos : pd.DataFrame
        Información de establecimientos
    municipios : gpd.GeoDataFrame
        Información geográfica de municipios
    departamentos : gpd.GeoDataFrame
        Información geográfica de departamentos
    indicadores_ised : pd.DataFrame
        Indicadores ISED por sede y año
    indicadores_conectividad : pd.DataFrame
        Indicadores de conectividad por sede y año
    rectores : pd.DataFrame, optional
        Información de rectores por sede
    output_path : str
        Ruta donde guardar el CSV
    """
    
    print("🔄 Iniciando generación de CSV mock completo...")
    
    # 1. BASE: Partir de sedes (ya tiene geometría)
    df_base = sedes.copy()
    
    # Extraer coordenadas de la geometría
    df_base['longitud'] = df_base.geometry.x
    df_base['latitud'] = df_base.geometry.y
    
    # Eliminar columna geometry para CSV
    df_base = pd.DataFrame(df_base.drop(columns=['geometry']))
    
    print(f"✓ Base de sedes: {len(df_base)} registros")
    
    # 2. JOIN con establecimientos (nombre_establecimiento)
    df_base = df_base.merge(
        establecimientos[['est_id', 'nombre_establecimiento', 'MPIO_CDPMP']],
        on='est_id',
        how='left'
    )
    print(f"✓ Agregado info establecimientos")
    
    # 3. JOIN con municipios (nombre municipio, código departamento)
    municipios_info = municipios[['MPIO_CDPMP', 'MPIO_CNMBR', 'DPTO_CCDGO']].copy()
    municipios_info = municipios_info.drop_duplicates(subset='MPIO_CDPMP')
    
    df_base = df_base.merge(
        municipios_info,
        on='MPIO_CDPMP',
        how='left'
    )
    print(f"✓ Agregado info municipios")
    
    # 4. JOIN con departamentos (nombre departamento)
    departamentos_info = departamentos[['DPTO_CCDGO', 'DPTO_CNMBR']].copy()
    departamentos_info = departamentos_info.drop_duplicates(subset='DPTO_CCDGO')
    
    df_base = df_base.merge(
        departamentos_info,
        on='DPTO_CCDGO',
        how='left',
        suffixes=('', '_dpto')
    )
    print(f"✓ Agregado info departamentos")
    
    # 5. JOIN con indicadores ISED
    # Necesitamos pivotar o mantener múltiples años
    # OPCIÓN A: Mantener formato largo (recomendado para filtros dinámicos)
    if 'year_reporte' in indicadores_ised.columns:
        df_ised = indicadores_ised.copy()
        
        # Renombrar columnas para claridad
        columnas_ised = [col for col in df_ised.columns if col not in ['sede_codigo', 'year_reporte']]
        rename_dict = {col: f'ised_{col}' for col in columnas_ised}
        df_ised = df_ised.rename(columns=rename_dict)
        
        # Join
        df_con_ised = df_base.merge(
            df_ised,
            on='sede_codigo',
            how='left'
        )
        print(f"✓ Agregado indicadores ISED: {len(df_con_ised)} registros")
    else:
        df_con_ised = df_base.copy()
        df_con_ised['year_reporte'] = None
        print("⚠ No se encontró columna 'year_reporte' en indicadores ISED")
    
    # 6. JOIN con indicadores conectividad
    if 'anio' in indicadores_conectividad.columns:
        df_conect = indicadores_conectividad.copy()
        
        # Renombrar para evitar conflictos
        columnas_conect = [col for col in df_conect.columns if col not in ['sede_codigo', 'anio']]
        rename_dict = {col: f'conect_{col}' for col in columnas_conect}
        df_conect = df_conect.rename(columns=rename_dict)
        
        # Ajustar nombre de columna año para unificar
        df_conect = df_conect.rename(columns={'anio': 'year_reporte'})
        
        # Join usando sede_codigo y year_reporte
        df_completo = df_con_ised.merge(
            df_conect,
            on=['sede_codigo', 'year_reporte'],
            how='left'
        )
        print(f"✓ Agregado indicadores conectividad: {len(df_completo)} registros")
    else:
        df_completo = df_con_ised.copy()
        print("⚠ No se encontró columna 'anio' en indicadores conectividad")
    
    # 7. JOIN con rectores (opcional)
    if rectores is not None and len(rectores) > 0:
        # Seleccionar columnas relevantes de rectores
        columnas_rector = [col for col in rectores.columns if col != 'sede_codigo']
        df_rectores = rectores[['sede_codigo'] + columnas_rector].copy()
        
        # Prefijo para claridad
        rename_dict = {col: f'rector_{col}' for col in columnas_rector}
        df_rectores = df_rectores.rename(columns=rename_dict)
        
        df_completo = df_completo.merge(
            df_rectores,
            on='sede_codigo',
            how='left'
        )
        print(f"✓ Agregado info rectores")
    
    # 8. LIMPIEZA Y ORDENAMIENTO FINAL
    # Reordenar columnas para mejor legibilidad
    columnas_principales = [
        'sede_codigo',
        'nombre_sede',
        'est_id',
        'nombre_establecimiento',
        'year_reporte',
        'zona',
        'direccion',
        'latitud',
        'longitud',
        'MPIO_CDPMP',
        'MPIO_CNMBR',
        'DPTO_CCDGO',
        'DPTO_CNMBR'
    ]
    
    # Filtrar solo columnas que existen
    columnas_principales = [col for col in columnas_principales if col in df_completo.columns]
    
    # Resto de columnas (indicadores)
    otras_columnas = [col for col in df_completo.columns if col not in columnas_principales]
    
    df_completo = df_completo[columnas_principales + otras_columnas]
    
    # 9. EXPORTAR
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df_completo.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ CSV generado exitosamente!")
    print(f"📍 Ubicación: {output_path}")
    print(f"📊 Total registros: {len(df_completo)}")
    print(f"📋 Total columnas: {len(df_completo.columns)}")
    print(f"\n🔍 Distribución por año:")
    if 'year_reporte' in df_completo.columns:
        print(df_completo['year_reporte'].value_counts().sort_index())
    
    # Estadísticas adicionales
    print(f"\n📈 Estadísticas:")
    print(f"  - Sedes únicas: {df_completo['sede_codigo'].nunique()}")
    print(f"  - Establecimientos únicos: {df_completo['est_id'].nunique()}")
    print(f"  - Municipios únicos: {df_completo['MPIO_CDPMP'].nunique()}")
    print(f"  - Departamentos únicos: {df_completo['DPTO_CCDGO'].nunique()}")
    
    # Muestra de columnas
    print(f"\n📝 Columnas generadas:")
    for i, col in enumerate(df_completo.columns, 1):
        print(f"  {i:2d}. {col}")
    
    return df_completo


# ============================================================================
# FUNCIÓN PARA GENERAR VERSIONES SIMPLIFICADAS (TESTING)
# ============================================================================

def generar_csv_mock_simple(
    sedes: gpd.GeoDataFrame,
    establecimientos: pd.DataFrame,
    municipios: gpd.GeoDataFrame,
    output_path: str = "datos/mock_sedes_simple.csv"
):
    """
    Genera una versión simplificada solo con info básica (útil para testing inicial).
    """
    df = sedes.copy()
    
    # Extraer coordenadas
    df['longitud'] = df.geometry.x
    df['latitud'] = df.geometry.y
    df = pd.DataFrame(df.drop(columns=['geometry']))
    
    # Join mínimo
    df = df.merge(
        establecimientos[['est_id', 'nombre_establecimiento', 'MPIO_CDPMP']],
        on='est_id',
        how='left'
    )
    
    municipios_info = municipios[['MPIO_CDPMP', 'MPIO_CNMBR', 'DPTO_CCDGO']].drop_duplicates(subset='MPIO_CDPMP')
    df = df.merge(municipios_info, on='MPIO_CDPMP', how='left')
    
    # Exportar
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"✅ CSV simple generado: {output_path}")
    print(f"   Registros: {len(df)} | Columnas: {len(df.columns)}")
    
    return df

# ------------------------------------------------------------------
# Wrapper simple para construir df_completo desde los archivos datos/
# ------------------------------------------------------------------

def build_df_completo() -> pd.DataFrame:
    """
    Arma el DataFrame maestro df_completo a partir de los archivos
    en la carpeta datos/ utilizando tu lógica de normalización.
    """

    print("📂 Cargando datos crudos desde carpeta 'datos/'...")

    THIS_FILE = Path(__file__).resolve()
    PROJECT_ROOT = THIS_FILE.parents[2]     # sube a 'cnc_project_full'
    DATA_DIR = PROJECT_ROOT / "database" / "datos"

    base_path = DATA_DIR

    ised = pd.read_excel(base_path / "Base_ISED_2022_2023.xlsx", sheet_name="Sheet1")
    rectores = pd.read_excel(base_path / "Códigos_CC422701  MD-MINTIC Rectores 2025.xlsx")
    conectividad = pd.read_csv(base_path / "Conectividad_2022_2025.txt", sep=",")

    municipios = gpd.read_file(base_path / "MGN_MPIO_POLITICO" / "MGN_MPIO_POLITICO.shp")
    departamentos = gpd.read_file(base_path / "MGN_DPTO_POLITICO" / "MGN_ADM_DPTO_POLITICO.shp")

    print("🔧 Normalizando datos (normalizar_datos)...")
    municipios, departamentos, establecimientos, sedes, rectores_norm, indicadores_ised, indicadores_conectividad = normalizar_datos(
        ised,
        rectores,
        conectividad,
        municipios,
        departamentos
    )

    print("🧱 Construyendo df_completo (generar_csv_mock_completo)...")
    df_completo = generar_csv_mock_completo(
        sedes=sedes,
        establecimientos=establecimientos,
        municipios=municipios,
        departamentos=departamentos,
        indicadores_ised=indicadores_ised,
        indicadores_conectividad=indicadores_conectividad,
        rectores=rectores_norm,
        output_path="datos/mock_sedes_completo.csv"  # sigue guardando el CSV si quieres
    )

    print("✅ df_completo listo en memoria (y CSV generado).")
    return df_completo
