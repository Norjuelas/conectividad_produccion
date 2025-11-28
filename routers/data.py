from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any
from services.query_engine import query_engine, DATASETS

router = APIRouter()

# ================================
# LISTADO DE DATASETS
# ================================
@router.get("/")
def list_datasets():
    return {"datasets":[{"id":k,"filters":v["filters"]} for k,v in DATASETS.items()]}

# ================================
# ENDPOINT PRINCIPAL (EL QUE FALTA)
# ================================
@router.get("/{dataset_id}")
def get_dataset_data(
    dataset_id: str,
    format: str = Query("json",enum=["json","geojson","columnar"]),
    elevation_col: Optional[str] = None,

    # filtros generales
    year_reporte: Optional[str]=None,
    zona: Optional[str]=None,
    DPTO_CNMBR: Optional[str]=None,
    MPIO_CNMBR: Optional[str]=None,

    # filtros sena_ised
    departamento: Optional[str]=None,
    d_conectado: Optional[str]=None,
    sector_atencion: Optional[str]=None,

    bbox: Optional[str]=None
):
    try:
        filters={k:v for k,v in {
            "year_reporte":year_reporte,"zona":zona,
            "DPTO_CNMBR":DPTO_CNMBR,"MPIO_CNMBR":MPIO_CNMBR,
            "departamento":departamento,"d_conectado":d_conectado,
            "sector_atencion":sector_atencion
        }.items() if v is not None}

        data=query_engine.get_data(dataset_id,format,filters,bbox,elevation_col)

        return data if format=="geojson" else {
            "dataset":dataset_id,"count":len(data),"data":data
        }

    except ValueError as e:
        raise HTTPException(status_code=404,detail=str(e))

# ================================
# RANGOS / BREAKS (CORREGIDO)
# ================================
@router.get("/{dataset_id}/breaks")
def get_dataset_breaks(dataset_id:str, field:str,
    method:str=Query("quantile",enum=["quantile","equal_interval","unique"]),
    bins:int=5):
    return query_engine.get_classification_breaks(dataset_id,field,method,bins)
@router.get("/sedes/connectividad")
def sedes_conectividad(
    conectado: str | None = Query(None, description="SI / NO"),
    tecnologia: str | None = Query(None, description="FIBRA ÓPTICA / RADIO ENLACE / etc"),
    min_mbps: float | None = Query(None, description="Filtrar por Mbps mínimos"),
    min_equipos: int | None = Query(None, description="Sedes con al menos X equipos"),
    min_ratio_terminales: float | None = Query(None, description="Promedio de acceso"),
    dpto: str | None = None,
    mpio: str | None = None,
    año: int | None = None,
    formato: str = "geojson"
):
    df = query_engine.get_data("sena_ised", "json", filters={}, bbox=None)

    import pandas as pd
    df = pd.DataFrame(df)   # recibir JSON → DataFrame

    if conectado:
        df = df[df["conectividad_def"] == conectado]

    if tecnologia:
        df = df[df["tecnologia_conec"].str.contains(tecnologia, case=False, na=False)]

    if min_mbps:
        df = df[pd.to_numeric(df["anchodebandaconsolidadombps"].str.extract(r'(\d+)', expand=False), errors="coerce") >= min_mbps]

    if min_equipos:
        df = df[pd.to_numeric(df["total_equipos"], errors="coerce") >= min_equipos]

    if min_ratio_terminales:
        df = df[pd.to_numeric(df["estudiantes_terminales"], errors="coerce") >= min_ratio_terminales]

    if dpto:
        df = df[df["dpto_ccdgo"].astype(str) == str(dpto)]

    if mpio:
        df = df[df["mpio_cnmbr"].str.contains(mpio, case=False, na=False)]

    if año:
        df = df[df["anno_inf"].astype(str) == str(año)]

    # devolver como GeoJSON
    return query_engine._df_to_geojson_optimized(df, "latitud", "longitud")