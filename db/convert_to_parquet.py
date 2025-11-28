import pandas as pd

# Archivos fuente
CSV = "D:/Proyectos/conectividad_produccion/db/sena_ised_geo.csv"
PARQUET = "D:/Proyectos/conectividad_produccion/db/sena_ised.parquet"

print("📥 Leyendo CSV…")
df = pd.read_csv(CSV, dtype=str, low_memory=False)

print("💾 Guardando en Parquet...")
df.to_parquet(PARQUET, compression="snappy")

print("🟢 Listo — Archivo generado:", PARQUET)
print("Tamaño original:", round(len(open(CSV, 'rb').read())/1024,2), "KB")
print("Tamaño parquet :", round(len(open(PARQUET, 'rb').read())/1024,2), "KB")
