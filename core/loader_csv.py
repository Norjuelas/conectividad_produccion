import pandas as pd
import os

def load_dataset(file_path: str):
    """
    Lee un archivo (Excel o CSV) y retorna un DataFrame de Pandas.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"El archivo no existe: {file_path}")
    
    # Detectar extensión
    if file_path.endswith('.csv'):
        df = pd.read_csv(
            file_path,
            dtype={'sede_codigo': str, 'MPIO_CDPMP': str},
            engine="python",
            on_bad_lines="skip"  # <<<< FIX IMPORTANTE
        )
    else:
        df = pd.read_excel(file_path)
    
    df.columns = df.columns.str.strip()  # Cleanup
    df = df.where(pd.notnull(df), None)  # JSON-safe
    
    return df
