import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv()
url = (f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
       f"@localhost:5432/{os.getenv('POSTGRES_DB')}")
engine = create_engine(url)

# Cuántas filas tiene raw_medicamentos en total (para ver si es un snapshot único o acumulado)
total_raw = pd.read_sql("SELECT COUNT(*) AS n FROM raw_medicamentos", engine)
print("Total filas en raw_medicamentos:", total_raw["n"].iloc[0])

# Columnas de raw_medicamentos (por si hay fecha de carga)
cols = pd.read_sql(
    "SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_medicamentos'",
    engine
)
print("Columnas de raw_medicamentos:", cols["column_name"].tolist())

# Comparación real: nregistro en raw_medicamentos que ya no está en dim_medicamentos (catálogo actual)
query = """
select data->>'nregistro' as numero_registro, data->>'nombre' as nombre
from raw_medicamentos
where data->>'nregistro' not in (
    select numero_registro from dim_medicamentos
)
"""
df = pd.read_sql(query, engine)
print("Total desaparecidos (raw vs dim actual):", len(df))
print(df.head(5).to_string(index=False))