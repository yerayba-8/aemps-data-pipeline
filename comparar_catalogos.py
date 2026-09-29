import json
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

url = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@localhost:5432/{os.getenv('POSTGRES_DB')}"
)
engine = create_engine(url)

# --- Conjunto viejo: lo que hay hoy en la base de datos ---
df_viejo = pd.read_sql(
    "SELECT numero_registro, nombre_medicamento FROM dim_medicamentos", engine
)
df_viejo["numero_registro"] = df_viejo["numero_registro"].astype(str).str.strip()
viejo = set(df_viejo["numero_registro"])

# --- Conjunto nuevo: el listado que acabas de descargar ---
with open("catalogo_completo_medicamentos.json", encoding="utf-8") as f:
    catalogo = json.load(f)
nombres_nuevo = {str(m["nregistro"]).strip(): m.get("nombre") for m in catalogo}
nuevo = set(nombres_nuevo)

# --- Comprobar duplicados antes de comparar ---
print(f"Filas viejo: {len(df_viejo)} | distintos: {len(viejo)}")
print(f"Filas nuevo: {len(catalogo)} | distintos: {len(nuevo)}")

# --- Operaciones entre conjuntos ---
desaparecidos = viejo - nuevo
nuevos = nuevo - viejo
en_los_dos = viejo & nuevo

print(f"\nDesaparecidos: {len(desaparecidos)}")
print(f"Nuevos:        {len(nuevos)}")
print(f"En los dos:    {len(en_los_dos)}")

print("\nDesaparecidos (nombre):")
print(df_viejo[df_viejo["numero_registro"].isin(desaparecidos)].to_string(index=False))

print("\nNuevos (nombre):")
for n in sorted(nuevos):
    print(f"  {n}  {nombres_nuevo[n]}")
    
def con_letras(conjunto):
    return sum(any(c.isalpha() for c in n) for n in conjunto)

print("Con letras en el nº de registro:")
print("  viejo:        ", con_letras(viejo), "de", len(viejo))
print("  nuevo:        ", con_letras(nuevo), "de", len(nuevo))
print("  desaparecidos:", con_letras(desaparecidos), "de", len(desaparecidos))
print("  nuevos:       ", con_letras(nuevos), "de", len(nuevos))