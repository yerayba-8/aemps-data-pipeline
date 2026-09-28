import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

load_dotenv()

# --- Conexión: las credenciales salen del .env, no del código ---
variables = ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"]
faltan = [v for v in variables if not os.getenv(v)]
if faltan:
    raise SystemExit(f"Faltan variables en el .env: {faltan}")

# URL.create escapa bien los caracteres especiales de la contraseña
engine = create_engine(
    URL.create(
        "postgresql",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host="localhost",
        port=5432,
        database=os.getenv("POSTGRES_DB"),
    ))

df_medicamentos = pd.read_sql('SELECT * FROM dim_medicamentos', engine)
df_principios = pd.read_sql('SELECT * FROM stg_principios_activos', engine)

print(df_medicamentos.shape)
print(df_principios.shape)

#Ejercicio 1 - Filtrado básico con pandas
""" print(df_medicamentos.columns.tolist())

print(df_medicamentos['condicion_prescripcion'].unique())

con_receta = df_medicamentos[df_medicamentos['condicion_prescripcion'] != 'Sin Receta']
sin_receta = df_medicamentos[df_medicamentos['condicion_prescripcion'] == 'Sin Receta'] 

#Total de filas
con_receta_count = con_receta.shape[0]
sin_receta_count = sin_receta.shape[0]

print(f"Total de medicamentos con receta: {con_receta_count}")
print(f"Total de medicamentos sin receta: {sin_receta_count}")

print(df_medicamentos['condicion_prescripcion'].value_counts())  """

# Ejercicio 2 - Agrupación con pandas
""" agrupacion = df_medicamentos.groupby('laboratorio_titular')['medicamento_id'].count().sort_values(ascending=False).head(10)

print(f'Primeros 10 medicamentos por laboratorio titular: {agrupacion}'  ) """

# Ejercicio 3 - Merge entre tablas
df_unido = df_medicamentos.merge(df_principios, on='medicamento_id')
print(df_unido.shape)

""" df_unido_filtrado = df_unido[df_unido['laboratorio_titular'] == 'Teva Pharma S.L.U.'].groupby('principio_activo_nombre')['principio_activo_nombre'].count().sort_values(ascending=False).head(10)
print(df_unido_filtrado) """

# Ejercicio 4 - Combinando SQL y pandas
""" top5_laboratorios = ['Teva Pharma S.L.U.', 'Laboratorio Stada S.L.','Laboratorios Normon S.A.', 'Laboratorios Cinfa S.A.','Kern Pharma S.L.']
df_top5 = df_unido[df_unido['laboratorio_titular'].isin(top5_laboratorios)]
conteo = df_top5.groupby(['laboratorio_titular','principio_activo_nombre']).size()
resultado = (
    conteo.reset_index(name='total').sort_values('total',ascending=False).groupby('laboratorio_titular').head(1)
)
print(resultado) """

# Ejercicio 5 - Crar una columna nueva a partir de una condición.en df_medicamentos tienes la columna numero_principios_activos. Quiero clasificar cada medicamento en dos tipos:"Monocomponente": tiene exactamente 1 principio activo."Combinado": tiene 2 o más
# --- Clasificación por número de principios activos ---
condiciones = [
    df_medicamentos["numero_principios_activos"] == 0,
    df_medicamentos["numero_principios_activos"] == 1,
    df_medicamentos["numero_principios_activos"] >= 2,
]
# 0 puede significar "no tiene" o "no se conoce": el JSON de EVARREST
# demostró que a veces es lo segundo
etiquetas = ["No informado", "Monocomponente", "Combinado"]

df_medicamentos["tipo"] = np.select(condiciones, etiquetas, default="Sin clasificar")

# --- Comprobaciones ---
assert (df_medicamentos["tipo"] != "Sin clasificar").all(), "Hay filas sin clasificar"

total_principios = df_medicamentos["numero_principios_activos"].sum()
assert total_principios == len(df_principios), (
    f"No cuadra: {total_principios} en dim_medicamentos vs {len(df_principios)} en stg_principios_activos"
)

# --- Resultados ---
totales = df_medicamentos["tipo"].value_counts()
print(totales)
print(f"\nTotal de medicamentos: {totales.sum()}")
print(f"Total de principios activos: {total_principios}")

print("\nMedicamentos con principios activos no informados:")
print(df_medicamentos[df_medicamentos["tipo"] == "No informado"])