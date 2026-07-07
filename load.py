import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_batch # Importamos execute_batch para inserciones masivas eficientes
import json

def crear_tabla_staging(conexion):
    """Función para crear la tabla  que alojará los JSONs en bruto"""
    # 1. Solicitamos un cursos a la conexión para poder enviar comandos SQL
    cursor = conexion.cursor()
    
    # 2. Definimos la sentencia SQL. Usamos JSONB para máxima eficiencia con JSONs
    sql_create_table = """
    CREATE TABLE IF NOT EXISTS raw_medicamentos (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    try:
        print("Verificando tabla 'raw_medicamentos'...")
        cursor.execute(sql_create_table)
        conexion.commit()       
    except Exception as e:
        conexion.rollback()
        print(f"Error al crear la tabla: {e}")
    finally:
        cursor.close()

def cargar_datos_en_staging(conexion, ruta_json):
    """Lee el JSON local e  inserta los datos en Postgres en bloques eficientes"""
    cursor = conexion.cursor()
    
    # 1. Sentencia SQL para insertar un registro JSONB
    # El '%s' es un placeholder que será reemplazado por el JSON en bruto
    sql_insert = "INSERT INTO raw_medicamentos (data) VALUES (%s);"
    
    try:
        print(f" Leyendo el archivo local '{ruta_json}'...")
        with open(ruta_json, "r", encoding="utf-8") as f:
            medicamentos = json.load(f)
        
        total_registros = len(medicamentos)
        print(f"Detectados {total_registros} registros para cargar.")
    
        # 2. Preparamos los datos en el formato que pide psycopg2.
        # execute_batch necesita una lista de tuplas, donde cada tupla contiene los valores a insertar.
        # Convertimos cada diccionario de medicamento en un string JSON puro.
        
        datos_preparados = [(json.dumps(med),) for med in medicamentos]
        
        print("Iniciando inserción masiva en bloques (Batch size: 1000)")
        
        # 3. Lanzamos la inserción masiva en bloques de 1000 en 1000.
        execute_batch(cursor, sql_insert, datos_preparados, page_size=1000)
        
        # 4. Confirmamos los cambvios de toda la carga
        conexion.commit()
        print(f" Carga completada con éxito. {total_registros} registros insertados en 'raw_medicamentos'.")
    
    except Exception as e:
        conexion.rollback()
        print(f"Error durante la carga de datos: {e}")
    finally:
        cursor.close()
        
def ejecutar_pipeline_carga():
    
    try:
        # Establecemos la conexión con el Data Warehouse
        conexion = psycopg2.connect(
            host= 'localhost',
            port= '5432',
            database= 'aemps_warehouse',
            user= 'yeray_admin',
            password= 'mi_password_seguro'
        )
        
        # Ejecutamos los dos pasos del pipeline de carga
        crear_tabla_staging(conexion)
        cargar_datos_en_staging(conexion, "catalogo_completo_medicamentos.json")
        
        conexion.close()
        print("Proceso finalizado. Conexión cerrada.")
    
    except OperationalError as e:
        print(f"Error operativo en la base de datos: {e}")
        
if __name__ == "__main__":
    ejecutar_pipeline_carga()
    