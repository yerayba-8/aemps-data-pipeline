from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Definimos las políticas de reintento en caso de fallo de red con la API
default_args = {
    'owner': 'yeray_bueno',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Si falla, Airflow lo reintentará 2 veces...
    'retry_delay': timedelta(minutes=5),   # ...esperando 5 minutos entre cada intento.
}

# 2. Inicializamos el DAG
with DAG(
    'aemps_data_pipeline_v1',
    default_args=default_args,
    description='Pipeline de datos de la AEMPS: Ingesta API Python + Transformación dbt',
    schedule_interval='@daily',            # Se ejecutará automáticamente todas las noches
    start_date=datetime(2026, 1, 1),
    catchup=False,                         # Evita que ejecute los días pasados del histórico
    tags=['aemps', 'dbt', 'postgres'],
) as dag:
    
    # Tarea 1: Descargar datos frescos de la API e inyectarlos en la capa Bronze (Postgres)
    task_ingesta_api = BashOperator(
        task_id='ingesta_api_python',
        bash_command='python /opt/airflow/extract.py',
    )

    # Tarea 2: Compilar y ejecutar las transformaciones en la capa Silver y Gold
    task_dbt_run = BashOperator(
        task_id='dbt_transformacion',
        bash_command='cd /opt/airflow/aemps_transform && dbt run --target docker',
    )

    # Tarea 3: Ejecutar los 14 tests de calidad de datos automatizados
    task_dbt_test = BashOperator(
        task_id='dbt_control_calidad',
        bash_command='cd /opt/airflow/aemps_transform && dbt test --target docker',
    )

    # 3. Definimos el orden estricto de ejecución (Lineal)
    task_ingesta_api >> task_dbt_run >> task_dbt_test