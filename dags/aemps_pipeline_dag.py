from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'yeray_bueno',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'aemps_data_pipeline_v1',
    default_args=default_args,
    description='Pipeline de datos de la AEMPS: Ingesta API Python + Transformación dbt (SCD2 + bajas)',
    schedule_interval='0 3 * * 1',   # todos los lunes a las 3:00 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['aemps', 'dbt', 'postgres'],
) as dag:

    # 1. Listado actual de medicamentos (rápido, sobrescribe catalogo_completo_medicamentos.json)
    task_extraccion_listado = BashOperator(
        task_id='extraccion_listado',
        bash_command='python /opt/airflow/extract.py 1',
    )

    # 2. Detalle completo de TODOS los medicamentos (lento, varias horas: es lo que
    #    permite detectar cambios en medicamentos ya existentes, no solo altas)
    task_extraccion_detalle = BashOperator(
        task_id='extraccion_detalle_completo',
        bash_command='python /opt/airflow/extract.py 2 --completo',
    )

    # 3. Carga del detalle a Postgres (upsert, nunca borra)
    task_carga_detalle = BashOperator(
        task_id='carga_detalle',
        bash_command='python /opt/airflow/load.py',
    )

    # 4. Carga del listado activo a Postgres (truncate + reload: es la señal de
    #    qué está vigente ahora, la que permite detectar bajas). Solo depende
    #    del listado (paso 1), así que puede correr en paralelo con el 2.
    task_carga_catalogo_activo = BashOperator(
        task_id='carga_catalogo_activo',
        bash_command='python /opt/airflow/load_catalogo.py',
    )

    # 5. Snapshot de dbt: historiza cambios y detecta bajas automáticamente
    task_dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/aemps_transform && dbt snapshot --target docker',
    )

    # 6. Transformaciones (dim_medicamentos, medicamentos_baja, etc.)
    task_dbt_run = BashOperator(
        task_id='dbt_transformacion',
        bash_command='cd /opt/airflow/aemps_transform && dbt run --target docker',
    )

    # 7. Tests de calidad de datos
    task_dbt_test = BashOperator(
        task_id='dbt_control_calidad',
        bash_command='cd /opt/airflow/aemps_transform && dbt test --target docker',
    )

    # Orden: el listado dispara dos ramas en paralelo (detalle completo, y carga
    # del catalogo activo). El snapshot espera a que AMBAS ramas terminen, porque
    # stg_medicamentos necesita las dos tablas actualizadas.
    task_extraccion_listado >> task_extraccion_detalle >> task_carga_detalle
    task_extraccion_listado >> task_carga_catalogo_activo

    [task_carga_detalle, task_carga_catalogo_activo] >> task_dbt_snapshot >> task_dbt_run >> task_dbt_test