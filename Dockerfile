FROM apache/airflow:2.10.2-python3.9

RUN pip install --no-cache-dir \
    dbt-core==1.10.22 \
    dbt-postgres==1.9.1 \
    requests