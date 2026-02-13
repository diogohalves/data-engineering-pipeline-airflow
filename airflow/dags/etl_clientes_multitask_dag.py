from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engeneering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_clientes_multitask",
    default_args=default_args,
    description="ETL clientes com Extract, Transform e Load",
    start_date=datetime(2026,1,1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl","multitask"],
) as dag:
    
    extract = BashOperator(
        task_id="extract_clientes",
        bash_command="python /opt/airflow/scripts/extract_clientes.py",
    )

    transform = BashOperator(
        task_id="transform_clientes",
        bash_command="python /opt/airflow/scripts/transform_clientes.py",
    )

    load = BashOperator(
        task_id="load_clientes",
        bash_command="python /opt/airflow/scripts/load_clientes.py",
    )

    extract >> transform >> load