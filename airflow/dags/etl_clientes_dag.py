from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args ={
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_clientes_incremental",
    default_args=default_args,
    description="ETL  incremental de clientes com Python + Postgres",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "incremental", "clientes"],
) as dag:
    
    executar_etl = BashOperator(
        task_id="executar_etl_clientes",
        bash_command="python /opt/airflow/scripts/etl_clientes_incremental.py",
    )