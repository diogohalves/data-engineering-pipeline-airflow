from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delta": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_clientes_com_sensor",
    default_args=default_args,
    description="ETL de clientes aguardando chegada de arquivo",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["etl", "sensor"],
) as dag:
    
    esperar_arquivo = FileSensor(
        task_id="esperar_arquivo_clientes",
        filepath="/opt/airflow/scripts/clientes_incremental.csv",
        poke_interval=30, # verifica a cada 30s
        timeout=300, # desiste após 5 minutos
        mode="poke",
    )

    executar_etl = BashOperator(
        task_id="executar_etl",
        bash_command="python /opt/airflow/scripts/load_clientes.py",
    )

    esperar_arquivo >> executar_etl