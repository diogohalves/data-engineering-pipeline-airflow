from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from scripts.extract_clientes_xcom import extract

def transform_task(ti):
    qtd = ti.xcom_pull(task_ids="extract")
    print(f"TRANSFORM recebeu {qtd} registros")

def load_task(ti):
    qtd = ti.xcom_pull(task_ids="extract")
    print(f"LOAD recebeu {qtd} registros")

with DAG(
    dag_id="etl_clientes_xcom",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["xcom"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    extract_task >> transform >> load
