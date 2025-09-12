from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="tutorial_dag",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ci"],
) as dag:
    EmptyOperator(task_id="done")
