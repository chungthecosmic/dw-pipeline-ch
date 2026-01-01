from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, World!")

with DAG(
    dag_id='hello_world',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example'],
) as dag:
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )