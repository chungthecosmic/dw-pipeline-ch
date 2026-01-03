from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.utils import extract_marketing, transform_marketing, load_marketing

with DAG(
    dag_id='etl_marketing',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['marketing'],
) as dag:
    extract = PythonOperator(task_id='extract', python_callable=extract_marketing)
    transform = PythonOperator(task_id='transform', python_callable=transform_marketing)
    load = PythonOperator(task_id='load', python_callable=load_marketing)

    extract >> transform >> load