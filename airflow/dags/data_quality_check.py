from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.utils import check_data_quality

with DAG(
    dag_id='data_quality_check',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['quality'],
) as dag:
    check = PythonOperator(task_id='check_quality', python_callable=check_data_quality)