from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.utils import extract_sales, transform_sales, load_sales
import pendulum

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id='etl_sales',
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    schedule='@daily',
    catchup=False,
    tags=['sales'],
) as dag:
    extract = PythonOperator(task_id='extract', python_callable=extract_sales)
    transform = PythonOperator(task_id='transform', python_callable=transform_sales)
    load = PythonOperator(task_id='load', python_callable=load_sales)

    extract >> transform >> load