from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import pendulum

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id='spark_submit_example',
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    schedule='@daily',
    catchup=False,
    tags=['spark'],
) as dag:
    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_task',
        application="/opt/airflow/dags/spark_jobs/example_spark_job.py",  # 컨테이너 내 경로
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
    )

    spark_submit_task