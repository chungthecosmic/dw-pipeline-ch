from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import pendulum

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id='foreign_stock_ingest',
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    schedule='0 9 * * 1-5',  # 월-금 오전 9시 (미국 장 마감 후)
    catchup=False,
    tags=['spark', 'stock', 'foreign'],
    description='해외 주식 시세 데이터 수집 (Yahoo Finance)',
) as dag:

    ingest_foreign_stock = SparkSubmitOperator(
        task_id='ingest_foreign_stock',
        application="/opt/spark-apps/jobs/foreign_stock_ingest.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
        driver_memory="1g",
        executor_memory="2g",
        executor_cores=1,
    )

    ingest_foreign_stock
