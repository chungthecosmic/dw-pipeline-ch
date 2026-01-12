from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import pendulum

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id='krx_stock_ingest',
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    schedule='0 18 * * 1-5',  # 월-금 오후 6시 (장 마감 후)
    catchup=False,
    tags=['spark', 'stock', 'krx', 'korea'],
    description='국내(KRX) 주식 시세 데이터 수집 (금융위원회 API)',
) as dag:

    ingest_krx_stock = SparkSubmitOperator(
        task_id='ingest_krx_stock',
        application="/opt/spark-apps/jobs/krx_api_ingest.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
        driver_memory="1g",
        executor_memory="2g",
        executor_cores=1,
        env_vars={
            'GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY': '{{ var.value.GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY }}'
        }
    )

    ingest_krx_stock
