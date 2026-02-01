from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import pendulum

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id='exchange_rate_ingest',
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    schedule='0 9 * * *',  # 매일 오전 9시
    catchup=False,
    tags=['spark', 'exchange_rate', 'currency'],
    description='환율 데이터 수집 (ExchangeRate-API, KRW 기준)',
) as dag:

    ingest_exchange_rate = SparkSubmitOperator(
        task_id='ingest_exchange_rate',
        application="/opt/spark-apps/jobs/exchange_rate_ingest.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
        driver_memory="1g",
        executor_memory="2g",
        executor_cores=1,
    )

    ingest_exchange_rate
