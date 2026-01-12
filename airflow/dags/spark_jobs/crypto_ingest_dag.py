from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import pendulum

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id='crypto_ingest',
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    schedule='0 */6 * * *',  # 6시간마다 실행 (00:00, 06:00, 12:00, 18:00)
    catchup=False,
    tags=['spark', 'crypto', 'cryptocurrency'],
    description='가상자산 시세 데이터 수집 (CoinGecko API)',
) as dag:

    ingest_crypto = SparkSubmitOperator(
        task_id='ingest_crypto',
        application="/opt/spark-apps/jobs/crypto_ingest.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
        driver_memory="1g",
        executor_memory="2g",
        executor_cores=1,
    )

    ingest_crypto
