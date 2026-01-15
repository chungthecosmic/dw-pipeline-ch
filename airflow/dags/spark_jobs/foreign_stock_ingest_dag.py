from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime
import pendulum
import os

kst = pendulum.timezone("Asia/Seoul")

# Alpha Vantage API 키 가져오기 (환경변수 또는 Airflow Variable)
alpha_vantage_api_key = os.getenv('ALPHA_VANTAGE_API_KEY') or Variable.get('ALPHA_VANTAGE_API_KEY', default_var=None)

with DAG(
    dag_id='foreign_stock_ingest',
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    schedule='0 9 * * 1-5',  # 월-금 오전 9시 (미국 장 마감 후)
    catchup=False,
    tags=['spark', 'stock', 'foreign'],
    description='해외 주식 시세 데이터 수집 (Alpha Vantage API)',
) as dag:

    ingest_foreign_stock = SparkSubmitOperator(
        task_id='ingest_foreign_stock',
        application="/opt/spark-apps/jobs/foreign_stock_ingest.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        env_vars={
            'ALPHA_VANTAGE_API_KEY': alpha_vantage_api_key
        },
        verbose=True,
        driver_memory="1g",
        executor_memory="2g",
        executor_cores=1,
    )

    ingest_foreign_stock
