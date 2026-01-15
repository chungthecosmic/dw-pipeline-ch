from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pendulum

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id='master_pipeline',
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    schedule='0 21 * * *',  # 매일 21:00 (모든 데이터 수집 후)
    catchup=False,
    tags=['master', 'orchestration'],
    description='전체 데이터 파이프라인 마스터 오케스트레이션',
) as dag:

    start = EmptyOperator(task_id='start')

    # 데이터 수집 DAG들은 이미 개별 스케줄로 실행됨
    # 여기서는 dbt 변환만 트리거

    # dbt 변환 트리거
    trigger_dbt_transform = TriggerDagRunOperator(
        task_id='trigger_dbt_transform',
        trigger_dag_id='dbt_transform',
        wait_for_completion=True,
        poke_interval=30,
    )

    end = EmptyOperator(task_id='end')

    start >> trigger_dbt_transform >> end
