from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

kst = pendulum.timezone("Asia/Seoul")

# dbt 프로젝트 경로
DBT_PROJECT_DIR = '/opt/dbt'
DBT_PROFILES_DIR = '/opt/dbt'

with DAG(
    dag_id='dbt_transform',
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    schedule='0 20 * * *',  # 매일 20:00 (데이터 수집 후)
    catchup=False,
    tags=['dbt', 'transformation'],
    description='dbt를 사용한 데이터 변환 파이프라인',
) as dag:

    # dbt 연결 테스트
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir {DBT_PROFILES_DIR}',
    )

    # dbt deps: 의존성 설치 (필요시)
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}',
    )

    # staging 모델 실행
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select staging.* --profiles-dir {DBT_PROFILES_DIR}',
    )

    # mart 모델 실행
    dbt_run_mart = BashOperator(
        task_id='dbt_run_mart',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select mart.* --profiles-dir {DBT_PROFILES_DIR}',
    )

    # dbt 테스트 실행
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}',
    )

    # 의존성 설정
    dbt_debug >> dbt_deps >> dbt_run_staging >> dbt_run_mart >> dbt_test
