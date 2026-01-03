# 예시: 커스텀 오퍼레이터 템플릿
from airflow.models.baseoperator import BaseOperator

class MyCustomOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        print("커스텀 오퍼레이터 실행")