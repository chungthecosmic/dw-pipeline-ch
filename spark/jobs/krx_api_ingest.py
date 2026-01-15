import os
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta

service_key = os.environ.get("GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY")

# 날짜 파라미터 받기: python krx_api_ingest.py 20260113
# 파라미터가 없으면 기본값 (2일 전 날짜) 사용
if len(sys.argv) > 1:
    basDt = sys.argv[1]  # YYYYMMDD 형식
else:
    basDt = (datetime.today() - timedelta(days=2)).strftime('%Y%m%d')

print(f"수집 대상 날짜: {basDt}")
params = {
    "serviceKey" : service_key,
    # "pageNo": '1',
    "resultType": 'json',
    "basDt": basDt,
    # "numOfRows": '10'
}

response = requests.get(
    #금융위원회_주식시세정보API
    "https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo",
    params=params
)

data = response.json()['response']['body']['items']['item']
print(data)

spark = SparkSession.builder \
    .appName("KRX Stocks API Ingest Job") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.createDataFrame(data)

print("== RAW Data ==")

df.show(5, truncate=False)

# 예시: 128MB 정도로 맞추고 싶을 때
target_size_mb = 128
row_size_bytes = 1000  # 대략적인 한 행 크기(예상)
num_rows = df.count()
total_size_mb = (num_rows * row_size_bytes) / (1024 * 1024)
num_partitions = max(1, int(total_size_mb / target_size_mb))
output_path = '/opt/spark-apps/output/krx/' + basDt

df.repartition(num_partitions).write.mode("overwrite").parquet(output_path)

spark.stop()