import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta

service_key = os.environ.get("GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY")

basDt = (datetime.today() - timedelta(days=6)).strftime('%Y%m%d') #어제 날짜로 수행
print(basDt)
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