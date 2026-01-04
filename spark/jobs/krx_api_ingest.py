import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


service_key = os.environ.get("GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY")
print(service_key)

params = {
    "serviceKey" : service_key,
    "pageNo": '1',
    "resultType": 'json',
    "basDt": '20251201',
    "numOfRows": '10'
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

output_path = '/opt/spark-apps/output/posts_user_1'

df.write.mode("overwrite").parquet(output_path)

spark.stop()