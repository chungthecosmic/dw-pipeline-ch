import os
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timedelta
import pandas as pd

# ExchangeRate-API 설정 (인증 불필요)
EXCHANGE_RATE_API_URL = 'https://open.er-api.com/v6/latest/KRW'

# 수집할 통화 목록 (KRW 기준)
TARGET_CURRENCIES = [
    'USD',  # 미국 달러
    'EUR',  # 유로
    'JPY',  # 일본 엔
    'CNY',  # 중국 위안
    'GBP',  # 영국 파운드
    'CHF',  # 스위스 프랑
    'AUD',  # 호주 달러
]

# 통화 정보 매핑
CURRENCY_INFO = {
    'USD': {'name': '미국 달러', 'country': '미국'},
    'EUR': {'name': '유로', 'country': '유럽연합'},
    'JPY': {'name': '일본 엔', 'country': '일본'},
    'CNY': {'name': '중국 위안', 'country': '중국'},
    'GBP': {'name': '영국 파운드', 'country': '영국'},
    'CHF': {'name': '스위스 프랑', 'country': '스위스'},
    'AUD': {'name': '호주 달러', 'country': '호주'},
}

# 날짜 파라미터 받기: python exchange_rate_ingest.py 20260113
if len(sys.argv) > 1:
    target_date = sys.argv[1]  # YYYYMMDD 형식
else:
    target_date = datetime.today().strftime('%Y%m%d')

print(f"수집 대상 날짜: {target_date}")
print(f"기준 통화: KRW (원화)")
print(f"대상 통화: {', '.join(TARGET_CURRENCIES)}")

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Exchange Rate Ingest Job") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


def fetch_exchange_rates():
    """ExchangeRate-API로 환율 데이터 수집 (KRW 기준)"""
    print(f"\nAPI 호출 중: {EXCHANGE_RATE_API_URL}")

    response = requests.get(EXCHANGE_RATE_API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    # 응답 검증
    if data.get('result') != 'success':
        raise ValueError(f"API Error: {data.get('error-type', 'Unknown error')}")

    rates = data.get('rates', {})
    base_code = data.get('base_code', 'KRW')
    time_last_update = data.get('time_last_update_utc', '')

    print(f"기준 통화: {base_code}")
    print(f"마지막 업데이트: {time_last_update}")

    return rates, time_last_update


def calculate_krw_rate(rate_from_krw):
    """KRW 기준 환율 계산 (1 외화 = X KRW)"""
    if rate_from_krw == 0:
        return 0
    return 1 / rate_from_krw


# 데이터 수집
try:
    rates, last_update = fetch_exchange_rates()
except Exception as e:
    print(f"환율 데이터 수집 실패: {str(e)}")
    spark.stop()
    exit(1)

# 대상 통화별 데이터 추출
all_data = []

print("\n== 수집된 환율 데이터 ==")
for currency in TARGET_CURRENCIES:
    if currency in rates:
        # API는 KRW 기준 (1 KRW = X 외화) 형태로 반환
        # 우리는 1 외화 = X KRW 형태로 변환
        rate_from_krw = rates[currency]
        rate_to_krw = calculate_krw_rate(rate_from_krw)

        currency_data = {
            'currency_code': currency,
            'currency_name': CURRENCY_INFO[currency]['name'],
            'country': CURRENCY_INFO[currency]['country'],
            'base_currency': 'KRW',
            'rate': rate_to_krw,  # 1 외화 = X KRW
            'inverse_rate': rate_from_krw,  # 1 KRW = X 외화
            'date': target_date,
            'api_update_time': last_update,
        }
        all_data.append(currency_data)

        print(f"  {currency} ({CURRENCY_INFO[currency]['name']}): 1 {currency} = {rate_to_krw:,.2f} KRW")
    else:
        print(f"  {currency}: 데이터 없음")

if not all_data:
    print("수집된 데이터가 없습니다.")
    spark.stop()
    exit(1)

print(f"\n총 {len(all_data)}개 통화 데이터 수집 완료")

# Pandas DataFrame을 Spark DataFrame으로 변환
pandas_df = pd.DataFrame(all_data)
df = spark.createDataFrame(pandas_df)

# 수집 시간 컬럼 추가
df = df.withColumn('collected_at', current_timestamp())

print("\n== RAW Data ==")
df.show(10, truncate=False)

# 파티션 계산
target_size_mb = 128
row_size_bytes = 300  # 대략적인 한 행 크기
num_rows = df.count()
total_size_mb = (num_rows * row_size_bytes) / (1024 * 1024)
num_partitions = max(1, int(total_size_mb / target_size_mb))

# 출력 경로 (S3)
output_path = f's3a://dw-pipeline-ch/raw/exchange_rate/{target_date}'

print(f"\n데이터 저장 중: {output_path}")
df.repartition(num_partitions).write.mode("overwrite").parquet(output_path)

print(f"저장 완료! (파티션 수: {num_partitions})")

spark.stop()
