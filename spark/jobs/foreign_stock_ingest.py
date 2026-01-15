import os
import sys
import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timedelta
import pandas as pd

# Alpha Vantage API 설정
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
ALPHA_VANTAGE_BASE_URL = 'https://www.alphavantage.co/query'

# Rate Limit 설정 (무료 티어: 분당 25회, 하루 500회)
RATE_LIMIT_DELAY = 2.5  # 초 (분당 24회로 제한)

# 수집할 주요 해외 주식 티커 리스트
TICKERS = [
    # 미국 주요 기술주
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 
    # 내가 가지고 있는 주식
    'DNA', 'SOFI',
    # 관심있는 주식
    # 'LEU', 'ONDS'
    # 미국 주요 지수 ETF
    'QQQ'
    # 기타 주요 종목
    # 'NFLX', 'CRM'
]

# 날짜 파라미터 받기: python foreign_stock_ingest.py 20260113
# 파라미터가 없으면 기본값 (오늘 날짜) 사용
if len(sys.argv) > 1:
    target_date = sys.argv[1]  # YYYYMMDD 형식
else:
    target_date = datetime.today().strftime('%Y%m%d')

print(f"수집 대상 날짜: {target_date}")

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Foreign Stock Price Ingest Job") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Alpha Vantage API를 사용하여 주식 데이터 수집
def fetch_stock_data(symbol):
    """Alpha Vantage API로 주식 데이터 수집"""
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': ALPHA_VANTAGE_API_KEY,
        'outputsize': 'compact'  # 최근 100개 데이터포인트 (무료 티어)
    }

    response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    # 에러 체크
    if 'Error Message' in data:
        raise ValueError(f"API Error: {data['Error Message']}")
    if 'Note' in data:
        raise ValueError(f"API Rate Limit: {data['Note']}")

    # Time Series 데이터 추출
    time_series = data.get('Time Series (Daily)', {})
    if not time_series:
        raise ValueError("No time series data found")

    # 가장 최근 날짜 데이터 추출
    latest_date = max(time_series.keys())
    latest_data = time_series[latest_date]

    return {
        'ticker': symbol,
        'date': latest_date,
        'open': float(latest_data['1. open']),
        'high': float(latest_data['2. high']),
        'low': float(latest_data['3. low']),
        'close': float(latest_data['4. close']),
        'volume': int(latest_data['5. volume']),
        'currency': 'USD',
        'exchange': 'NASDAQ/NYSE',
        'company_name': symbol
    }

# 데이터 수집
all_data = []

for idx, ticker in enumerate(TICKERS):
    try:
        print(f"티커 {ticker} 데이터 수집 중... ({idx+1}/{len(TICKERS)})")

        stock_data = fetch_stock_data(ticker)
        all_data.append(stock_data)

        print(f"  {ticker}: ${stock_data['close']:.2f} (거래량: {stock_data['volume']:,}, 날짜: {stock_data['date']})")

        # Rate Limit 대응: 다음 요청 전 대기
        if idx < len(TICKERS) - 1:  # 마지막 요청 후에는 대기 불필요
            time.sleep(RATE_LIMIT_DELAY)

    except Exception as e:
        print(f"  {ticker} 수집 실패: {str(e)}")
        continue

if not all_data:
    print("수집된 데이터가 없습니다.")
    spark.stop()
    exit(1)

print(f"\n총 {len(all_data)}개 종목 데이터 수집 완료")

# Pandas DataFrame을 Spark DataFrame으로 변환
pandas_df = pd.DataFrame(all_data)
df = spark.createDataFrame(pandas_df)

# 수집 시간 컬럼 추가
df = df.withColumn('collected_at', current_timestamp())

print("\n== RAW Data ==")
df.show(10, truncate=False)

# 파티션 계산
target_size_mb = 128
row_size_bytes = 500  # 대략적인 한 행 크기
num_rows = df.count()
total_size_mb = (num_rows * row_size_bytes) / (1024 * 1024)
num_partitions = max(1, int(total_size_mb / target_size_mb))

# 출력 경로
output_path = f'/opt/spark-apps/output/foreign_stock/{target_date}'

print(f"\n데이터 저장 중: {output_path}")
df.repartition(num_partitions).write.mode("overwrite").parquet(output_path)

print(f"저장 완료! (파티션 수: {num_partitions})")

spark.stop()
