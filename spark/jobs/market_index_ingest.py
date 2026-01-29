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

# 수집할 주식 지수 ETF 목록
# Alpha Vantage는 지수를 직접 제공하지 않으므로 대표 ETF 사용
INDEX_ETFS = [
    {
        'ticker': 'SPY',
        'index_name': 'S&P 500',
        'index_symbol': '^GSPC',
        'description': 'SPDR S&P 500 ETF Trust - S&P 500 지수 추종',
        'exchange': 'NYSE ARCA',
    },
    {
        'ticker': 'DIA',
        'index_name': 'Dow Jones Industrial Average',
        'index_symbol': '^DJI',
        'description': 'SPDR Dow Jones Industrial Average ETF - 다우존스 지수 추종',
        'exchange': 'NYSE ARCA',
    },
    {
        'ticker': 'QQQ',
        'index_name': 'NASDAQ-100',
        'index_symbol': '^NDX',
        'description': 'Invesco QQQ Trust - 나스닥 100 지수 추종',
        'exchange': 'NASDAQ',
    },
]

# 날짜 파라미터 받기: python market_index_ingest.py 20260113
if len(sys.argv) > 1:
    target_date = sys.argv[1]  # YYYYMMDD 형식
else:
    target_date = datetime.today().strftime('%Y%m%d')

print(f"수집 대상 날짜: {target_date}")
print(f"수집 대상 지수: {', '.join([idx['index_name'] for idx in INDEX_ETFS])}")

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Market Index Ingest Job") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


def fetch_index_data(etf_info):
    """Alpha Vantage API로 지수 ETF 데이터 수집"""
    ticker = etf_info['ticker']

    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': ticker,
        'apikey': ALPHA_VANTAGE_API_KEY,
        'outputsize': 'compact'  # 최근 100개 데이터포인트
    }

    response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
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

    # 전일 데이터 (변동률 계산용)
    sorted_dates = sorted(time_series.keys(), reverse=True)
    prev_close = None
    change = None
    change_percent = None

    if len(sorted_dates) > 1:
        prev_date = sorted_dates[1]
        prev_close = float(time_series[prev_date]['4. close'])
        current_close = float(latest_data['4. close'])
        change = current_close - prev_close
        change_percent = (change / prev_close) * 100

    return {
        'ticker': ticker,
        'index_name': etf_info['index_name'],
        'index_symbol': etf_info['index_symbol'],
        'description': etf_info['description'],
        'exchange': etf_info['exchange'],
        'date': latest_date,
        'open': float(latest_data['1. open']),
        'high': float(latest_data['2. high']),
        'low': float(latest_data['3. low']),
        'close': float(latest_data['4. close']),
        'volume': int(latest_data['5. volume']),
        'prev_close': prev_close,
        'change': change,
        'change_percent': change_percent,
        'currency': 'USD',
    }


# 데이터 수집
all_data = []

print("\n== 지수 ETF 데이터 수집 ==")
for idx, etf_info in enumerate(INDEX_ETFS):
    try:
        ticker = etf_info['ticker']
        index_name = etf_info['index_name']

        print(f"\n{index_name} ({ticker}) 데이터 수집 중... ({idx+1}/{len(INDEX_ETFS)})")

        index_data = fetch_index_data(etf_info)
        all_data.append(index_data)

        # 결과 출력
        change_str = ""
        if index_data['change'] is not None:
            sign = "+" if index_data['change'] >= 0 else ""
            change_str = f" ({sign}{index_data['change']:.2f}, {sign}{index_data['change_percent']:.2f}%)"

        print(f"  종가: ${index_data['close']:.2f}{change_str}")
        print(f"  거래량: {index_data['volume']:,}")
        print(f"  날짜: {index_data['date']}")

        # Rate Limit 대응: 다음 요청 전 대기
        if idx < len(INDEX_ETFS) - 1:
            time.sleep(RATE_LIMIT_DELAY)

    except Exception as e:
        print(f"  {etf_info['ticker']} 수집 실패: {str(e)}")
        continue

if not all_data:
    print("수집된 데이터가 없습니다.")
    spark.stop()
    exit(1)

print(f"\n총 {len(all_data)}개 지수 데이터 수집 완료")

# Pandas DataFrame을 Spark DataFrame으로 변환
pandas_df = pd.DataFrame(all_data)
df = spark.createDataFrame(pandas_df)

# 수집 시간 컬럼 추가
df = df.withColumn('collected_at', current_timestamp())

print("\n== RAW Data ==")
df.show(10, truncate=False)

# 파티션 계산
target_size_mb = 128
row_size_bytes = 600  # 대략적인 한 행 크기
num_rows = df.count()
total_size_mb = (num_rows * row_size_bytes) / (1024 * 1024)
num_partitions = max(1, int(total_size_mb / target_size_mb))

# 출력 경로 (S3)
output_path = f's3a://dw-pipeline-ch/raw/market_index/{target_date}'

print(f"\n데이터 저장 중: {output_path}")
df.repartition(num_partitions).write.mode("overwrite").parquet(output_path)

print(f"저장 완료! (파티션 수: {num_partitions})")

spark.stop()
