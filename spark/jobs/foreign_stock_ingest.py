import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

# 수집할 주요 해외 주식 티커 리스트
TICKERS = [
    # 미국 주요 기술주
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'AMD',
    # 미국 주요 지수 ETF
    'SPY', 'QQQ', 'DIA',
    # 기타 주요 종목
    'NFLX', 'DIS', 'PYPL', 'INTC', 'CSCO', 'ADBE', 'CRM'
]

# 어제 날짜
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"수집 대상 날짜: {yesterday}")

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Foreign Stock Price Ingest Job") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 데이터 수집
all_data = []

for ticker in TICKERS:
    try:
        print(f"티커 {ticker} 데이터 수집 중...")
        stock = yf.Ticker(ticker)

        # 최근 5일 데이터 수집 (휴장일 고려)
        hist = stock.history(period="5d")

        if not hist.empty:
            # 가장 최근 데이터
            latest_data = hist.iloc[-1]

            data_dict = {
                'ticker': ticker,
                'date': hist.index[-1].strftime('%Y-%m-%d'),
                'open': float(latest_data['Open']),
                'high': float(latest_data['High']),
                'low': float(latest_data['Low']),
                'close': float(latest_data['Close']),
                'volume': int(latest_data['Volume']),
                'currency': 'USD',
                'exchange': stock.info.get('exchange', 'UNKNOWN'),
                'company_name': stock.info.get('longName', ticker)
            }
            all_data.append(data_dict)
            print(f"  {ticker}: ${latest_data['Close']:.2f} (거래량: {latest_data['Volume']:,})")
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
output_date = datetime.today().strftime('%Y%m%d')
output_path = f'/opt/spark-apps/output/foreign_stock/{output_date}'

print(f"\n데이터 저장 중: {output_path}")
df.repartition(num_partitions).write.mode("overwrite").parquet(output_path)

print(f"저장 완료! (파티션 수: {num_partitions})")

spark.stop()
