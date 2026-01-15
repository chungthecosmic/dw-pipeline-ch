import os
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime
import time

# 수집할 주요 가상자산 목록 (CoinGecko ID 기준)
CRYPTO_IDS = [
    'bitcoin', 'ethereum', 'tether', 'binancecoin', 'ripple',
    'cardano', 'solana', 'polkadot', 'dogecoin', 'avalanche-2',
    'shiba-inu', 'polygon', 'chainlink', 'litecoin', 'bitcoin-cash',
    'stellar', 'uniswap', 'monero', 'ethereum-classic', 'filecoin',
    # 한국 거래소에서 인기 있는 종목들
    'aptos', 'sui', 'sei-network', 'the-open-network'
]

# 날짜 파라미터 받기: python crypto_ingest.py 20260113
# 파라미터가 없으면 기본값 (오늘 날짜) 사용
if len(sys.argv) > 1:
    target_date = sys.argv[1]  # YYYYMMDD 형식
    # YYYY-MM-DD 형식으로 변환 (데이터 컬럼용)
    current_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}"
else:
    target_date = datetime.today().strftime('%Y%m%d')
    current_date = datetime.today().strftime('%Y-%m-%d')

print(f"수집 날짜: {current_date} (저장 경로: {target_date})")

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Crypto Price Ingest Job") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# CoinGecko API로 데이터 수집
all_data = []

# 한번에 많은 요청을 피하기 위해 배치로 처리
batch_size = 10
for i in range(0, len(CRYPTO_IDS), batch_size):
    batch = CRYPTO_IDS[i:i+batch_size]
    ids_param = ','.join(batch)

    try:
        print(f"배치 {i//batch_size + 1}/{(len(CRYPTO_IDS)-1)//batch_size + 1} 수집 중...")

        # CoinGecko API v3 호출
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': 'usd',
            'ids': ids_param,
            'order': 'market_cap_desc',
            'per_page': batch_size,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '24h'
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        for coin in data:
            coin_data = {
                'coin_id': coin['id'],
                'symbol': coin['symbol'].upper(),
                'name': coin['name'],
                'current_price_usd': float(coin['current_price']) if coin['current_price'] else 0.0,
                'market_cap_usd': float(coin['market_cap']) if coin['market_cap'] else 0.0,
                'market_cap_rank': int(coin['market_cap_rank']) if coin['market_cap_rank'] else None,
                'total_volume_24h_usd': float(coin['total_volume']) if coin['total_volume'] else 0.0,
                'high_24h_usd': float(coin['high_24h']) if coin['high_24h'] else 0.0,
                'low_24h_usd': float(coin['low_24h']) if coin['low_24h'] else 0.0,
                'price_change_24h_usd': float(coin['price_change_24h']) if coin['price_change_24h'] else 0.0,
                'price_change_percentage_24h': float(coin['price_change_percentage_24h']) if coin['price_change_percentage_24h'] else 0.0,
                'circulating_supply': float(coin['circulating_supply']) if coin['circulating_supply'] else 0.0,
                'total_supply': float(coin['total_supply']) if coin['total_supply'] else 0.0,
                'ath_usd': float(coin['ath']) if coin['ath'] else 0.0,
                'atl_usd': float(coin['atl']) if coin['atl'] else 0.0,
                'last_updated': coin['last_updated']
            }
            all_data.append(coin_data)
            print(f"  {coin['symbol'].upper()}: ${coin['current_price']:,.2f} (시가총액 순위: {coin['market_cap_rank']})")

        # API Rate Limit 고려하여 대기 (무료 API는 분당 10-50 요청 제한)
        if i + batch_size < len(CRYPTO_IDS):
            time.sleep(6)  # 6초 대기 (안전하게 분당 10회 이하로 제한)

    except requests.exceptions.RequestException as e:
        print(f"배치 수집 실패: {str(e)}")
        continue
    except Exception as e:
        print(f"데이터 처리 실패: {str(e)}")
        continue

if not all_data:
    print("수집된 데이터가 없습니다.")
    spark.stop()
    exit(1)

print(f"\n총 {len(all_data)}개 코인 데이터 수집 완료")

# Spark DataFrame 생성
df = spark.createDataFrame(all_data)

# 수집 시간 및 날짜 컬럼 추가
df = df.withColumn('collected_at', current_timestamp()) \
       .withColumn('date', lit(current_date))

print("\n== RAW Data ==")
df.show(10, truncate=False)

# 통계 정보 출력
print("\n== 통계 ==")
print(f"총 시가총액: ${df.agg({'market_cap_usd': 'sum'}).collect()[0][0]:,.0f}")
print(f"평균 가격: ${df.agg({'current_price_usd': 'avg'}).collect()[0][0]:,.2f}")
print(f"24시간 평균 변동률: {df.agg({'price_change_percentage_24h': 'avg'}).collect()[0][0]:.2f}%")

# 파티션 계산
target_size_mb = 128
row_size_bytes = 800  # 대략적인 한 행 크기
num_rows = df.count()
total_size_mb = (num_rows * row_size_bytes) / (1024 * 1024)
num_partitions = max(1, int(total_size_mb / target_size_mb))

# 출력 경로
output_path = f'/opt/spark-apps/output/crypto/{target_date}'

print(f"\n데이터 저장 중: {output_path}")
df.repartition(num_partitions).write.mode("overwrite").parquet(output_path)

print(f"저장 완료! (파티션 수: {num_partitions})")

spark.stop()
