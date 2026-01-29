#!/usr/bin/env python3
"""
Spark를 사용하여 Iceberg 테이블을 생성하는 스크립트
AWS S3 (버킷: dw-pipeline-ch)에 데이터 저장
"""

from pyspark.sql import SparkSession

# Spark 세션 생성 (Iceberg 설정 포함)
spark = SparkSession.builder \
    .appName("Create Iceberg Tables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://dw-pipeline-ch/warehouse") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

print("="*80)
print("Iceberg 테이블 생성 시작")
print("="*80)

# 1. 스키마 생성
print("\n[1/6] Iceberg 스키마 생성 중...")
spark.sql("CREATE SCHEMA IF NOT EXISTS iceberg.stock_data")
print("✓ 스키마 생성 완료: iceberg.stock_data")

# 2. 국내 주식 (KRX) Iceberg 테이블 생성
print("\n[2/6] KRX 주식 테이블 생성 중...")
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.stock_data.krx_stock_price (
    basDt STRING COMMENT '기준일자',
    srtnCd STRING COMMENT '단축코드',
    isinCd STRING COMMENT 'ISIN 코드',
    itmsNm STRING COMMENT '종목명',
    mrktCtg STRING COMMENT '시장구분',
    clpr STRING COMMENT '종가',
    vs STRING COMMENT '전일대비',
    fltRt STRING COMMENT '등락률',
    mkp STRING COMMENT '시가',
    hipr STRING COMMENT '고가',
    lopr STRING COMMENT '저가',
    trqu STRING COMMENT '거래량',
    trPrc STRING COMMENT '거래대금',
    lstgStCnt STRING COMMENT '상장주식수',
    mrktTotAmt STRING COMMENT '시가총액'
)
USING iceberg
PARTITIONED BY (basDt)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""")
print("✓ KRX 주식 테이블 생성 완료")

# 3. 해외 주식 Iceberg 테이블 생성
print("\n[3/6] 해외 주식 테이블 생성 중...")
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.stock_data.foreign_stock_price (
    ticker STRING COMMENT '티커 심볼',
    date STRING COMMENT '날짜',
    open DOUBLE COMMENT '시가',
    high DOUBLE COMMENT '고가',
    low DOUBLE COMMENT '저가',
    close DOUBLE COMMENT '종가',
    volume BIGINT COMMENT '거래량',
    currency STRING COMMENT '통화',
    exchange STRING COMMENT '거래소',
    company_name STRING COMMENT '회사명',
    collected_at TIMESTAMP COMMENT '수집 시간'
)
USING iceberg
PARTITIONED BY (date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""")
print("✓ 해외 주식 테이블 생성 완료")

# 4. 가상자산 Iceberg 테이블 생성
print("\n[4/6] 가상자산 테이블 생성 중...")
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.stock_data.crypto_price (
    coin_id STRING COMMENT '코인 ID',
    symbol STRING COMMENT '심볼',
    name STRING COMMENT '이름',
    current_price_usd DOUBLE COMMENT '현재 가격 (USD)',
    market_cap_usd DOUBLE COMMENT '시가총액 (USD)',
    market_cap_rank INT COMMENT '시가총액 순위',
    total_volume_24h_usd DOUBLE COMMENT '24시간 거래량 (USD)',
    high_24h_usd DOUBLE COMMENT '24시간 최고가 (USD)',
    low_24h_usd DOUBLE COMMENT '24시간 최저가 (USD)',
    price_change_24h_usd DOUBLE COMMENT '24시간 가격 변화 (USD)',
    price_change_percentage_24h DOUBLE COMMENT '24시간 가격 변화율 (%)',
    circulating_supply DOUBLE COMMENT '유통 공급량',
    total_supply DOUBLE COMMENT '총 공급량',
    ath_usd DOUBLE COMMENT '역대 최고가 (USD)',
    atl_usd DOUBLE COMMENT '역대 최저가 (USD)',
    last_updated STRING COMMENT '마지막 업데이트',
    collected_at TIMESTAMP COMMENT '수집 시간',
    date STRING COMMENT '날짜'
)
USING iceberg
PARTITIONED BY (date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""")
print("✓ 가상자산 테이블 생성 완료")

# 5. 환율 Iceberg 테이블 생성
print("\n[5/6] 환율 테이블 생성 중...")
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.stock_data.exchange_rate (
    currency_code STRING COMMENT '통화 코드',
    currency_name STRING COMMENT '통화명',
    country STRING COMMENT '국가',
    base_currency STRING COMMENT '기준 통화',
    rate DOUBLE COMMENT '환율 (1 외화 = X KRW)',
    inverse_rate DOUBLE COMMENT '역환율 (1 KRW = X 외화)',
    date STRING COMMENT '날짜',
    api_update_time STRING COMMENT 'API 업데이트 시간',
    collected_at TIMESTAMP COMMENT '수집 시간'
)
USING iceberg
PARTITIONED BY (date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""")
print("✓ 환율 테이블 생성 완료")

# 6. 주식 지수 Iceberg 테이블 생성
print("\n[6/6] 주식 지수 테이블 생성 중...")
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.stock_data.market_index (
    ticker STRING COMMENT '티커 (ETF)',
    index_name STRING COMMENT '지수명',
    index_symbol STRING COMMENT '지수 심볼',
    description STRING COMMENT '설명',
    exchange STRING COMMENT '거래소',
    date STRING COMMENT '날짜',
    open DOUBLE COMMENT '시가',
    high DOUBLE COMMENT '고가',
    low DOUBLE COMMENT '저가',
    close DOUBLE COMMENT '종가',
    volume BIGINT COMMENT '거래량',
    prev_close DOUBLE COMMENT '전일 종가',
    change DOUBLE COMMENT '변동',
    change_percent DOUBLE COMMENT '변동률 (%)',
    currency STRING COMMENT '통화',
    collected_at TIMESTAMP COMMENT '수집 시간'
)
USING iceberg
PARTITIONED BY (date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""")
print("✓ 주식 지수 테이블 생성 완료")

# 생성된 테이블 확인
print("\n" + "="*80)
print("생성된 테이블 목록:")
print("="*80)
tables = spark.sql("SHOW TABLES IN iceberg.stock_data").collect()
for table in tables:
    print(f"  - {table.namespace}.{table.tableName}")

print("\n" + "="*80)
print("✓ 모든 Iceberg 테이블 생성 완료!")
print("  - Catalog: iceberg")
print("  - Schema: stock_data")
print("  - 저장소: s3a://dw-pipeline-ch/warehouse")
print("  - 포맷: Iceberg v2 (Parquet + Snappy)")
print("="*80)

spark.stop()
