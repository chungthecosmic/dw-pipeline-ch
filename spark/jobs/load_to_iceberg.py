#!/usr/bin/env python3
"""
S3에 저장된 원시 데이터를 Iceberg 테이블에 적재하는 스크립트
사용법: python load_to_iceberg.py [data_source] [date]
  - data_source: krx, foreign_stock, crypto, exchange_rate, market_index
  - date: YYYYMMDD 형식 (생략 시 최신 데이터 자동 탐색)
"""

import sys
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# 파라미터 받기
VALID_SOURCES = ['krx', 'foreign_stock', 'crypto', 'exchange_rate', 'market_index']

if len(sys.argv) < 2:
    print("사용법: python load_to_iceberg.py [data_source] [date]")
    print(f"  - data_source: {', '.join(VALID_SOURCES)}")
    print("  - date: YYYYMMDD 형식 (옵션)")
    sys.exit(1)

data_source = sys.argv[1]
if data_source not in VALID_SOURCES:
    print(f"오류: 잘못된 data_source '{data_source}'")
    print(f"가능한 값: {', '.join(VALID_SOURCES)}")
    sys.exit(1)

# 날짜 파라미터 (옵션)
if len(sys.argv) > 2:
    target_date = sys.argv[2]
else:
    # 기본값: KRX는 2일 전, 나머지는 오늘
    if data_source == 'krx':
        target_date = (datetime.today() - timedelta(days=2)).strftime('%Y%m%d')
    else:
        target_date = datetime.today().strftime('%Y%m%d')

print("="*80)
print(f"Iceberg 테이블 데이터 적재 시작")
print(f"  - 데이터 소스: {data_source}")
print(f"  - 날짜: {target_date}")
print("="*80)

# Spark 세션 생성 (Iceberg + S3 설정)
spark = SparkSession.builder \
    .appName(f"Load {data_source} to Iceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://dw-pipeline-ch/warehouse") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# S3 경로 및 Iceberg 테이블 매핑
source_path = f"s3a://dw-pipeline-ch/raw/{data_source}/{target_date}"

# 데이터 소스별 테이블 이름 매핑
table_mapping = {
    'krx': 'iceberg.stock_data.krx_stock_price',
    'foreign_stock': 'iceberg.stock_data.foreign_stock_price',
    'crypto': 'iceberg.stock_data.crypto_price',
    'exchange_rate': 'iceberg.stock_data.exchange_rate',
    'market_index': 'iceberg.stock_data.market_index'
}

target_table = table_mapping[data_source]

try:
    # 1. S3에서 원시 데이터 읽기
    print(f"\n[1/3] S3에서 데이터 읽는 중: {source_path}")
    df = spark.read.parquet(source_path)

    record_count = df.count()
    print(f"✓ 읽기 완료: {record_count:,}건")

    # 데이터 미리보기
    print("\n== 데이터 미리보기 ==")
    df.show(5, truncate=False)

    # 2. 스키마 검증 (옵션)
    print(f"\n[2/3] 스키마 검증 중...")
    print(f"  컬럼 수: {len(df.columns)}")
    print(f"  컬럼 목록: {', '.join(df.columns)}")

    # 3. Iceberg 테이블에 적재
    print(f"\n[3/3] Iceberg 테이블에 적재 중: {target_table}")

    # 데이터 소스별로 파티션 컬럼이 다름
    partition_mapping = {
        'krx': 'basDt',
        'foreign_stock': 'date',
        'crypto': 'date',
        'exchange_rate': 'date',
        'market_index': 'date'
    }
    partition_col = partition_mapping[data_source]

    # Iceberg 테이블에 INSERT (append 모드)
    # 같은 날짜 데이터가 있으면 먼저 삭제 후 적재
    print(f"  파티션 컬럼: {partition_col}")
    print(f"  적재 모드: overwrite (파티션 단위)")

    # partitionOverwriteMode를 dynamic으로 설정하여 해당 파티션만 덮어쓰기
    df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .option("overwrite-mode", "dynamic") \
        .save(target_table)

    print(f"✓ 적재 완료: {record_count:,}건")

    # 4. 적재 결과 확인
    print(f"\n== 적재 결과 확인 ==")
    result_df = spark.sql(f"SELECT * FROM {target_table} WHERE {partition_col} = '{target_date}' LIMIT 5")
    result_df.show(5, truncate=False)

    # 전체 레코드 수 확인
    total_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]['cnt']
    print(f"\n테이블 전체 레코드 수: {total_count:,}건")

    print("\n" + "="*80)
    print("✓ 데이터 적재 완료!")
    print(f"  - 소스: {source_path}")
    print(f"  - 대상: {target_table}")
    print(f"  - 적재 건수: {record_count:,}건")
    print("="*80)

except Exception as e:
    print(f"\n오류 발생: {str(e)}")
    import traceback
    traceback.print_exc()
    spark.stop()
    sys.exit(1)

spark.stop()
