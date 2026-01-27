# CLAUDE.md

## Communication Language
**IMPORTANT: Claude must communicate in Korean (한국어) when working with this project.**

- 작업 보고서: 한국어
- 커밋 메시지: 한국어
- 설명 및 답변: 한국어

## Docker Command
**IMPORTANT: docker compose (not docker-compose)**

## Overview

데이터 웨어하우스 파이프라인 프로젝트(`dw-pipeline-ch`)로, Airflow, Spark, AWS S3, Hive Metastore, Trino, dbt를 통합한 ETL/ELT 워크플로우를 제공합니다.

주요 데이터 소스:
- 국내 주식 (KRX API)
- 해외 주식 (Alpha Vantage API)
- 가상자산 (CoinGecko API)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Orchestration (Airflow)                     │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Processing (Spark Cluster)                   │
│                  Master + Worker 1 + Worker 2                   │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Storage (AWS S3)                           │
│              Bucket: dw-pipeline-ch                             │
│   ├── raw/krx/YYYYMMDD/          (원시 데이터)                  │
│   ├── raw/foreign_stock/YYYYMMDD/                               │
│   ├── raw/crypto/YYYYMMDD/                                      │
│   └── warehouse/                  (Iceberg 테이블)              │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Catalog (Hive Metastore)                      │
│              테이블 스키마 및 메타데이터 관리                    │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Query (Trino + dbt)                        │
│               SQL 쿼리 및 데이터 변환                           │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
dw-pipeline-ch/
├── airflow/
│   ├── config/airflow.cfg
│   ├── dags/
│   │   ├── common/              # 공통 유틸리티
│   │   ├── spark_jobs/          # Spark job DAGs
│   │   ├── dbt_transform_dag.py
│   │   └── master_pipeline_dag.py
│   ├── logs/                    # (gitignore)
│   └── plugins/
├── dbt/
│   ├── models/
│   │   ├── staging/             # 스테이징 모델
│   │   └── mart/                # 마트 모델
│   ├── dbt_project.yml
│   └── profiles.yml
├── hive/
│   └── conf/
│       ├── core-site.xml        # S3 접근 설정
│       └── hive-site.xml
├── spark/
│   ├── data/                    # 참조 데이터
│   └── jobs/
│       ├── krx_api_ingest.py
│       ├── foreign_stock_ingest.py
│       ├── crypto_ingest.py
│       ├── create_iceberg_tables.py
│       └── load_to_iceberg.py
├── trino/
│   ├── catalog/
│   │   ├── hive.properties
│   │   └── iceberg.properties
│   └── jvm.config
├── docker-compose.yaml
├── Dockerfile.spark
├── Dockerfile.hive
└── .env                         # API 키 (gitignore)
```

## Environment Setup

### Prerequisites
- Docker and Docker Compose
- AWS CLI configured with profile `dw-pipeline`
- `.env` 파일:
  ```
  AIRFLOW__CORE__FERNET_KEY=<your-fernet-key>
  AIRFLOW__API_AUTH__JWT_SECRET=<your-jwt-secret>
  GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY=<krx-api-key>
  ALPHA_VANTAGE_API_KEY=<alpha-vantage-api-key>
  ```

### AWS Profile Setup
```bash
# ~/.aws/credentials
[dw-pipeline]
aws_access_key_id = <your-access-key>
aws_secret_access_key = <your-secret-key>

# ~/.aws/config
[profile dw-pipeline]
region = ap-northeast-2
```

### Starting Services
```bash
# 전체 서비스 시작
docker compose up -d

# 특정 서비스만 시작
docker compose up -d spark-master spark-worker-1 spark-worker-2
docker compose up -d hive-metastore-db hive-metastore trino

# 로그 확인
docker compose logs -f [service-name]
```

## Data Sources

### 1. 국내 주식 (KRX)
- **API**: 금융위원회 주식시세정보
- **인증**: `GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY`
- **Job**: `spark/jobs/krx_api_ingest.py`
- **스케줄**: 월~금 18:00 KST (장 마감 후)
- **데이터 지연**: 1-2일

### 2. 해외 주식 (Alpha Vantage)
- **API**: TIME_SERIES_DAILY
- **인증**: `ALPHA_VANTAGE_API_KEY`
- **Job**: `spark/jobs/foreign_stock_ingest.py`
- **종목**: AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, DNA, SOFI, QQQ
- **Rate Limit**: 분당 25회, 하루 500회 (무료 티어)

### 3. 가상자산 (CoinGecko)
- **API**: CoinGecko API v3
- **인증**: 불필요
- **Job**: `spark/jobs/crypto_ingest.py`
- **코인**: BTC, ETH, BNB, XRP 등 24개
- **스케줄**: 6시간마다

## Spark Jobs

### 수동 실행
```bash
# 컨테이너 내부에서 실행
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/krx_api_ingest.py [YYYYMMDD]'

# 해외 주식
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/foreign_stock_ingest.py'

# 가상자산
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/crypto_ingest.py'
```

### Iceberg 테이블 생성 및 로드
```bash
# 테이블 생성
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/create_iceberg_tables.py'

# 데이터 로드
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/load_to_iceberg.py krx 20260123'

docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/load_to_iceberg.py foreign_stock 20260127'

docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/load_to_iceberg.py crypto 20260127'
```

## Iceberg Tables

### Catalog Structure
- **Catalog**: `iceberg` (Spark), `iceberg` (Trino)
- **Schema**: `stock_data`
- **Warehouse**: `s3a://dw-pipeline-ch/warehouse`

### Tables
| 테이블 | 파티션 | 설명 |
|--------|--------|------|
| `stock_data.krx_stock_price` | `basDt` | 국내 주식 시세 |
| `stock_data.foreign_stock_price` | `date` | 해외 주식 시세 |
| `stock_data.crypto_price` | `date` | 가상자산 시세 |

### Spark Iceberg Config
```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://dw-pipeline-ch/warehouse") \
    .getOrCreate()
```

### Trino Iceberg Config
Trino는 Hive Metastore 카탈로그를 사용합니다 (`trino/catalog/iceberg.properties`).
**참고**: Spark(Hadoop 카탈로그)와 Trino(Hive Metastore 카탈로그)가 다른 카탈로그를 사용하므로, Trino에서 Iceberg 테이블을 조회하려면 Hive Metastore가 필요합니다.

## Port Reference

| Service | Port | Purpose |
|---------|------|---------|
| Airflow API | 8080 | Web UI / API |
| Spark Master | 8088 | Web UI |
| Spark Master | 7077 | Job submission |
| Spark Worker 1 | 8089 | Web UI |
| Spark Worker 2 | 8090 | Web UI |
| Trino | 8081 | Web UI & SQL |
| Hive Metastore | 9083 | Thrift service |
| Flower | 5555 | Celery monitoring (optional) |

## S3 Bucket Structure

```
s3://dw-pipeline-ch/
├── raw/
│   ├── krx/YYYYMMDD/           # 국내 주식 원시 데이터 (Parquet)
│   ├── foreign_stock/YYYYMMDD/ # 해외 주식 원시 데이터 (Parquet)
│   └── crypto/YYYYMMDD/        # 가상자산 원시 데이터 (Parquet)
└── warehouse/
    └── stock_data/             # Iceberg 테이블
        ├── krx_stock_price/
        ├── foreign_stock_price/
        └── crypto_price/
```

## Common Issues

### Docker 컨테이너 문제
```bash
# 컨테이너 완전 정리
docker compose down --volumes --remove-orphans
docker system prune -f
```

### Spark Submit 에러
`/opt/spark/work-dir` 관련 에러 발생 시:
```bash
# bash -c로 실행
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/<job>.py'
```

### KRX API 데이터 없음
- 주말/공휴일에는 데이터 없음
- 날짜 파라미터로 평일 지정: `krx_api_ingest.py 20260123`

### Trino-Iceberg 연동
Spark(Hadoop 카탈로그)와 Trino(Hive Metastore 카탈로그) 간 호환성 문제:
1. Hive Metastore 실행 필요
2. 또는 Spark에서도 Hive Metastore 카탈로그 사용하도록 변경

## DAG Schedule Summary

| DAG | 스케줄 | 실행 시간 |
|-----|-------|----------|
| `krx_stock_ingest` | 월~금 | 18:00 KST |
| `foreign_stock_ingest` | 월~금 | 09:00 KST |
| `crypto_ingest` | 매일 | 00:00, 06:00, 12:00, 18:00 KST |
