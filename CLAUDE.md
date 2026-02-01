# CLAUDE.md

# Docker compose는 docker-compose가 아닌 docker compose로 입력할 것

# 중간에 git commit을 적절히 섞어줄 것

## Communication Language
**IMPORTANT: Claude must communicate in Korean (한국어) when working with this project.**

- 작업 보고서: 한국어
- 커밋 메시지: 한국어
- 설명 및 답변: 한국어

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

데이터 웨어하우스 파이프라인 프로젝트(`dw-pipeline-ch`)로, Airflow, Spark, AWS S3, DuckDB를 통합한 ETL/ELT 워크플로우를 제공합니다.

주요 데이터 소스:
- 국내 주식 (KRX API)
- 해외 주식 (Alpha Vantage API)
- 가상자산 (CoinGecko API)
- 환율 (ExchangeRate-API)
- 주식 지수 (Alpha Vantage API)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Orchestration (Airflow)                     │
│              CeleryExecutor + Redis + PostgreSQL                │
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
│   ├── raw/krx/YYYYMMDD/          (원시 데이터 - Parquet)        │
│   ├── raw/foreign_stock/YYYYMMDD/                               │
│   ├── raw/crypto/YYYYMMDD/                                      │
│   └── warehouse/stock_data/      (Iceberg 테이블)               │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Query (DuckDB)                             │
│              경량 분석용 SQL 엔진 + Iceberg 지원                 │
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
│   │   └── master_pipeline_dag.py
│   ├── logs/                    # (gitignore)
│   └── plugins/
├── duckdb/
│   ├── query_iceberg.py         # DuckDB Iceberg 조회 스크립트
│   └── start_harlequin.py       # Harlequin TUI 시작 스크립트
├── spark/
│   ├── data/                    # 참조 데이터
│   └── jobs/
│       ├── krx_api_ingest.py
│       ├── foreign_stock_ingest.py
│       ├── crypto_ingest.py
│       ├── exchange_rate_ingest.py
│       ├── market_index_ingest.py
│       ├── create_iceberg_tables.py
│       └── load_to_iceberg.py
├── docker-compose.yaml
├── Dockerfile.spark
├── Dockerfile.duckdb
└── .env                         # API 키 (gitignore)
```

## Environment Setup

**Prerequisites**
- Docker and Docker Compose
- AWS CLI configured with profile `dw-pipeline`
- `.env` 파일:
  ```
  AIRFLOW__CORE__FERNET_KEY=<your-fernet-key>
  AIRFLOW__API_AUTH__JWT_SECRET=<your-jwt-secret>
  GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY=<krx-api-key>
  ALPHA_VANTAGE_API_KEY=<alpha-vantage-api-key>
  ```

**AWS Profile Setup**
```bash
# ~/.aws/credentials
[dw-pipeline]
aws_access_key_id = <your-access-key>
aws_secret_access_key = <your-secret-key>

# ~/.aws/config
[profile dw-pipeline]
region = ap-northeast-2
```

**Starting Services**
```bash
# 전체 서비스 시작
docker compose up -d

# 특정 서비스만 시작
docker compose up -d spark-master spark-worker-1 spark-worker-2
docker compose up -d duckdb

# Flower 모니터링 포함
docker compose --profile flower up -d

# 로그 확인
docker compose logs -f [service-name]

# 서비스 중지
docker compose down
docker compose down -v  # 볼륨 포함
```

**Accessing Services**
- Airflow UI: http://localhost:8080 (user: airflow, pass: airflow)
- Spark Master UI: http://localhost:8088
- Flower: http://localhost:5555 (optional)

## Data Sources

### 1. 국내 주식 (KRX)
- **API**: 금융위원회 주식시세정보 API
- **인증**: `GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY`
- **Job**: `spark/jobs/krx_api_ingest.py`
- **스케줄**: 월~금 18:00 KST (장 마감 후)
- **데이터 지연**: 1-2일 (API 특성상)
- **저장**: `s3a://dw-pipeline-ch/raw/krx/YYYYMMDD/`

### 2. 해외 주식 (Alpha Vantage)
- **API**: TIME_SERIES_DAILY
- **인증**: `ALPHA_VANTAGE_API_KEY`
- **Job**: `spark/jobs/foreign_stock_ingest.py`
- **종목**: AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, DNA, SOFI, QQQ
- **Rate Limit**: 분당 25회, 하루 500회 (무료 티어)
- **저장**: `s3a://dw-pipeline-ch/raw/foreign_stock/YYYYMMDD/`

### 3. 가상자산 (CoinGecko)
- **API**: CoinGecko API v3
- **인증**: 불필요
- **Job**: `spark/jobs/crypto_ingest.py`
- **코인**: BTC, ETH, BNB, XRP 등 24개
- **스케줄**: 6시간마다 (00:00, 06:00, 12:00, 18:00 KST)
- **저장**: `s3a://dw-pipeline-ch/raw/crypto/YYYYMMDD/`

### 4. 환율 (ExchangeRate-API)
- **API**: ExchangeRate-API Open Access
- **인증**: 불필요
- **Job**: `spark/jobs/exchange_rate_ingest.py`
- **기준 통화**: KRW (원화)
- **대상 통화**: USD, EUR, JPY, CNY, GBP, CHF, AUD
- **저장**: `s3a://dw-pipeline-ch/raw/exchange_rate/YYYYMMDD/`

### 5. 주식 지수 (Alpha Vantage)
- **API**: TIME_SERIES_DAILY (ETF 기반)
- **인증**: `ALPHA_VANTAGE_API_KEY`
- **Job**: `spark/jobs/market_index_ingest.py`
- **지수**: S&P 500 (SPY), 다우존스 (DIA), 나스닥 100 (QQQ)
- **저장**: `s3a://dw-pipeline-ch/raw/market_index/YYYYMMDD/`

## Spark Jobs

### 수동 실행
```bash
# 국내 주식 (날짜 파라미터 옵션)
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/krx_api_ingest.py [YYYYMMDD]'

# 해외 주식
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/foreign_stock_ingest.py'

# 가상자산
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/crypto_ingest.py'

# 환율
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/exchange_rate_ingest.py'

# 주식 지수
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/market_index_ingest.py'
```

### Iceberg 테이블 생성 및 로드
```bash
# 테이블 생성
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/create_iceberg_tables.py'

# 데이터 로드 (data_source: krx, foreign_stock, crypto, exchange_rate, market_index)
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/load_to_iceberg.py <data_source> <YYYYMMDD>'
```

## Iceberg Tables

### Catalog Structure
- **Catalog**: `iceberg`
- **Schema**: `stock_data`
- **Warehouse**: `s3a://dw-pipeline-ch/warehouse`

### Tables
| 테이블 | 파티션 | 설명 |
|--------|--------|------|
| `iceberg.stock_data.krx_stock_price` | `basDt` | 국내 주식 시세 |
| `iceberg.stock_data.foreign_stock_price` | `date` | 해외 주식 시세 |
| `iceberg.stock_data.crypto_price` | `date` | 가상자산 시세 |
| `iceberg.stock_data.exchange_rate` | `date` | KRW 기준 환율 |
| `iceberg.stock_data.market_index` | `date` | 미국 주식 지수 |

### Spark Iceberg Config
```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://dw-pipeline-ch/warehouse") \
    .getOrCreate()
```

### DuckDB Iceberg 조회
```bash
# 전체 테이블 조회
docker exec -it duckdb python /app/query_iceberg.py

# 특정 테이블만 조회
docker exec -it duckdb python /app/query_iceberg.py krx
docker exec -it duckdb python /app/query_iceberg.py foreign_stock
docker exec -it duckdb python /app/query_iceberg.py crypto
docker exec -it duckdb python /app/query_iceberg.py exchange_rate
docker exec -it duckdb python /app/query_iceberg.py market_index

# Harlequin TUI (터미널 기반 GUI)
docker exec -it duckdb python /app/start_harlequin.py

# DuckDB CLI 직접 사용
docker exec -it duckdb duckdb
```

## S3 Bucket Structure

```
s3://dw-pipeline-ch/
├── raw/
│   ├── krx/YYYYMMDD/           # 국내 주식 원시 데이터 (Parquet)
│   ├── foreign_stock/YYYYMMDD/ # 해외 주식 원시 데이터 (Parquet)
│   ├── crypto/YYYYMMDD/        # 가상자산 원시 데이터 (Parquet)
│   ├── exchange_rate/YYYYMMDD/ # 환율 원시 데이터 (Parquet)
│   └── market_index/YYYYMMDD/  # 주식 지수 원시 데이터 (Parquet)
└── warehouse/
    └── stock_data/             # Iceberg 테이블
        ├── krx_stock_price/
        ├── foreign_stock_price/
        ├── crypto_price/
        ├── exchange_rate/
        └── market_index/
```

## Port Reference

| Service | Port | Purpose |
|---------|------|---------|
| Airflow API | 8080 | Web UI / API |
| Spark Master | 8088 | Web UI |
| Spark Master | 7077 | Job submission |
| Spark Worker 1 | 8089 | Web UI |
| Spark Worker 2 | 8090 | Web UI |
| Flower | 5555 | Celery monitoring (optional) |

## DAG Schedule Summary

| DAG | 스케줄 | 실행 시간 |
|-----|-------|----------|
| `krx_stock_ingest` | 월~금 | 18:00 KST |
| `foreign_stock_ingest` | 월~금 | 09:00 KST |
| `crypto_ingest` | 매일 | 00:00, 06:00, 12:00, 18:00 KST |
| `exchange_rate_ingest` | 매일 | 09:00 KST |
| `market_index_ingest` | 월~금 | 09:00 KST |

모든 DAG: `catchup=False` (과거 실행 스킵)

## Common Issues

### Docker 컨테이너 문제
```bash
docker compose down --volumes --remove-orphans
docker system prune -f
```

### Spark Submit 에러
`/opt/spark/work-dir` 관련 에러 시 `bash -c` 방식 사용:
```bash
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/<job>.py'
```

### KRX API 데이터 없음
- 주말/공휴일에는 데이터 없음
- 날짜 파라미터로 평일 지정: `krx_api_ingest.py 20260123`

### DuckDB Iceberg 조회
DuckDB의 `iceberg_scan()` 함수로 S3의 Iceberg 테이블 직접 조회 가능:
```sql
SELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/krx_stock_price');
```
