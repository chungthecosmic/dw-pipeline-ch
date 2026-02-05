# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Claude Instructions

**Communication Language**: 한국어 (Korean)
- 작업 보고서, 커밋 메시지, 설명 및 답변 모두 한국어로 작성

**Commands**:
- Docker Compose: `docker compose` (not `docker-compose`)
- 작업 중간에 적절히 git commit 수행

---

## Overview

데이터 웨어하우스 파이프라인 프로젝트로, Airflow, Spark, AWS S3, DuckDB를 통합한 ETL/ELT 워크플로우를 제공합니다.

### 데이터 소스
| 소스 | API | 인증 |
|-----|-----|-----|
| 국내 주식 | 금융위원회 KRX API | API 키 필요 |
| 해외 주식 | Alpha Vantage | API 키 필요 |
| 가상자산 | CoinGecko | 불필요 |
| 환율 | ExchangeRate-API | 불필요 |
| 주식 지수 | Alpha Vantage (ETF) | API 키 필요 |

---

## Quick Start

### 1. 서비스 시작
```bash
# Spark 클러스터 시작
docker compose up -d spark-master spark-worker-1 spark-worker-2

# DuckDB 시작
docker compose up -d duckdb

# 전체 서비스 (Airflow 포함)
docker compose up -d
```

### 2. 데이터 수집 (Spark Job)
```bash
# 국내 주식
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/krx_api_ingest.py [YYYYMMDD]'

# 해외 주식
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/foreign_stock_ingest.py [YYYYMMDD]'

# 가상자산
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/crypto_ingest.py [YYYYMMDD]'

# 환율
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/exchange_rate_ingest.py [YYYYMMDD]'

# 주식 지수
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/market_index_ingest.py [YYYYMMDD]'
```

### 3. Iceberg 테이블 적재
```bash
# 테이블 생성 (최초 1회)
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/create_iceberg_tables.py'

# 데이터 적재 (data_source: krx, foreign_stock, crypto, exchange_rate, market_index)
docker exec -e HOME=/home/spark -w /opt/spark spark-master bash -c \
  '/opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/jobs/load_to_iceberg.py <data_source> <YYYYMMDD>'
```

### 4. 데이터 조회 (DuckDB)
```bash
# 전체 테이블 조회
docker exec duckdb python /app/query_iceberg.py

# 특정 테이블만 조회
docker exec duckdb python /app/query_iceberg.py krx
docker exec duckdb python /app/query_iceberg.py foreign_stock
docker exec duckdb python /app/query_iceberg.py crypto
docker exec duckdb python /app/query_iceberg.py exchange_rate
docker exec duckdb python /app/query_iceberg.py market_index
```

---

## Web App (데이터 카탈로그)

테이블 스키마 및 데이터를 조회할 수 있는 웹 애플리케이션입니다.

### 서비스 시작
```bash
docker compose up -d webapp-backend webapp-frontend
```

### 접속
- **Frontend**: http://localhost:5173
- **Backend API**: http://localhost:8000

### 기능
- 테이블 목록 및 메타데이터 조회
- 테이블 스키마 확인
- 테이블 통계 (레코드 수, 파티션 수, 날짜 범위)
- 샘플 데이터 미리보기
- SQL 쿼리 실행 (SELECT만 허용)

### API 엔드포인트
| 엔드포인트 | 설명 |
|-----------|------|
| GET /api/tables | 테이블 목록 |
| GET /api/tables/{id} | 테이블 정보 |
| GET /api/tables/{id}/schema | 테이블 스키마 |
| GET /api/tables/{id}/stats | 테이블 통계 |
| GET /api/tables/{id}/data | 테이블 데이터 |
| POST /api/tables/query | SQL 쿼리 실행 |

---

## DuckDB 사용법

### Harlequin TUI (SQL 에디터)
터미널 기반 GUI SQL 에디터로, 인터랙티브하게 쿼리 작성 및 실행 가능:
```bash
docker exec -it duckdb python /app/start_harlequin.py
```

**Harlequin 단축키:**
- `Ctrl+Enter`: 쿼리 실행
- `Ctrl+O`: 파일 열기
- `Ctrl+S`: 쿼리 저장
- `F1`: 도움말

### DuckDB CLI
직접 SQL 명령어 입력:
```bash
docker exec -it duckdb duckdb
```

### Iceberg 테이블 직접 조회
```sql
-- S3의 Iceberg 테이블 조회
SELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/krx_stock_price');
SELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/foreign_stock_price');
SELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/crypto_price');
SELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/exchange_rate');
SELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/market_index');
```

---

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
│   ├── raw/                      (원시 데이터 - Parquet)         │
│   └── warehouse/stock_data/     (Iceberg 테이블)                │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Query (DuckDB + Harlequin)                 │
│              경량 분석용 SQL 엔진 + Iceberg 지원                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
dw-pipeline-ch/
├── airflow/
│   ├── config/airflow.cfg
│   ├── dags/
│   │   ├── common/                    # 공통 유틸리티
│   │   └── spark_jobs/                # Spark job DAGs
│   │       ├── krx_stock_ingest_dag.py
│   │       ├── foreign_stock_ingest_dag.py
│   │       ├── crypto_ingest_dag.py
│   │       ├── exchange_rate_ingest_dag.py
│   │       └── market_index_ingest_dag.py
│   ├── logs/                          # (gitignore)
│   └── plugins/
├── duckdb/
│   ├── query_iceberg.py               # Iceberg 테이블 조회 스크립트
│   └── start_harlequin.py             # Harlequin TUI 시작 스크립트
├── spark/
│   ├── data/                          # 참조 데이터
│   └── jobs/
│       ├── krx_api_ingest.py          # 국내 주식 수집
│       ├── foreign_stock_ingest.py    # 해외 주식 수집
│       ├── crypto_ingest.py           # 가상자산 수집
│       ├── exchange_rate_ingest.py    # 환율 수집
│       ├── market_index_ingest.py     # 주식 지수 수집
│       ├── create_iceberg_tables.py   # Iceberg 테이블 생성
│       └── load_to_iceberg.py         # Iceberg 테이블 적재
├── webapp/
│   ├── backend/                       # FastAPI 백엔드
│   │   ├── app/
│   │   │   ├── main.py
│   │   │   ├── database.py
│   │   │   └── routers/tables.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── frontend/                      # React + TypeScript 프론트엔드
│       ├── src/
│       │   ├── App.tsx
│       │   ├── api/tables.ts
│       │   └── types/index.ts
│       ├── package.json
│       └── Dockerfile
├── docker-compose.yaml
├── Dockerfile.spark
├── Dockerfile.duckdb
└── .env                               # API 키 (gitignore)
```

---

## Environment Setup

### Prerequisites
- Docker and Docker Compose
- AWS CLI configured with profile `dw-pipeline`

### .env 파일
```
AIRFLOW__CORE__FERNET_KEY=<your-fernet-key>
AIRFLOW__API_AUTH__JWT_SECRET=<your-jwt-secret>
GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY=<krx-api-key>
ALPHA_VANTAGE_API_KEY=<alpha-vantage-api-key>
```

### AWS Profile
```bash
# ~/.aws/credentials
[dw-pipeline]
aws_access_key_id = <your-access-key>
aws_secret_access_key = <your-secret-key>

# ~/.aws/config
[profile dw-pipeline]
region = ap-northeast-2
```

---

## Data Sources Detail

### 1. 국내 주식 (KRX)
- **API**: 금융위원회 주식시세정보 API
- **인증**: `GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY`
- **스케줄**: 월~금 18:00 KST
- **데이터 지연**: 1-2일 (API 특성상)
- **저장**: `s3a://dw-pipeline-ch/raw/krx/YYYYMMDD/`

### 2. 해외 주식 (Alpha Vantage)
- **API**: TIME_SERIES_DAILY
- **인증**: `ALPHA_VANTAGE_API_KEY`
- **종목**: AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, DNA, SOFI, QQQ
- **Rate Limit**: 분당 25회, 하루 500회 (무료 티어)
- **스케줄**: 월~금 09:00 KST
- **저장**: `s3a://dw-pipeline-ch/raw/foreign_stock/YYYYMMDD/`

### 3. 가상자산 (CoinGecko)
- **API**: CoinGecko API v3
- **인증**: 불필요
- **코인**: BTC, ETH, BNB, XRP 등 24개
- **스케줄**: 6시간마다 (00:00, 06:00, 12:00, 18:00 KST)
- **저장**: `s3a://dw-pipeline-ch/raw/crypto/YYYYMMDD/`

### 4. 환율 (ExchangeRate-API)
- **API**: ExchangeRate-API Open Access
- **인증**: 불필요
- **기준 통화**: KRW (원화)
- **대상 통화**: USD, EUR, JPY, CNY, GBP, CHF, AUD
- **스케줄**: 매일 09:00 KST
- **저장**: `s3a://dw-pipeline-ch/raw/exchange_rate/YYYYMMDD/`

### 5. 주식 지수 (Alpha Vantage)
- **API**: TIME_SERIES_DAILY (ETF 기반)
- **인증**: `ALPHA_VANTAGE_API_KEY`
- **지수**: S&P 500 (SPY), 다우존스 (DIA), 나스닥 100 (QQQ)
- **스케줄**: 월~금 09:00 KST
- **저장**: `s3a://dw-pipeline-ch/raw/market_index/YYYYMMDD/`

---

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

---

## S3 Bucket Structure

```
s3://dw-pipeline-ch/
├── raw/
│   ├── krx/YYYYMMDD/           # 국내 주식 (Parquet)
│   ├── foreign_stock/YYYYMMDD/ # 해외 주식 (Parquet)
│   ├── crypto/YYYYMMDD/        # 가상자산 (Parquet)
│   ├── exchange_rate/YYYYMMDD/ # 환율 (Parquet)
│   └── market_index/YYYYMMDD/  # 주식 지수 (Parquet)
└── warehouse/
    └── stock_data/             # Iceberg 테이블
        ├── krx_stock_price/
        ├── foreign_stock_price/
        ├── crypto_price/
        ├── exchange_rate/
        └── market_index/
```

---

## Airflow DAG Schedule

| DAG | 스케줄 | 실행 시간 |
|-----|-------|----------|
| `krx_stock_ingest` | 월~금 | 18:00 KST |
| `foreign_stock_ingest` | 월~금 | 09:00 KST |
| `crypto_ingest` | 매일 | 00:00, 06:00, 12:00, 18:00 KST |
| `exchange_rate_ingest` | 매일 | 09:00 KST |
| `market_index_ingest` | 월~금 | 09:00 KST |

모든 DAG: `catchup=False` (과거 실행 스킵)

---

## Port Reference

| Service | Port | Purpose |
|---------|------|---------|
| Airflow API | 8080 | Web UI / API |
| Spark Master | 8088 | Web UI |
| Spark Master | 7077 | Job submission |
| Spark Worker 1 | 8089 | Web UI |
| Spark Worker 2 | 8090 | Web UI |
| Flower | 5555 | Celery monitoring (optional) |
| Webapp Frontend | 5173 | React Web UI |
| Webapp Backend | 8000 | FastAPI REST API |

---

## Troubleshooting

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

### Alpha Vantage Rate Limit
- 분당 25회, 하루 500회 제한
- 해외 주식과 주식 지수 수집 시 2.5초 간격 자동 대기
