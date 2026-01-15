# CLAUDE.md

## Communication Language
**IMPORTANT: Claude must communicate in Korean (한국어) when working with this project.**

- 작업 보고서: 한국어
- 커밋 메시지: 한국어
- 설명 및 답변: 한국어

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

데이터 웨어하우스 파이프라인 프로젝트(`dw-pipeline-ch`)로, Airflow, Spark, MinIO, Hive Metastore, Trino, dbt를 통합한 ETL/ELT 워크플로우를 제공합니다. 주요 사용 사례는 국내 주식(KRX), 해외 주식(Alpha Vantage), 가상자산(CoinGecko) 시세 데이터를 수집하고 처리하는 분산 데이터 파이프라인입니다.

## Architecture

**Orchestration Layer (Airflow)**
- CeleryExecutor with Redis and PostgreSQL로 워크플로우 오케스트레이션 관리
- DAG 위치: `airflow/dags/`, 공통 유틸리티: `airflow/dags/common/`
- Spark 기반 DAG: `airflow/dags/spark_jobs/`
- Airflow API 서버: port 8080

**Processing Layer (Spark)**
- Spark 클러스터: master 1대 + worker 2대 (각 2GB 메모리, 1 코어)
- Master UI: port 8088, Worker UI: ports 8089-8090
- Spark job 위치: `spark/jobs/` → 컨테이너 내 `/opt/spark-apps`로 마운트
- 커스텀 이미지: `apache/spark:3.5.0` + Python 패키지 (requests, pandas)

**Storage & Catalog Layer**
- **MinIO**: S3 호환 객체 스토리지 (API: port 9000, Console: port 9001)
- **Hive Metastore**: PostgreSQL 기반 메타데이터 저장소 (port 9083)
  - Trino와 Spark가 공유하는 테이블 스키마 정보 관리
  - MinIO(S3) 연동 설정 포함
- **로컬 볼륨**: Spark 출력 데이터 저장 (`spark/output/`)
  - 국내주식: `spark/output/krx/YYYYMMDD/`
  - 해외주식: `spark/output/foreign_stock/YYYYMMDD/`
  - 가상자산: `spark/output/crypto/YYYYMMDD/`

**Query Layer**
- **Trino**: 분산 SQL 쿼리 엔진 (port 8081)
  - Hive Metastore 연동 (`thrift://hive-metastore:9083`)
  - MinIO S3 연동 설정 (`trino/catalog/hive.properties`)
- **dbt**: Trino 기반 데이터 변환 도구

**Key Integration Points**
- Airflow → Spark: `SparkSubmitOperator`로 `spark://spark-master:7077` 연결
- Spark → Storage: Parquet 파일을 로컬 볼륨에 저장
- Spark/Trino → Hive Metastore: 테이블 메타데이터 공유
- Hive Metastore → MinIO: S3 스타일 접근으로 객체 스토리지 연동

## Environment Setup

**Prerequisites**
- Docker and Docker Compose
- `.env` 파일 생성 (프로젝트 루트):
  - `AIRFLOW__CORE__FERNET_KEY`
  - `AIRFLOW__API_AUTH__JWT_SECRET`
  - `GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY` (KRX API용)
  - `ALPHA_VANTAGE_API_KEY` (Alpha Vantage API용)
  - Optional: `AIRFLOW_UID` (default: 50000)

**Starting Services**
```bash
# 모든 서비스 시작
docker-compose up -d

# Flower 모니터링 포함
docker-compose --profile flower up -d

# 로그 확인
docker-compose logs -f [service-name]
```

**Accessing Services**
- Airflow UI: http://localhost:8080 (user: airflow, pass: airflow)
- Spark Master UI: http://localhost:8088
- Trino UI: http://localhost:8081
- MinIO Console: http://localhost:9001 (user: minio, pass: minio123)

**Stopping Services**
```bash
docker-compose down
# 볼륨 정리 포함
docker-compose down -v
```

**Airflow 초기 설정**
```bash
# Spark 연결 설정
docker exec -it <airflow-container> airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077'

# KRX API 키 설정 (Airflow Variable)
docker exec -it <airflow-container> airflow variables set \
    GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY "your_api_key_here"
```

## Data Sources

### 1. 국내 주식 (KRX)
- **API**: 금융위원회 주식시세정보 API
- **인증**: 환경변수 `GOV_OPEN_API_STOCK_PRICE_SERVICE_KEY` 필요
- **수집 주기**: 월~금 18:00 KST (장 마감 후)
- **데이터 지연**: 1-2일 (API 특성상)
- **Job**: `spark/jobs/krx_api_ingest.py`
- **DAG**: `krx_stock_ingest` (airflow/dags/spark_jobs/krx_stock_ingest_dag.py)

### 2. 해외 주식 (Alpha Vantage)
- **API**: Alpha Vantage TIME_SERIES_DAILY API
- **인증**: 환경변수 `ALPHA_VANTAGE_API_KEY` 필요 (무료 티어 사용 가능)
- **수집 종목**: 17개 종목
  - 미국 주요 기술주: AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, AMD, TSM
  - 보유 주식: DNA, SOFI
  - 관심 주식: LEU, ONDS, HIMS
  - 지수 ETF: SPY, QQQ
  - 기타: NFLX, CRM
- **수집 주기**: 월~금 09:00 KST (미국 장 마감 후)
- **Rate Limit**: 무료 티어 - 분당 25회, 하루 500회 (요청 간 2.5초 대기)
- **Job**: `spark/jobs/foreign_stock_ingest.py`
- **DAG**: `foreign_stock_ingest` (airflow/dags/spark_jobs/foreign_stock_ingest_dag.py)

### 3. 가상자산 (CoinGecko)
- **API**: CoinGecko API v3
- **인증**: 불필요 (무료, Rate Limit: 분당 10-50회)
- **수집 코인**: 주요 가상자산 24개 (BTC, ETH, BNB, XRP 등)
- **수집 주기**: 6시간마다 (00:00, 06:00, 12:00, 18:00 KST)
- **Rate Limit 대응**: 배치당 6초 대기
- **Job**: `spark/jobs/crypto_ingest.py`
- **DAG**: `crypto_ingest` (airflow/dags/spark_jobs/crypto_ingest_dag.py)

## DAG Patterns

**Basic ETL DAG (PythonOperator)**
Extract → Transform → Load 패턴:
- `etl_sales_dag.py` - 매출 데이터 파이프라인 (tag: 'sales')
- `etl_marketing_dag.py` - 마케팅 데이터 파이프라인 (tag: 'marketing')
- `data_quality_check.py` - 데이터 품질 점검 (tag: 'quality')

**Spark Job Submission DAG**
`SparkSubmitOperator` 사용 패턴:
- `krx_stock_ingest` - 국내 주식 수집 (월~금 18:00)
- `foreign_stock_ingest` - 해외 주식 수집 (월~금 09:00)
- `crypto_ingest` - 가상자산 수집 (6시간마다)

필수 설정:
- `conn_id="spark_default"` (Airflow Connections에서 설정)
- `conf={"spark.master": "spark://spark-master:7077"}`
- `application="/opt/spark-apps/jobs/[job_file].py"`
- `driver_memory="1g"`, `executor_memory="2g"`, `executor_cores=1`

**Timezone Handling**
`pendulum.timezone("Asia/Seoul")`로 KST 타임존 설정

## Spark Job Development

**Job Location**
`spark/jobs/` 디렉토리에 작성 → 컨테이너 내 `/opt/spark-apps/jobs/`로 마운트

**Common Pattern (3개 job 공통)**
1. 외부 API에서 데이터 수집
2. Pandas DataFrame → Spark DataFrame 변환
3. `collected_at`, `date` 등 메타데이터 컬럼 추가
4. 동적 파티셔닝 (target: ~128MB per partition)
5. Parquet 형식으로 저장

**Job Details**

*krx_api_ingest.py*
- 금융위원회 API 호출 (requests)
- 날짜 오프셋: `datetime.today() - timedelta(days=2)`
- 출력: `/opt/spark-apps/output/krx/{YYYYMMDD}/`

*foreign_stock_ingest.py*
- Alpha Vantage TIME_SERIES_DAILY API 사용 (requests)
- 티커 리스트: 17개 종목 (AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, AMD, TSM, DNA, SOFI, LEU, ONDS, HIMS, SPY, QQQ, NFLX, CRM)
- API 응답에서 가장 최근 날짜 데이터 추출 (outputsize=compact, 최근 100개 데이터포인트)
- Rate Limit 대응: 요청 간 2.5초 대기 (분당 24회로 제한)
- 출력: `/opt/spark-apps/output/foreign_stock/{YYYYMMDD}/`

*crypto_ingest.py*
- CoinGecko API v3 (requests)
- 배치 처리 (10개씩) + Rate Limit 대응 (6초 대기)
- 시가총액, 거래량, 24시간 변동률 등 수집
- 출력: `/opt/spark-apps/output/crypto/{YYYYMMDD}/`

**Running Jobs Manually**
```bash
# 국내 주식
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/jobs/krx_api_ingest.py

# 해외 주식
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/jobs/foreign_stock_ingest.py

# 가상자산
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/jobs/crypto_ingest.py
```

**Adding New Dependencies**
`Dockerfile.spark` 수정 후 이미지 재빌드:
```bash
# Dockerfile.spark에 패키지 추가
RUN pip3 install requests pandas [new-package]

# 재빌드
docker-compose build spark-master spark-worker-1 spark-worker-2
docker-compose up -d
```

## Data Flow

1. **Ingestion**: Spark job이 외부 API에서 데이터 수집 (KRX, Alpha Vantage, CoinGecko)
2. **Storage**: Raw 데이터를 Parquet 형식으로 로컬 볼륨에 저장
3. **Cataloging**: Hive Metastore가 테이블 스키마 메타데이터 관리
4. **Querying**: Trino가 Hive Metastore를 통해 데이터에 SQL 인터페이스 제공
5. **Transformation**: dbt 모델이 비즈니스 로직 적용
6. **Orchestration**: Airflow가 전체 워크플로우 스케줄링 및 모니터링

## Configuration Files

**Airflow**
- Custom config: `airflow/config/airflow.cfg`
- Logs: `airflow/logs/`
- Plugins: `airflow/plugins/`

**Spark**
- Dockerfile: `Dockerfile.spark`
- Jobs: `spark/jobs/*.py`
- Output: `spark/output/*/YYYYMMDD/`

**Trino**
- Catalog: `trino/catalog/hive.properties`
  - Hive Metastore URI: `thrift://hive-metastore:9083`
  - MinIO S3 설정 포함 (endpoint, credentials, path-style-access)
- JVM config: `trino/jvm.config` (heap 2GB)

**Hive Metastore**
- DB: PostgreSQL (hive-metastore-db)
- Service: Apache Hive 4.0.0 (metastore only)
- Port: 9083 (Thrift)

**dbt**
- Project root: `dbt/` (컨테이너 내 `/usr/app`)

## Port Reference

| Service | Port | Purpose |
|---------|------|---------|
| Airflow API | 8080 | Web UI / API |
| Spark Master | 8088 | Web UI |
| Spark Master | 7077 | Job submission |
| Spark Worker 1 | 8089 | Web UI |
| Spark Worker 2 | 8090 | Web UI |
| Trino | 8081 | Web UI & SQL interface |
| MinIO API | 9000 | S3 API |
| MinIO Console | 9001 | Web UI |
| Hive Metastore | 9083 | Thrift service |
| Flower | 5555 | Celery monitoring (optional) |

## DAG Schedule Summary

| DAG | 스케줄 | 실행 시간 | 설명 |
|-----|-------|----------|------|
| `krx_stock_ingest` | 월~금 | 18:00 KST | 국내 주식 (장 마감 후) |
| `foreign_stock_ingest` | 월~금 | 09:00 KST | 해외 주식 (미국 장 마감 후) |
| `crypto_ingest` | 매일 | 00:00, 06:00, 12:00, 18:00 KST | 가상자산 (6시간마다) |
| `etl_sales` | 매일 | @daily | 매출 데이터 ETL |
| `etl_marketing` | 매일 | @daily | 마케팅 데이터 ETL |
| `data_quality_check` | 매일 | @daily | 데이터 품질 점검 |

모든 DAG: `catchup=False` (과거 실행 스킵)

## Common Issues

**Port Mapping**
- Airflow: 8080 (Web UI / API)
- Spark Master: 8088 (Web UI), 7077 (Job submission)
- Trino: 8081 (Web UI & SQL interface)
- MinIO: 9000 (API), 9001 (Console)

**Data Availability**
- KRX API: 1-2일 지연 (`timedelta(days=2)` 사용)
- 해외 주식: Alpha Vantage는 실시간이 아닌 일봉 데이터 (최근 100개 데이터포인트)
- 가상자산: CoinGecko API Rate Limit 주의 (무료: 분당 10-50회)

**Volume Permissions**
- Airflow 볼륨 권한: `chown -R ${AIRFLOW_UID}:0 ./airflow/`
- Spark 출력 디렉토리 권한 확인 필요

**Persistent Data Locations**
- `postgres-db-volume`: Airflow 메타데이터 (Docker volume)
- `./hive-metastore-db`: Hive Metastore 메타데이터
- `./minio`: MinIO 객체 스토리지 데이터
- `./spark/output`: Spark job 출력 (krx, foreign_stock, crypto)
- `./airflow/logs`: Airflow 실행 로그

**API Limitations**
- Alpha Vantage: 무료 티어 - 분당 25회, 하루 500회 제한 (요청 간 2.5초 대기로 대응)
- CoinGecko: 무료 API는 Rate Limit 존재 (배치 처리로 대응)
- KRX API: 서비스 키 발급 필요, 일일 요청 제한 확인
