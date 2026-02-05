"""
DuckDB 데이터베이스 연결 관리
"""

import duckdb
import boto3
from contextlib import contextmanager

# S3 버킷 및 테이블 정보
S3_BUCKET = "dw-pipeline-ch"
WAREHOUSE_PATH = "warehouse/stock_data"

TABLES_INFO = {
    "krx_stock_price": {
        "name": "국내 주식 (KRX)",
        "description": "금융위원회 API를 통해 수집한 국내 주식 시세 데이터",
        "partition": "basDt",
        "source": "금융위원회 주식시세정보 API",
    },
    "foreign_stock_price": {
        "name": "해외 주식",
        "description": "Alpha Vantage API를 통해 수집한 해외 주식 시세 데이터",
        "partition": "date",
        "source": "Alpha Vantage TIME_SERIES_DAILY",
    },
    "crypto_price": {
        "name": "가상자산",
        "description": "CoinGecko API를 통해 수집한 가상자산 시세 데이터",
        "partition": "date",
        "source": "CoinGecko API v3",
    },
    "exchange_rate": {
        "name": "환율",
        "description": "ExchangeRate-API를 통해 수집한 KRW 기준 환율 데이터",
        "partition": "date",
        "source": "ExchangeRate-API Open Access",
    },
    "market_index": {
        "name": "주식 지수",
        "description": "Alpha Vantage API를 통해 수집한 미국 주요 지수 데이터 (ETF 기반)",
        "partition": "date",
        "source": "Alpha Vantage TIME_SERIES_DAILY",
    },
}


def get_table_path(table_name: str) -> str:
    """테이블의 S3 경로 반환"""
    return f"s3://{S3_BUCKET}/{WAREHOUSE_PATH}/{table_name}"


def create_connection() -> duckdb.DuckDBPyConnection:
    """DuckDB 연결 생성"""
    conn = duckdb.connect()

    # 확장 기능 설치 및 로드
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute("INSTALL iceberg;")
    conn.execute("LOAD iceberg;")

    # AWS credentials 설정
    try:
        session = boto3.Session(profile_name='dw-pipeline')
        credentials = session.get_credentials()

        conn.execute(f"SET s3_access_key_id='{credentials.access_key}';")
        conn.execute(f"SET s3_secret_access_key='{credentials.secret_key}';")
        conn.execute(f"SET s3_region='ap-northeast-2';")
    except Exception as e:
        print(f"AWS credentials 설정 실패: {e}")

    return conn


@contextmanager
def get_db():
    """DuckDB 연결 컨텍스트 매니저"""
    conn = create_connection()
    try:
        yield conn
    finally:
        conn.close()
