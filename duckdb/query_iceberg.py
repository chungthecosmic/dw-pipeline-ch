#!/usr/bin/env python3
"""
DuckDB를 사용한 Iceberg 테이블 조회 스크립트

사용법:
    python query_iceberg.py                    # 전체 테이블 조회
    python query_iceberg.py krx                # KRX 데이터만 조회
    python query_iceberg.py foreign_stock      # 해외 주식만 조회
    python query_iceberg.py crypto             # 가상자산만 조회
    python query_iceberg.py exchange_rate      # 환율만 조회
    python query_iceberg.py market_index       # 주식 지수만 조회
"""

import sys
import duckdb
import boto3
from botocore.config import Config


def get_aws_credentials():
    """AWS credentials 가져오기"""
    session = boto3.Session(profile_name='dw-pipeline')
    credentials = session.get_credentials()
    return {
        'access_key': credentials.access_key,
        'secret_key': credentials.secret_key,
        'region': 'ap-northeast-2'
    }


def setup_duckdb():
    """DuckDB 설정 및 S3 연결"""
    conn = duckdb.connect()

    # 확장 기능 설치 및 로드
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute("INSTALL iceberg;")
    conn.execute("LOAD iceberg;")

    # AWS credentials 설정
    creds = get_aws_credentials()
    conn.execute(f"SET s3_access_key_id='{creds['access_key']}';")
    conn.execute(f"SET s3_secret_access_key='{creds['secret_key']}';")
    conn.execute(f"SET s3_region='{creds['region']}';")

    return conn


def query_krx(conn):
    """KRX 국내 주식 데이터 조회"""
    print("\n" + "="*70)
    print("KRX 국내 주식 데이터")
    print("="*70)

    try:
        result = conn.execute("""
            SELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/krx_stock_price')
            LIMIT 20
        """).fetchdf()
        print(result.to_string(index=False))
        print(f"\n총 {len(result)}건 조회됨")
    except Exception as e:
        print(f"조회 실패: {e}")


def query_foreign_stock(conn):
    """해외 주식 데이터 조회"""
    print("\n" + "="*70)
    print("해외 주식 데이터")
    print("="*70)

    try:
        result = conn.execute("""
            SELECT ticker, date, open, high, low, close, volume
            FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/foreign_stock_price')
            ORDER BY date DESC, ticker
            LIMIT 20
        """).fetchdf()
        print(result.to_string(index=False))
        print(f"\n총 {len(result)}건 조회됨")
    except Exception as e:
        print(f"조회 실패: {e}")


def query_crypto(conn):
    """가상자산 데이터 조회"""
    print("\n" + "="*70)
    print("가상자산 데이터")
    print("="*70)

    try:
        result = conn.execute("""
            SELECT symbol, name, current_price_usd, market_cap_rank, price_change_percentage_24h
            FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/crypto_price')
            ORDER BY market_cap_rank
            LIMIT 20
        """).fetchdf()
        print(result.to_string(index=False))
        print(f"\n총 {len(result)}건 조회됨")
    except Exception as e:
        print(f"조회 실패: {e}")


def query_exchange_rate(conn):
    """환율 데이터 조회"""
    print("\n" + "="*70)
    print("환율 데이터 (KRW 기준)")
    print("="*70)

    try:
        result = conn.execute("""
            SELECT currency_code, currency_name, country,
                   ROUND(rate, 2) as rate_krw,
                   date
            FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/exchange_rate')
            ORDER BY date DESC, rate_krw DESC
            LIMIT 20
        """).fetchdf()
        print(result.to_string(index=False))
        print(f"\n총 {len(result)}건 조회됨")
    except Exception as e:
        print(f"조회 실패: {e}")


def query_market_index(conn):
    """주식 지수 데이터 조회"""
    print("\n" + "="*70)
    print("주식 지수 데이터")
    print("="*70)

    try:
        result = conn.execute("""
            SELECT index_name, ticker, date,
                   ROUND(close, 2) as close,
                   ROUND(change, 2) as change,
                   ROUND(change_percent, 2) as change_pct,
                   volume
            FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/market_index')
            ORDER BY date DESC, index_name
            LIMIT 20
        """).fetchdf()
        print(result.to_string(index=False))
        print(f"\n총 {len(result)}건 조회됨")
    except Exception as e:
        print(f"조회 실패: {e}")


def main():
    conn = setup_duckdb()

    # 명령행 인수 처리
    if len(sys.argv) > 1:
        table = sys.argv[1].lower()
        if table == 'krx':
            query_krx(conn)
        elif table == 'foreign_stock':
            query_foreign_stock(conn)
        elif table == 'crypto':
            query_crypto(conn)
        elif table == 'exchange_rate':
            query_exchange_rate(conn)
        elif table == 'market_index':
            query_market_index(conn)
        else:
            print(f"알 수 없는 테이블: {table}")
            print("사용 가능한 테이블: krx, foreign_stock, crypto, exchange_rate, market_index")
    else:
        # 전체 조회
        query_krx(conn)
        query_foreign_stock(conn)
        query_crypto(conn)
        query_exchange_rate(conn)
        query_market_index(conn)

    conn.close()


if __name__ == "__main__":
    main()
