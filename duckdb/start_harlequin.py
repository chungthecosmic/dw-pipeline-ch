#!/usr/bin/env python3
"""
Harlequin 시작 스크립트 - S3 Iceberg 테이블 연결 설정

사용법:
    python start_harlequin.py
"""

import subprocess
import boto3


def get_init_script():
    """DuckDB 초기화 SQL 생성"""
    session = boto3.Session(profile_name='dw-pipeline')
    credentials = session.get_credentials()

    init_sql = f"""
-- AWS S3 설정
INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;

SET s3_access_key_id='{credentials.access_key}';
SET s3_secret_access_key='{credentials.secret_key}';
SET s3_region='ap-northeast-2';

-- Iceberg 테이블 뷰 생성
CREATE OR REPLACE VIEW krx_stock AS
SELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/krx_stock_price');

CREATE OR REPLACE VIEW foreign_stock AS
SELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/foreign_stock_price');

CREATE OR REPLACE VIEW crypto AS
SELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/crypto_price');

-- 환영 메시지
SELECT '=== DuckDB + Harlequin ===' as message
UNION ALL
SELECT '사용 가능한 뷰:'
UNION ALL
SELECT '  - krx_stock (국내 주식)'
UNION ALL
SELECT '  - foreign_stock (해외 주식)'
UNION ALL
SELECT '  - crypto (가상자산)';
"""
    return init_sql


def main():
    # 초기화 SQL 파일 생성
    init_sql = get_init_script()
    with open('/tmp/init.sql', 'w') as f:
        f.write(init_sql)

    print("Harlequin 시작 중...")
    print("Ctrl+Q로 종료")
    print()

    # Harlequin 실행 (초기화 스크립트 포함)
    subprocess.run([
        'harlequin',
        '--init-path', '/tmp/init.sql'
    ])


if __name__ == "__main__":
    main()
