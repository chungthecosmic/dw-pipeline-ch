# dbt 프로젝트: dw_pipeline

데이터 웨어하우스 파이프라인의 dbt 변환 레이어

## 프로젝트 구조

```
dbt/
├── dbt_project.yml      # 프로젝트 설정
├── profiles.yml         # Trino 연결 설정
├── models/
│   ├── staging/         # 원천 데이터 정제
│   │   ├── stg_krx_stock.sql
│   │   ├── stg_foreign_stock.sql
│   │   ├── stg_crypto.sql
│   │   └── sources.yml
│   └── mart/            # 비즈니스 로직 적용
│       ├── mart_stock_daily.sql
│       ├── mart_crypto_daily.sql
│       └── schema.yml
├── macros/              # 재사용 가능한 SQL 함수
└── tests/               # 데이터 품질 테스트
```

## 데이터 플로우

1. **Staging Layer** (View)
   - `stg_krx_stock`: 국내 주식 데이터 정제
   - `stg_foreign_stock`: 해외 주식 데이터 정제
   - `stg_crypto`: 가상자산 데이터 정제

2. **Mart Layer** (Table)
   - `mart_stock_daily`: 국내+해외 주식 통합 일별 데이터
   - `mart_crypto_daily`: 가상자산 일별 시장 데이터

## dbt 실행 방법

### 별도 dbt 컨테이너에서 실행 (개발용)

```bash
# dbt 컨테이너 접속
docker exec -it dbt bash

# 모든 모델 실행
dbt run

# 특정 모델만 실행
dbt run --select mart_stock_daily

# 테스트 실행
dbt test

# 문서 생성
dbt docs generate
dbt docs serve
```

### Airflow에서 실행 (프로덕션)

Airflow DAG `dbt_transform`가 자동으로 실행합니다.

## 주요 명령어

```bash
# 모델 실행
dbt run                          # 모든 모델
dbt run --select staging.*       # staging 모델만
dbt run --select mart.*          # mart 모델만

# 테스트
dbt test                         # 모든 테스트
dbt test --select mart_stock_daily  # 특정 모델 테스트

# 의존성 확인
dbt deps

# 프로젝트 검증
dbt debug
dbt compile
```

## Trino 연결 정보

- **Host**: trino
- **Port**: 8080
- **Database**: hive
- **Schema**: default (raw), staging, mart
- **User**: trino

## 참고사항

- Staging 모델은 View로 생성 (실시간 데이터 반영)
- Mart 모델은 Table로 생성 (쿼리 성능 최적화)
- Trino의 Hive Metastore를 통해 데이터 접근
