"""
테이블 관련 API 라우터
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

from ..database import get_db, TABLES_INFO, get_table_path

router = APIRouter()


class TableInfo(BaseModel):
    id: str
    name: str
    description: str
    partition: str
    source: str


class TableSchema(BaseModel):
    column_name: str
    data_type: str


class TableStats(BaseModel):
    record_count: int
    min_date: Optional[str]
    max_date: Optional[str]
    partition_count: int


class QueryRequest(BaseModel):
    sql: str


class QueryResult(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    row_count: int


@router.get("", response_model=List[TableInfo])
async def list_tables():
    """테이블 목록 조회"""
    tables = []
    for table_id, info in TABLES_INFO.items():
        tables.append(TableInfo(
            id=table_id,
            name=info["name"],
            description=info["description"],
            partition=info["partition"],
            source=info["source"],
        ))
    return tables


@router.get("/{table_id}", response_model=TableInfo)
async def get_table(table_id: str):
    """특정 테이블 정보 조회"""
    if table_id not in TABLES_INFO:
        raise HTTPException(status_code=404, detail="테이블을 찾을 수 없습니다")

    info = TABLES_INFO[table_id]
    return TableInfo(
        id=table_id,
        name=info["name"],
        description=info["description"],
        partition=info["partition"],
        source=info["source"],
    )


@router.get("/{table_id}/schema", response_model=List[TableSchema])
async def get_table_schema(table_id: str):
    """테이블 스키마 조회"""
    if table_id not in TABLES_INFO:
        raise HTTPException(status_code=404, detail="테이블을 찾을 수 없습니다")

    try:
        with get_db() as conn:
            path = get_table_path(table_id)
            df = conn.execute(f"""
                SELECT * FROM iceberg_scan('{path}') LIMIT 1
            """).fetchdf()

            schema = []
            for col in df.columns:
                schema.append(TableSchema(
                    column_name=col,
                    data_type=str(df[col].dtype),
                ))
            return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스키마 조회 실패: {str(e)}")


@router.get("/{table_id}/stats", response_model=TableStats)
async def get_table_stats(table_id: str):
    """테이블 통계 조회"""
    if table_id not in TABLES_INFO:
        raise HTTPException(status_code=404, detail="테이블을 찾을 수 없습니다")

    try:
        with get_db() as conn:
            path = get_table_path(table_id)
            partition_col = TABLES_INFO[table_id]["partition"]

            # 레코드 수
            count_df = conn.execute(f"""
                SELECT COUNT(*) as cnt FROM iceberg_scan('{path}')
            """).fetchdf()
            record_count = int(count_df['cnt'].iloc[0])

            # 날짜 범위
            date_df = conn.execute(f"""
                SELECT
                    MIN({partition_col}) as min_date,
                    MAX({partition_col}) as max_date,
                    COUNT(DISTINCT {partition_col}) as partition_count
                FROM iceberg_scan('{path}')
            """).fetchdf()

            return TableStats(
                record_count=record_count,
                min_date=str(date_df['min_date'].iloc[0]) if date_df['min_date'].iloc[0] else None,
                max_date=str(date_df['max_date'].iloc[0]) if date_df['max_date'].iloc[0] else None,
                partition_count=int(date_df['partition_count'].iloc[0]),
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"통계 조회 실패: {str(e)}")


@router.get("/{table_id}/data")
async def get_table_data(
    table_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """테이블 데이터 조회"""
    if table_id not in TABLES_INFO:
        raise HTTPException(status_code=404, detail="테이블을 찾을 수 없습니다")

    try:
        with get_db() as conn:
            path = get_table_path(table_id)
            partition_col = TABLES_INFO[table_id]["partition"]

            df = conn.execute(f"""
                SELECT * FROM iceberg_scan('{path}')
                ORDER BY {partition_col} DESC
                LIMIT {limit} OFFSET {offset}
            """).fetchdf()

            return {
                "columns": df.columns.tolist(),
                "rows": df.values.tolist(),
                "row_count": len(df),
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 실패: {str(e)}")


@router.post("/query", response_model=QueryResult)
async def execute_query(request: QueryRequest):
    """SQL 쿼리 실행"""
    # 간단한 보안 체크 (SELECT만 허용)
    sql_lower = request.sql.strip().lower()
    if not sql_lower.startswith("select"):
        raise HTTPException(status_code=400, detail="SELECT 쿼리만 실행 가능합니다")

    # 위험한 키워드 체크
    dangerous_keywords = ["drop", "delete", "update", "insert", "alter", "create", "truncate"]
    for keyword in dangerous_keywords:
        if keyword in sql_lower:
            raise HTTPException(status_code=400, detail=f"'{keyword}' 키워드는 사용할 수 없습니다")

    try:
        with get_db() as conn:
            df = conn.execute(request.sql).fetchdf()

            # 결과 크기 제한
            if len(df) > 10000:
                df = df.head(10000)

            return QueryResult(
                columns=df.columns.tolist(),
                rows=df.values.tolist(),
                row_count=len(df),
            )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"쿼리 실행 실패: {str(e)}")
