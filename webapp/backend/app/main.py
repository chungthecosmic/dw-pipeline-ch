"""
DW Pipeline 데이터 카탈로그 API
FastAPI 기반 백엔드
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import tables

app = FastAPI(
    title="DW Pipeline 데이터 카탈로그 API",
    description="Iceberg 테이블 스키마 및 데이터 조회 API",
    version="1.0.0",
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(tables.router, prefix="/api/tables", tags=["tables"])


@app.get("/")
async def root():
    return {"message": "DW Pipeline 데이터 카탈로그 API", "version": "1.0.0"}


@app.get("/api/health")
async def health_check():
    return {"status": "healthy"}
