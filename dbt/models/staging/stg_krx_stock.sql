-- 국내(KRX) 주식 스테이징 모델
-- 금융위원회 API로 수집한 원천 데이터를 정제

{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select
        *
    from {{ source('raw', 'krx_stock_raw') }}
)

select
    -- 종목 정보
    cast(srtncd as varchar) as stock_code,        -- 단축코드
    cast(isincdn as varchar) as isin_code,        -- ISIN 코드
    cast(itmsNm as varchar) as stock_name,        -- 종목명
    cast(mrktCtg as varchar) as market_category,  -- 시장구분

    -- 가격 정보
    cast(clpr as double) as close_price,          -- 종가
    cast(vs as double) as price_change,           -- 대비
    cast(fltRt as double) as change_rate,         -- 등락률
    cast(mkp as double) as market_price,          -- 시가
    cast(hipr as double) as high_price,           -- 고가
    cast(lopr as double) as low_price,            -- 저가

    -- 거래 정보
    cast(trqu as bigint) as trading_volume,       -- 거래량
    cast(trPrc as bigint) as trading_value,       -- 거래대금
    cast(lstgStCnt as bigint) as listed_shares,   -- 상장주식수
    cast(mrktTotAmt as bigint) as market_cap,     -- 시가총액

    -- 날짜 정보
    cast(basDt as varchar) as base_date,          -- 기준일자
    current_timestamp as loaded_at

from source_data
where basDt is not null
