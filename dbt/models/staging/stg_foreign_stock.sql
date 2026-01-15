-- 해외 주식 스테이징 모델
-- Yahoo Finance API로 수집한 원천 데이터를 정제

{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select
        *
    from {{ source('raw', 'foreign_stock_raw') }}
)

select
    -- 종목 정보
    cast(ticker as varchar) as ticker,
    cast(company_name as varchar) as company_name,
    cast(exchange as varchar) as exchange,
    cast(currency as varchar) as currency,

    -- 가격 정보
    cast(open as double) as open_price,
    cast(high as double) as high_price,
    cast(low as double) as low_price,
    cast(close as double) as close_price,

    -- 거래 정보
    cast(volume as bigint) as trading_volume,

    -- 날짜 정보
    cast(date as varchar) as trade_date,
    collected_at as loaded_at

from source_data
where date is not null
