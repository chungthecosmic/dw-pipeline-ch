-- 주식 일별 통합 마트
-- 국내 주식(KRX)과 해외 주식(Yahoo Finance)을 통합하여 일별 주식 데이터 제공

{{ config(
    materialized='table',
    schema='mart'
) }}

with krx_stock as (
    select
        stock_code as symbol,
        stock_name as name,
        'KRX' as market,
        'KRW' as currency,
        close_price,
        price_change,
        change_rate,
        market_price as open_price,
        high_price,
        low_price,
        trading_volume,
        market_cap,
        base_date as trade_date,
        loaded_at
    from {{ ref('stg_krx_stock') }}
),

foreign_stock as (
    select
        ticker as symbol,
        company_name as name,
        exchange as market,
        currency,
        close_price,
        close_price - open_price as price_change,
        case
            when open_price > 0
            then ((close_price - open_price) / open_price) * 100
            else 0
        end as change_rate,
        open_price,
        high_price,
        low_price,
        trading_volume,
        null as market_cap,
        trade_date,
        loaded_at
    from {{ ref('stg_foreign_stock') }}
),

combined as (
    select * from krx_stock
    union all
    select * from foreign_stock
)

select
    symbol,
    name,
    market,
    currency,
    trade_date,

    -- 가격 지표
    open_price,
    high_price,
    low_price,
    close_price,
    price_change,
    change_rate,

    -- 거래 지표
    trading_volume,
    market_cap,

    -- 메타데이터
    loaded_at,
    current_timestamp as transformed_at

from combined
order by trade_date desc, market, symbol
