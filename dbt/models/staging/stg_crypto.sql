-- 가상자산 스테이징 모델
-- CoinGecko API로 수집한 원천 데이터를 정제

{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select
        *
    from {{ source('raw', 'crypto_raw') }}
)

select
    -- 코인 정보
    cast(coin_id as varchar) as coin_id,
    cast(symbol as varchar) as symbol,
    cast(name as varchar) as coin_name,
    cast(market_cap_rank as integer) as market_cap_rank,

    -- 가격 정보 (USD 기준)
    cast(current_price_usd as double) as current_price_usd,
    cast(high_24h_usd as double) as high_24h_usd,
    cast(low_24h_usd as double) as low_24h_usd,
    cast(ath_usd as double) as ath_usd,
    cast(atl_usd as double) as atl_usd,

    -- 변동 정보
    cast(price_change_24h_usd as double) as price_change_24h_usd,
    cast(price_change_percentage_24h as double) as price_change_percentage_24h,

    -- 시장 정보
    cast(market_cap_usd as double) as market_cap_usd,
    cast(total_volume_24h_usd as double) as total_volume_24h_usd,

    -- 공급량 정보
    cast(circulating_supply as double) as circulating_supply,
    cast(total_supply as double) as total_supply,

    -- 날짜 정보
    cast(date as varchar) as snapshot_date,
    cast(last_updated as timestamp) as last_updated,
    collected_at as loaded_at

from source_data
where coin_id is not null
