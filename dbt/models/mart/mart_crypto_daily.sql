-- 가상자산 일별 마트
-- CoinGecko 데이터를 기반으로 일별 가상자산 시장 현황 제공

{{ config(
    materialized='table',
    schema='mart'
) }}

with crypto_data as (
    select
        coin_id,
        symbol,
        coin_name,
        market_cap_rank,
        current_price_usd,
        high_24h_usd,
        low_24h_usd,
        price_change_24h_usd,
        price_change_percentage_24h,
        market_cap_usd,
        total_volume_24h_usd,
        circulating_supply,
        total_supply,
        ath_usd,
        atl_usd,
        snapshot_date,
        loaded_at
    from {{ ref('stg_crypto') }}
)

select
    coin_id,
    symbol,
    coin_name,
    market_cap_rank,
    snapshot_date,

    -- 가격 정보
    current_price_usd,
    high_24h_usd,
    low_24h_usd,
    price_change_24h_usd,
    price_change_percentage_24h,

    -- 시장 정보
    market_cap_usd,
    total_volume_24h_usd,

    -- 공급량 정보
    circulating_supply,
    total_supply,
    case
        when total_supply > 0
        then (circulating_supply / total_supply) * 100
        else null
    end as circulating_supply_percentage,

    -- 역사적 가격
    ath_usd,
    atl_usd,
    case
        when ath_usd > 0
        then ((current_price_usd - ath_usd) / ath_usd) * 100
        else null
    end as ath_change_percentage,

    -- 거래 비율
    case
        when market_cap_usd > 0
        then (total_volume_24h_usd / market_cap_usd) * 100
        else 0
    end as volume_to_marketcap_ratio,

    -- 메타데이터
    loaded_at,
    current_timestamp as transformed_at

from crypto_data
order by snapshot_date desc, market_cap_rank
