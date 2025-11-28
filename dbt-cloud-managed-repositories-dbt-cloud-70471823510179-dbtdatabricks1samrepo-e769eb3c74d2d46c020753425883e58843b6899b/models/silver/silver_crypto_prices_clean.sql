{{ config(materialized="table") }}



WITH BASE AS (
    SELECT 
        id as coin_id,
        batch_id,
        CAST(ath AS decimal(18, 6)) as ath,
        CAST(ath_change_percentage AS decimal(10, 4)) as ath_change_percentage,
        CAST(ath_date AS timestamp) as ath_date,
        CAST(atl AS decimal(18,6)) as atl,
        CAST(atl_change_percentage AS decimal(18, 6)) as atl_change_percentage,
        CAST(atl_date AS timestamp) as atl_date,
        circulating_supply,
        CAST(current_price AS decimal(14, 4)) as current_price,
        CAST(fully_diluted_valuation AS bigint)  as fully_diluted_valuation,
        CAST(high_24h AS decimal(18,4)) as high_24h,
        CAST(last_updated AS timestamp) as last_updated,
        name                   as coin_name,
        symbol                 as symbol,
        image                  as image_url,
        cast(low_24h as decimal(18,6))                 as low_24h,
        cast(price_change_24h as decimal(18,6))        as price_change_24h,
        cast(price_change_percentage_24h as decimal(10,4)) as price_change_pct_24h,
        cast(market_cap as bigint)                     as market_cap,
        market_cap_change_24h,
        cast(market_cap_change_percentage_24h as decimal(10,4)) as market_cap_change_pct_24h,
        cast(market_cap_rank as int)                  as market_cap_rank,
        max_supply,
        total_supply,
        cast(total_volume as bigint)                  as volume_24h,
        from_json(roi, 'STRUCT<times DOUBLE, currency STRING, percentage DOUBLE>') as roi_struct,
        ingestion_timestamp
    FROM {{ ref("stg_bronze_crypto_prices") }}
), deduped AS (
    SELECT *, 
        CAST(roi_struct.times AS decimal(18,6)) as roi_times,
        CAST(roi_struct.percentage AS decimal(10,4)) as roi_pct,
        roi_struct.currency as roi_currency,
        row_number() over(partition by coin_id, last_updated order by ingestion_timestamp desc) as rank
    FROM BASE
)


SELECT * FROM deduped
WHERE rank = 1
