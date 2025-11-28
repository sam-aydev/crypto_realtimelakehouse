{{ config(materialized="table") }}

WITH dim_coin AS (
    SELECT  
        {{ generate_surrogate_key(['coin_id']) }} AS coin_sk,
        coin_id,
        coin_name,
        symbol,
        image_url,
        max_supply,
        total_supply
    FROM {{ ref("silver_crypto_prices_clean") }}
)

SELECT * FROM dim_coin