WITH dim_market AS (
    SELECT
        {{ generate_surrogate_key(['coin_id', 'last_updated']) }} AS market_snapshot_sk,
        coin_id,
        last_updated,
        market_cap,
        market_cap_rank,
        market_cap_change_24h,
        market_cap_change_pct_24h,
        volume_24h
    FROM {{ ref("silver_crypto_prices_clean") }}
)

SELECT * FROM dim_market