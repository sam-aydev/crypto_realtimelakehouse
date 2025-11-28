WITH dim_price_extremes AS (
    SELECT
        {{ generate_surrogate_key(['coin_id', 'last_updated']) }} AS price_extremes_sk,
        coin_id,
        last_updated,
        high_24h,
        low_24h,
        ath,
        ath_change_percentage,
        ath_date,
        atl,
        atl_change_percentage,
        atl_date
    FROM {{ ref("silver_crypto_prices_clean") }}
)

SELECT * FROM dim_price_extremes