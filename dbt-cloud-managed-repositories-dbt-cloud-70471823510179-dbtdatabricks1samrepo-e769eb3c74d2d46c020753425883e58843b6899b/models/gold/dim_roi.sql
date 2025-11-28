WITH dim_roi AS (
        SELECT
        {{ generate_surrogate_key(['coin_id', 'roi_times', 'roi_pct']) }} AS roi_sk,
        coin_id,
        roi_times,
        roi_pct,
        roi_currency
    FROM {{ ref("silver_crypto_prices_clean") }}
)

SELECT * FROM dim_roi
WHERE roi_times IS NOT NULL

