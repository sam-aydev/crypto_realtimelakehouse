

WITH coin_dim AS (
    SELECT coin_sk, coin_id
    FROM {{ ref('dim_coin') }}
),

roi_dim AS (
    SELECT roi_sk, coin_id, roi_times, roi_pct
    FROM {{ ref('dim_roi') }}
),

market_dim AS (
    SELECT market_snapshot_sk, coin_id, last_updated
    FROM {{ ref('dim_market') }}
),

price_dim AS (
    SELECT price_extremes_sk, coin_id, last_updated
    FROM {{ ref('dim_price_extremes') }}
),

joined AS (
    SELECT
        {{ generate_surrogate_key(['s.coin_id', 's.last_updated']) }} AS price_fact_sk,

        -- FK links
        c.coin_sk,
        r.roi_sk,
        m.market_snapshot_sk,
        p.price_extremes_sk,

        -- Natural keys
        s.coin_id,
        s.last_updated,

        -- Measures
        s.current_price,
        s.price_change_24h,
        s.price_change_pct_24h,
        s.circulating_supply,
        s.fully_diluted_valuation
    FROM {{ ref("silver_crypto_prices_clean") }} s
    LEFT JOIN coin_dim c
        ON s.coin_id = c.coin_id
    LEFT JOIN roi_dim r
        ON s.coin_id = r.coin_id
        AND s.roi_times = r.roi_times
    LEFT JOIN market_dim m
        ON s.coin_id = m.coin_id
        AND s.last_updated = m.last_updated
    LEFT JOIN price_dim p
        ON s.coin_id = p.coin_id
        AND s.last_updated = p.last_updated
)

SELECT * FROM joined
