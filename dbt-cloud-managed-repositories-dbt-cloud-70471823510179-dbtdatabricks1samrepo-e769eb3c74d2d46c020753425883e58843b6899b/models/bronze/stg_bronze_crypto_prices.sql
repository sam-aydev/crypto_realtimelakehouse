
-- {{ config(materialized="table") }}
SELECT * FROM {{ source('crypto_bronze', 'crypto_prices_data') }}
