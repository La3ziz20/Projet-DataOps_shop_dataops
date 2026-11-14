-- Select EUR -> USD exchange rates from raw_fx_rates and cast types explicitly
SELECT
    CAST(rate_date AS DATE) AS rate_date,
    CAST(base AS VARCHAR) AS base,
    CAST(currency AS VARCHAR) AS currency,
    CAST(rate AS DOUBLE) AS rate
FROM {{ source('raw', 'raw_fx_rates') }}
WHERE base = 'EUR'
  AND currency = 'USD'
