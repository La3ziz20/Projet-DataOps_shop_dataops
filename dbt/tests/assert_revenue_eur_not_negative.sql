-- Singular test: fail if any row in mart_revenue_daily has negative revenue_eur
SELECT
    order_date,
    country,
    revenue_eur
FROM {{ ref('mart_revenue_daily') }}
WHERE revenue_eur < 0
