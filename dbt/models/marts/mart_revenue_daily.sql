-- Aggregate daily revenue per country in EUR and convert to USD via LEFT JOIN
WITH daily_orders AS (
    SELECT
        order_date,
        country,
        ROUND(SUM(revenue), 2) AS revenue_eur
    FROM {{ ref('stg_orders') }}
    GROUP BY order_date, country
)
SELECT
    o.order_date,
    o.country,
    o.revenue_eur,
    -- Keep LEFT JOIN so a missing FX rate shows as NULL instead of dropping the day's revenue
    ROUND(o.revenue_eur * fx.rate, 2) AS revenue_usd
FROM daily_orders o
LEFT JOIN {{ ref('stg_fx_rates') }} fx
    ON fx.rate_date = o.order_date
