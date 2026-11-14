-- Q1: Total revenue per country in EUR
SELECT
    country,
    ROUND(SUM(revenue_eur), 2) AS total_revenue_eur
FROM {{ ref('mart_revenue_daily') }}
GROUP BY country
ORDER BY total_revenue_eur DESC;

-- Q2: Best day by revenue in USD
SELECT
    order_date,
    ROUND(SUM(revenue_usd), 2) AS total_revenue_usd
FROM {{ ref('mart_revenue_daily') }}
GROUP BY order_date
ORDER BY total_revenue_usd DESC NULLS LAST
LIMIT 1;

-- Q3: Share of total revenue per country, in percent
SELECT
    country,
    ROUND(SUM(revenue_eur), 2) AS revenue_eur,
    ROUND(100.0 * SUM(revenue_eur) / SUM(SUM(revenue_eur)) OVER (), 2) AS share_pct
FROM {{ ref('mart_revenue_daily') }}
GROUP BY country
ORDER BY share_pct DESC;
