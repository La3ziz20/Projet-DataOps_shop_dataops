-- Select from raw_orders, rename nothing unnecessarily, and cast types explicitly
SELECT
    CAST(order_id AS BIGINT) AS order_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(country AS VARCHAR) AS country,
    CAST(product AS VARCHAR) AS product,
    CAST(quantity AS BIGINT) AS quantity,
    CAST(unit_price AS DOUBLE) AS unit_price,
    CAST(revenue AS DOUBLE) AS revenue
FROM {{ source('raw', 'raw_orders') }}
