CREATE OR REPLACE VIEW dwh.dm_daily_sales AS
SELECT
    purchase_date AS date_key,
    COUNT(DISTINCT order_id) AS orders_count,
    SUM(price) AS gross_sales,
    SUM(freight_value) AS freight_total,
    SUM(price + freight_value) AS gmv
FROM dwh.fact_sales
GROUP BY purchase_date;
