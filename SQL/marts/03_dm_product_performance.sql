CREATE OR REPLACE VIEW dwh.dm_product_performance AS
SELECT
    fs.product_key,
    COUNT(*) AS items_sold,
    COUNT(DISTINCT fs.order_id) AS orders_count,
    SUM(fs.price) AS product_revenue,
    SUM(fs.freight_value) AS freight_total,
    AVG(fs.price) AS avg_item_price
FROM dwh.fact_sales fs
GROUP BY fs.product_key;
