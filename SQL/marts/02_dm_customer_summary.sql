CREATE OR REPLACE VIEW dwh.dm_customer_summary AS
SELECT
    fs.customer_key,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    SUM(fs.price + fs.freight_value) AS total_spent,
    AVG(fs.price + fs.freight_value) AS avg_order_value,
    MIN(fs.purchase_date) AS first_purchase_date,
    MAX(fs.purchase_date) AS last_purchase_date
FROM dwh.fact_sales fs
GROUP BY fs.customer_key;
