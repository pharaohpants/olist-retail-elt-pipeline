TRUNCATE TABLE dwh.fact_sales;

INSERT INTO dwh.fact_sales (
    order_id, order_item_id, order_status,
    product_key, seller_key, customer_key,
    purchase_date, price, freight_value, quantity
)
SELECT
    oi.order_id,
    oi.order_item_id,
    o.order_status,
    dp.product_key,
    ds.seller_key,
    dc.customer_key,
    o.order_purchase_timestamp::date AS purchase_date,
    oi.price,
    oi.freight_value,
    1 AS quantity
FROM staging.order_items oi
JOIN staging.orders o ON o.order_id = oi.order_id
JOIN dwh.dim_product dp
  ON dp.product_id = oi.product_id
 AND dp.is_current = TRUE
JOIN dwh.dim_seller ds
  ON ds.seller_id = oi.seller_id
 AND ds.is_current = TRUE
JOIN dwh.dim_customer dc
  ON dc.customer_id = o.customer_id
 AND dc.is_current = TRUE
JOIN dwh.dim_date dd
  ON dd.date_key = o.order_purchase_timestamp::date;
