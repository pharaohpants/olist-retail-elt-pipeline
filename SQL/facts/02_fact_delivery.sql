TRUNCATE TABLE dwh.fact_delivery;

INSERT INTO dwh.fact_delivery (
    order_id, order_status, customer_key, seller_key,
    purchase_date, approved_date, delivered_carrier_date,
    delivered_customer_date, estimated_delivery_date,
    delivery_lead_time_days, carrier_to_customer_days, delay_days, on_time_flag
)
SELECT
    o.order_id,
    o.order_status,
    dc.customer_key,
    ds.seller_key,
    o.order_purchase_timestamp::date AS purchase_date,
    o.order_approved_at::date AS approved_date,
    o.order_delivered_carrier_date::date AS delivered_carrier_date,
    o.order_delivered_customer_date::date AS delivered_customer_date,
    o.order_estimated_delivery_date::date AS estimated_delivery_date,
    CASE
        WHEN o.order_delivered_customer_date IS NULL THEN NULL
        ELSE (o.order_delivered_customer_date::date - o.order_purchase_timestamp::date)
    END AS delivery_lead_time_days,
    CASE
        WHEN o.order_delivered_customer_date IS NULL OR o.order_delivered_carrier_date IS NULL THEN NULL
        ELSE (o.order_delivered_customer_date::date - o.order_delivered_carrier_date::date)
    END AS carrier_to_customer_days,
    CASE
        WHEN o.order_delivered_customer_date IS NULL OR o.order_estimated_delivery_date IS NULL THEN NULL
        ELSE (o.order_delivered_customer_date::date - o.order_estimated_delivery_date::date)
    END AS delay_days,
    CASE
        WHEN o.order_delivered_customer_date IS NULL OR o.order_estimated_delivery_date IS NULL THEN NULL
        ELSE (o.order_delivered_customer_date::date <= o.order_estimated_delivery_date::date)
    END AS on_time_flag
FROM staging.orders o
JOIN (
    SELECT
        oi.order_id,
        MIN(oi.seller_id) AS seller_id
    FROM staging.order_items oi
    GROUP BY oi.order_id
) order_seller ON order_seller.order_id = o.order_id
JOIN dwh.dim_customer dc
  ON dc.customer_id = o.customer_id
 AND dc.is_current = TRUE
JOIN dwh.dim_seller ds
  ON ds.seller_id = order_seller.seller_id
 AND ds.is_current = TRUE
JOIN dwh.dim_date dd
  ON dd.date_key = o.order_purchase_timestamp::date;
