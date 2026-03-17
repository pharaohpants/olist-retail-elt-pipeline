TRUNCATE TABLE dwh.fact_review;

INSERT INTO dwh.fact_review (
    review_id, order_id, customer_key, product_key, seller_key,
    review_score, review_comment_title, review_comment_message,
    sentiment_label, sentiment_score, review_creation_date
)
SELECT
    r.review_id,
    r.order_id,
    dc.customer_key,
    dp.product_key,
    ds.seller_key,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    CASE
        WHEN r.review_score >= 4 THEN 'positive'
        WHEN r.review_score = 3 THEN 'neutral'
        ELSE 'negative'
    END AS sentiment_label,
    ((r.review_score - 1)::numeric / 4) * 2 - 1 AS sentiment_score,
    r.review_creation_date::date AS review_creation_date
FROM staging.reviews r
JOIN staging.orders o ON o.order_id = r.order_id
JOIN (
    SELECT
        oi.order_id,
        MIN(oi.product_id) AS product_id,
        MIN(oi.seller_id) AS seller_id
    FROM staging.order_items oi
    GROUP BY oi.order_id
) x ON x.order_id = r.order_id
JOIN dwh.dim_customer dc
  ON dc.customer_id = o.customer_id
 AND dc.is_current = TRUE
JOIN dwh.dim_product dp
  ON dp.product_id = x.product_id
 AND dp.is_current = TRUE
JOIN dwh.dim_seller ds
  ON ds.seller_id = x.seller_id
 AND ds.is_current = TRUE
JOIN dwh.dim_date dd
  ON dd.date_key = r.review_creation_date::date;
