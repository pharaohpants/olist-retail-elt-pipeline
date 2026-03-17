SELECT COUNT(*) FROM (
    SELECT customer_id
    FROM dwh.dim_customer
    WHERE is_current = TRUE
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) x;
