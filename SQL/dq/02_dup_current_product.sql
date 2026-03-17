SELECT COUNT(*) FROM (
    SELECT product_id
    FROM dwh.dim_product
    WHERE is_current = TRUE
    GROUP BY product_id
    HAVING COUNT(*) > 1
) x;
