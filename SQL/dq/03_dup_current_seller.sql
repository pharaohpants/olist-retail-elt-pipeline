SELECT COUNT(*) FROM (
    SELECT seller_id
    FROM dwh.dim_seller
    WHERE is_current = TRUE
    GROUP BY seller_id
    HAVING COUNT(*) > 1
) x;
