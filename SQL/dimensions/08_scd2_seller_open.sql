WITH src AS (
    SELECT
        s.seller_id,
        l.location_key
    FROM staging.sellers s
    JOIN dwh.dim_location l
      ON l.zip_code_prefix = s.seller_zip_code_prefix
     AND l.city = LOWER(TRIM(s.seller_city))
     AND l.state = s.seller_state
),
current_dim AS (
    SELECT seller_id, location_key
    FROM dwh.dim_seller
    WHERE is_current = TRUE
)
INSERT INTO dwh.dim_seller (
    seller_id, location_key, effective_date, end_date, is_current
)
SELECT
    s.seller_id, s.location_key, CURRENT_DATE, DATE '9999-12-31', TRUE
FROM src s
LEFT JOIN current_dim d ON d.seller_id = s.seller_id
WHERE d.seller_id IS NULL
   OR COALESCE(d.location_key, -1) <> COALESCE(s.location_key, -1);
