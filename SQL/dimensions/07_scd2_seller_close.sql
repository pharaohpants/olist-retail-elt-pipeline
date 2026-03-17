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
changed AS (
    SELECT d.seller_key
    FROM src s
    JOIN dwh.dim_seller d
      ON d.seller_id = s.seller_id
     AND d.is_current = TRUE
    WHERE COALESCE(d.location_key, -1) <> COALESCE(s.location_key, -1)
)
UPDATE dwh.dim_seller d
SET end_date = CURRENT_DATE - INTERVAL '1 day',
    is_current = FALSE
FROM changed c
WHERE d.seller_key = c.seller_key;
