WITH src AS (
    SELECT
        c.customer_id,
        c.customer_unique_id,
        l.location_key
    FROM staging.customers c
    JOIN dwh.dim_location l
      ON l.zip_code_prefix = c.customer_zip_code_prefix
     AND l.city = LOWER(TRIM(c.customer_city))
     AND l.state = c.customer_state
),
changed AS (
    SELECT d.customer_key
    FROM src s
    JOIN dwh.dim_customer d
      ON d.customer_id = s.customer_id
     AND d.is_current = TRUE
    WHERE COALESCE(d.customer_unique_id, '') <> COALESCE(s.customer_unique_id, '')
       OR COALESCE(d.location_key, -1) <> COALESCE(s.location_key, -1)
)
UPDATE dwh.dim_customer d
SET end_date = CURRENT_DATE - INTERVAL '1 day',
    is_current = FALSE
FROM changed c
WHERE d.customer_key = c.customer_key;
