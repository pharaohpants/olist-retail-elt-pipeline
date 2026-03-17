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
current_dim AS (
    SELECT
        customer_id,
        customer_unique_id,
        location_key
    FROM dwh.dim_customer
    WHERE is_current = TRUE
)
INSERT INTO dwh.dim_customer (
    customer_id, customer_unique_id, location_key,
    effective_date, end_date, is_current
)
SELECT
    s.customer_id, s.customer_unique_id, s.location_key,
    CURRENT_DATE, DATE '9999-12-31', TRUE
FROM src s
LEFT JOIN current_dim d ON d.customer_id = s.customer_id
WHERE d.customer_id IS NULL
   OR COALESCE(d.customer_unique_id, '') <> COALESCE(s.customer_unique_id, '')
   OR COALESCE(d.location_key, -1) <> COALESCE(s.location_key, -1);
