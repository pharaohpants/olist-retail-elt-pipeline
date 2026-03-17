WITH src AS (
    SELECT
        p.product_id,
        p.product_category_name,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    FROM staging.products p
),
changed AS (
    SELECT d.product_key
    FROM src s
    JOIN dwh.dim_product d
      ON d.product_id = s.product_id
     AND d.is_current = TRUE
    WHERE COALESCE(d.product_category_name, '') <> COALESCE(s.product_category_name, '')
       OR COALESCE(d.product_weight_g, -1) <> COALESCE(s.product_weight_g, -1)
       OR COALESCE(d.product_length_cm, -1) <> COALESCE(s.product_length_cm, -1)
       OR COALESCE(d.product_height_cm, -1) <> COALESCE(s.product_height_cm, -1)
       OR COALESCE(d.product_width_cm, -1) <> COALESCE(s.product_width_cm, -1)
)
UPDATE dwh.dim_product d
SET end_date = CURRENT_DATE - INTERVAL '1 day',
    is_current = FALSE
FROM changed c
WHERE d.product_key = c.product_key;
