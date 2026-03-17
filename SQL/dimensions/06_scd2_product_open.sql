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
current_dim AS (
    SELECT
        product_id,
        product_category_name,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM dwh.dim_product
    WHERE is_current = TRUE
)
INSERT INTO dwh.dim_product (
    product_id, product_category_name, product_weight_g,
    product_length_cm, product_height_cm, product_width_cm,
    effective_date, end_date, is_current
)
SELECT
    s.product_id, s.product_category_name, s.product_weight_g,
    s.product_length_cm, s.product_height_cm, s.product_width_cm,
    CURRENT_DATE, DATE '9999-12-31', TRUE
FROM src s
LEFT JOIN current_dim d ON d.product_id = s.product_id
WHERE d.product_id IS NULL
   OR COALESCE(d.product_category_name, '') <> COALESCE(s.product_category_name, '')
   OR COALESCE(d.product_weight_g, -1) <> COALESCE(s.product_weight_g, -1)
   OR COALESCE(d.product_length_cm, -1) <> COALESCE(s.product_length_cm, -1)
   OR COALESCE(d.product_height_cm, -1) <> COALESCE(s.product_height_cm, -1)
   OR COALESCE(d.product_width_cm, -1) <> COALESCE(s.product_width_cm, -1);
