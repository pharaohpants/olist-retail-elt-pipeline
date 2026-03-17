INSERT INTO dwh.dim_location (zip_code_prefix, city, state)
SELECT DISTINCT
    customer_zip_code_prefix AS zip_code_prefix,
    LOWER(TRIM(customer_city)) AS city,
    customer_state AS state
FROM staging.customers
UNION
SELECT DISTINCT
    seller_zip_code_prefix AS zip_code_prefix,
    LOWER(TRIM(seller_city)) AS city,
    seller_state AS state
FROM staging.sellers
ON CONFLICT (zip_code_prefix, city, state) DO NOTHING;
