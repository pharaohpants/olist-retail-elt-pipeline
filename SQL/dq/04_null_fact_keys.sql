SELECT COUNT(*) FROM dwh.fact_sales
WHERE product_key IS NULL OR seller_key IS NULL OR customer_key IS NULL;
