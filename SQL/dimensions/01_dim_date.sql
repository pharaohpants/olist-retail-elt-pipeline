INSERT INTO dwh.dim_date (
    date_key, year, quarter, month, month_name, week_of_year,
    day, day_of_week, day_name, is_weekend
)
SELECT DISTINCT
    d::date AS date_key,
    EXTRACT(YEAR FROM d)::int AS year,
    EXTRACT(QUARTER FROM d)::int AS quarter,
    EXTRACT(MONTH FROM d)::int AS month,
    TO_CHAR(d, 'Month')::varchar AS month_name,
    EXTRACT(WEEK FROM d)::int AS week_of_year,
    EXTRACT(DAY FROM d)::int AS day,
    EXTRACT(ISODOW FROM d)::int AS day_of_week,
    TO_CHAR(d, 'Day')::varchar AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT generate_series(
        (SELECT MIN(order_purchase_timestamp)::date FROM staging.orders),
        (SELECT MAX(COALESCE(order_delivered_customer_date, order_purchase_timestamp))::date FROM staging.orders),
        interval '1 day'
    ) AS d
) x
ON CONFLICT (date_key) DO NOTHING;
