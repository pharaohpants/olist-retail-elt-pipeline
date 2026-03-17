-- =========================================
-- CREATE SCHEMA
-- =========================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- =========================================
-- STAGING TABLES (tetap sama - mirror source)
-- =========================================

CREATE TABLE staging.orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE staging.order_items (
    order_id VARCHAR,
    order_item_id INT,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date TIMESTAMP,
    price NUMERIC,
    freight_value NUMERIC
);

CREATE TABLE staging.products (
    product_id VARCHAR PRIMARY KEY,
    product_category_name VARCHAR,
    product_name_lenght REAL,
    product_description_lenght REAL,
    product_photos_qty REAL,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE staging.customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix INT,
    customer_city VARCHAR,
    customer_state VARCHAR
);

CREATE TABLE staging.sellers (
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR,
    seller_state VARCHAR
);

CREATE TABLE staging.reviews (
    review_id VARCHAR,
    order_id VARCHAR,
    review_score INT,
    review_comment_title VARCHAR,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- =========================================
-- DIMENSION TABLES
-- =========================================

-- ----------------------------------------
-- DIM_DATE: Diperkaya untuk agregasi harian, mingguan, bulanan, kuartalan, tahunan
-- ----------------------------------------
CREATE TABLE dwh.dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR,
    week_of_year INT,
    day INT,
    day_of_week INT,
    day_name VARCHAR,
    is_weekend BOOLEAN
);

-- ----------------------------------------
-- DIM_LOCATION: Normalisasi wilayah (dipakai customer & seller)
-- Mendukung analisis regional untuk Sales, Delivery, Review
-- ----------------------------------------
CREATE TABLE dwh.dim_location (
    location_key SERIAL PRIMARY KEY,
    zip_code_prefix INT,
    city VARCHAR,
    state VARCHAR,
    UNIQUE (zip_code_prefix, city, state)
);

-- ----------------------------------------
-- DIM_CUSTOMER: SCD Type 2 - menyimpan histori perubahan lokasi
-- Mendukung: analisis regional + tren jangka panjang
-- ----------------------------------------
CREATE TABLE dwh.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR,
    customer_unique_id VARCHAR,
    location_key INT,
    effective_date DATE NOT NULL,
    end_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,

    CONSTRAINT fk_customer_location
        FOREIGN KEY (location_key)
        REFERENCES dwh.dim_location(location_key)
);

-- ----------------------------------------
-- DIM_PRODUCT: SCD Type 2 - menyimpan histori perubahan kategori & dimensi
-- Mendukung: analisis per product, per kategori, tren harga jangka panjang
-- ----------------------------------------
CREATE TABLE dwh.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR,
    product_category_name VARCHAR,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    effective_date DATE NOT NULL,
    end_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- ----------------------------------------
-- DIM_SELLER: SCD Type 2 - menyimpan histori perubahan lokasi seller
-- Mendukung: analisis regional seller + tren jangka panjang
-- ----------------------------------------
CREATE TABLE dwh.dim_seller (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR,
    location_key INT,
    effective_date DATE NOT NULL,
    end_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,

    CONSTRAINT fk_seller_location
        FOREIGN KEY (location_key)
        REFERENCES dwh.dim_location(location_key)
);

-- =========================================
-- FACT TABLES
-- =========================================

-- ----------------------------------------
-- FACT_SALES
-- Grain: 1 row per order item
-- KPI: Total revenue, quantity sold, number of orders
-- Mendukung: analisis per product, per kategori, per wilayah, bulanan/tahunan
-- ----------------------------------------
CREATE TABLE dwh.fact_sales (
    sales_key SERIAL PRIMARY KEY,

    order_id VARCHAR,
    order_item_id INT,
    order_status VARCHAR,

    product_key INT,
    seller_key INT,
    customer_key INT,

    purchase_date DATE,

    price NUMERIC,
    freight_value NUMERIC,
    quantity INT DEFAULT 1,

    CONSTRAINT fk_sales_product
        FOREIGN KEY (product_key)
        REFERENCES dwh.dim_product(product_key),

    CONSTRAINT fk_sales_seller
        FOREIGN KEY (seller_key)
        REFERENCES dwh.dim_seller(seller_key),

    CONSTRAINT fk_sales_customer
        FOREIGN KEY (customer_key)
        REFERENCES dwh.dim_customer(customer_key),

    CONSTRAINT fk_sales_date
        FOREIGN KEY (purchase_date)
        REFERENCES dwh.dim_date(date_key)
);

-- ----------------------------------------
-- FACT_DELIVERY
-- Grain: 1 row per order
-- KPI: delivery_lead_time_days, delay_days, on_time_flag
-- Mendukung: analisis harian, agregasi mingguan/bulanan, per wilayah
-- ----------------------------------------
CREATE TABLE dwh.fact_delivery (
    delivery_key SERIAL PRIMARY KEY,

    order_id VARCHAR,
    order_status VARCHAR,

    customer_key INT,
    seller_key INT,

    purchase_date DATE,
    approved_date DATE,
    delivered_carrier_date DATE,
    delivered_customer_date DATE,
    estimated_delivery_date DATE,

    delivery_lead_time_days INT,       -- delivered - purchase
    carrier_to_customer_days INT,      -- delivered - carrier
    delay_days INT,                    -- delivered - estimated (negatif = early)
    on_time_flag BOOLEAN,              -- delivered <= estimated

    CONSTRAINT fk_delivery_customer
        FOREIGN KEY (customer_key)
        REFERENCES dwh.dim_customer(customer_key),

    CONSTRAINT fk_delivery_seller
        FOREIGN KEY (seller_key)
        REFERENCES dwh.dim_seller(seller_key),

    CONSTRAINT fk_delivery_purchase_date
        FOREIGN KEY (purchase_date)
        REFERENCES dwh.dim_date(date_key)
);

-- ----------------------------------------
-- FACT_REVIEW
-- Grain: 1 row per review
-- KPI: avg rating, review count, positive/negative %, sentiment score
-- Mendukung: per product, per seller, per wilayah, bulanan/kuartalan
-- Mendukung: sentiment analysis & text clustering
-- ----------------------------------------
CREATE TABLE dwh.fact_review (
    review_key SERIAL PRIMARY KEY,

    review_id VARCHAR,
    order_id VARCHAR,

    customer_key INT,
    product_key INT,
    seller_key INT,

    review_score INT,
    review_comment_title VARCHAR,
    review_comment_message TEXT,
    sentiment_label VARCHAR,           -- 'positive', 'neutral', 'negative'
    sentiment_score NUMERIC,           -- output dari model sentiment

    review_creation_date DATE,

    CONSTRAINT fk_review_customer
        FOREIGN KEY (customer_key)
        REFERENCES dwh.dim_customer(customer_key),

    CONSTRAINT fk_review_product
        FOREIGN KEY (product_key)
        REFERENCES dwh.dim_product(product_key),

    CONSTRAINT fk_review_seller
        FOREIGN KEY (seller_key)
        REFERENCES dwh.dim_seller(seller_key),

    CONSTRAINT fk_review_date
        FOREIGN KEY (review_creation_date)
        REFERENCES dwh.dim_date(date_key)
);

-- =========================================
-- INDEX FOR QUERY PERFORMANCE
-- =========================================

-- DIMENSION INDEX
CREATE INDEX idx_customer_current ON dwh.dim_customer(is_current);
CREATE INDEX idx_customer_id ON dwh.dim_customer(customer_id);
CREATE INDEX idx_product_current ON dwh.dim_product(is_current);
CREATE INDEX idx_product_id ON dwh.dim_product(product_id);
CREATE INDEX idx_product_category ON dwh.dim_product(product_category_name);
CREATE INDEX idx_seller_current ON dwh.dim_seller(is_current);
CREATE INDEX idx_seller_id ON dwh.dim_seller(seller_id);
CREATE INDEX idx_location_state ON dwh.dim_location(state);
CREATE INDEX idx_location_city ON dwh.dim_location(city);

-- DATE INDEX
CREATE INDEX idx_date_year_month ON dwh.dim_date(year, month);
CREATE INDEX idx_date_quarter ON dwh.dim_date(year, quarter);
CREATE INDEX idx_date_week ON dwh.dim_date(year, week_of_year);

-- SALES INDEX
CREATE INDEX idx_sales_product ON dwh.fact_sales(product_key);
CREATE INDEX idx_sales_customer ON dwh.fact_sales(customer_key);
CREATE INDEX idx_sales_seller ON dwh.fact_sales(seller_key);
CREATE INDEX idx_sales_date ON dwh.fact_sales(purchase_date);
CREATE INDEX idx_sales_order_status ON dwh.fact_sales(order_status);

-- DELIVERY INDEX
CREATE INDEX idx_delivery_customer ON dwh.fact_delivery(customer_key);
CREATE INDEX idx_delivery_seller ON dwh.fact_delivery(seller_key);
CREATE INDEX idx_delivery_date ON dwh.fact_delivery(purchase_date);
CREATE INDEX idx_delivery_ontime ON dwh.fact_delivery(on_time_flag);
CREATE INDEX idx_delivery_status ON dwh.fact_delivery(order_status);

-- REVIEW INDEX
CREATE INDEX idx_review_customer ON dwh.fact_review(customer_key);
CREATE INDEX idx_review_product ON dwh.fact_review(product_key);
CREATE INDEX idx_review_seller ON dwh.fact_review(seller_key);
CREATE INDEX idx_review_date ON dwh.fact_review(review_creation_date);
CREATE INDEX idx_review_score ON dwh.fact_review(review_score);
CREATE INDEX idx_review_sentiment ON dwh.fact_review(sentiment_label);