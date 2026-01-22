CREATE TABLE IF NOT EXISTS stores (
    store_id SERIAL PRIMARY KEY,
    store_code VARCHAR(20) UNIQUE NOT NULL,
    store_name VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    region VARCHAR(50),
    store_type VARCHAR(30),
    opened_date DATE
);

CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    sku_code VARCHAR(30) UNIQUE NOT NULL,
    product_name VARCHAR(150),
    brand VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    pack_size VARCHAR(30),
    unit_price NUMERIC(10,2),
    launch_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS calendar (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    week_of_year INT,
    quarter INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS sales_fact (
    sales_id BIGSERIAL PRIMARY KEY,
    date_id DATE NOT NULL,
    store_id INT NOT NULL,
    product_id INT NOT NULL,
    units_sold INT,
    gross_sales NUMERIC(12,2),
    discount_amount NUMERIC(12,2),
    net_sales NUMERIC(12,2),
    promo_flag BOOLEAN DEFAULT FALSE,
    inventory_on_hand INT,

    CONSTRAINT fk_sales_date FOREIGN KEY (date_id) REFERENCES calendar(date_id),
    CONSTRAINT fk_sales_store FOREIGN KEY (store_id) REFERENCES stores(store_id),
    CONSTRAINT fk_sales_product FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT uq_sales UNIQUE (date_id, store_id, product_id)
);
