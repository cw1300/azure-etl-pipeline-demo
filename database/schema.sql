-- Sales Analytics Database Schema

-- Create tables
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    region VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS sales (
    sale_id INTEGER PRIMARY KEY,
    product_id INTEGER,
    customer_id INTEGER,
    quantity INTEGER,
    amount DECIMAL(10, 2),
    sale_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Create staging tables for ETL process
CREATE TABLE IF NOT EXISTS stg_sales (
    sale_id INTEGER,
    product_id INTEGER,
    customer_id INTEGER,
    quantity INTEGER,
    amount DECIMAL(10, 2),
    sale_date DATE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create fact and dimension tables for star schema
CREATE TABLE IF NOT EXISTS dim_product (
    product_key INTEGER PRIMARY KEY,
    product_id INTEGER,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INTEGER PRIMARY KEY,
    customer_id INTEGER,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    region VARCHAR(50),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key INTEGER PRIMARY KEY,
    product_key INTEGER,
    customer_key INTEGER,
    date_key INTEGER,
    quantity INTEGER,
    amount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

-- Create audit and control tables
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name VARCHAR(100),
    status VARCHAR(20),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INTEGER,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS data_quality_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    table_name VARCHAR(100),
    check_type VARCHAR(50),
    check_result BOOLEAN,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);