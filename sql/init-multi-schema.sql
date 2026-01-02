-- sql/init-multi-schema.sql

-- Create databases
CREATE DATABASE sales_db;
CREATE DATABASE analytics_db;
CREATE DATABASE ref_db;

-- Connect to sales_db and create schema
\c sales_db;

CREATE SCHEMA IF NOT EXISTS sales;
SET search_path TO sales, public;

CREATE TABLE sales.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    amount_usd DECIMAL(10,2),
    cost_usd DECIMAL(10,2),
    order_date DATE,
    status VARCHAR(50)
);

CREATE TABLE sales.order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES sales.orders(order_id),
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2)
);

-- Connect to ref_db and create schema
\c ref_db;

CREATE SCHEMA IF NOT EXISTS ref;
SET search_path TO ref, public;

CREATE TABLE ref.customers (
    customer_id SERIAL PRIMARY KEY,
    full_name VARCHAR(255),
    country_code VARCHAR(2),
    email VARCHAR(255),
    created_at TIMESTAMP
);

CREATE TABLE ref.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    unit_cost DECIMAL(10,2)
);

-- Connect to analytics_db and create schema
\c analytics_db;

CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics, public;

CREATE TABLE analytics.customer_segments (
    customer_id INTEGER PRIMARY KEY,
    segment_name VARCHAR(100),
    lifetime_value DECIMAL(10,2),
    last_activity_date DATE
);

CREATE TABLE analytics.daily_metrics (
    metric_date DATE,
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,2),
    PRIMARY KEY (metric_date, metric_name)
);

-- Insert sample data
\c sales_db;
INSERT INTO sales.orders VALUES
    (1, 1001, 1000.00, 600.00, '2024-01-15', 'completed'),
    (2, 1002, 500.00, 300.00, '2024-01-16', 'completed'),
    (3, 1003, 750.00, 450.00, '2024-01-17', 'pending');

\c ref_db;
INSERT INTO ref.customers VALUES
    (1001, 'John Doe', 'US', 'john@example.com', '2023-01-01'),
    (1002, 'Jane Smith', 'UK', 'jane@example.com', '2023-02-01'),
    (1003, 'Bob Johnson', 'DE', 'bob@example.com', '2023-03-01');

\c analytics_db;
INSERT INTO analytics.customer_segments VALUES
    (1001, 'enterprise', 50000.00, '2024-01-15'),
    (1002, 'premium', 25000.00, '2024-01-16'),
    (1003, 'standard', 10000.00, '2024-01-17');