/*
===============================================================================
Indexes - Gold Layer
===============================================================================
Script Purpose:
    This script creates indexes on Gold layer tables to optimize query
    performance for analytical workloads. Each index is dropped if it
    already exists before being recreated (defensive pattern).

Usage Notes:
    - Run this script after executing EXEC gold.load_gold;
    - Safe to re-run at any time — existing indexes are dropped first.
===============================================================================
*/


-- ============================================================
-- fact_orders: most frequently joined and filtered columns
-- ============================================================

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_fact_orders_order_id' AND object_id = OBJECT_ID('gold.fact_orders'))
    DROP INDEX idx_fact_orders_order_id ON gold.fact_orders;
CREATE INDEX idx_fact_orders_order_id ON gold.fact_orders (order_id);

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_fact_orders_customer_key' AND object_id = OBJECT_ID('gold.fact_orders'))
    DROP INDEX idx_fact_orders_customer_key ON gold.fact_orders;
CREATE INDEX idx_fact_orders_customer_key ON gold.fact_orders (customer_key);

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_fact_orders_date_key' AND object_id = OBJECT_ID('gold.fact_orders'))
    DROP INDEX idx_fact_orders_date_key ON gold.fact_orders;
CREATE INDEX idx_fact_orders_date_key ON gold.fact_orders (date_key);


-- ============================================================
-- dim tables: surrogate keys used in fact table joins
-- ============================================================

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_dim_customers_key' AND object_id = OBJECT_ID('gold.dim_customers'))
    DROP INDEX idx_dim_customers_key ON gold.dim_customers;
CREATE INDEX idx_dim_customers_key ON gold.dim_customers (customer_key);

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_dim_products_key' AND object_id = OBJECT_ID('gold.dim_products'))
    DROP INDEX idx_dim_products_key ON gold.dim_products;
CREATE INDEX idx_dim_products_key ON gold.dim_products (product_key);

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_dim_sellers_key' AND object_id = OBJECT_ID('gold.dim_sellers'))
    DROP INDEX idx_dim_sellers_key ON gold.dim_sellers;
CREATE INDEX idx_dim_sellers_key ON gold.dim_sellers (seller_key);

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_dim_date_key' AND object_id = OBJECT_ID('gold.dim_date'))
    DROP INDEX idx_dim_date_key ON gold.dim_date;
CREATE INDEX idx_dim_date_key ON gold.dim_date (date_key);
