/*
===============================================================================
Indexes - Gold Layer
===============================================================================
Script Purpose:
    This script creates indexes on Gold layer tables to optimize query
    performance for analytical workloads. Each index is dropped if it
    already exists before being recreated (defensive pattern).

Index Strategy:
    fact_orders  : surrogate keys used in filters/joins + business key (order_id)
    dim tables   : business keys used in ETL joins (surrogate keys skipped —
                   they are PRIMARY KEYs and already have a clustered index)

Usage Notes:
    - Run this script after executing EXEC gold.load_gold;
    - Safe to re-run at any time — existing indexes are dropped first.
===============================================================================
*/

-- ============================================================
-- fact_orders: frequently filtered and joined columns
-- ============================================================

-- order_id: business key, used in external lookups and reporting queries
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_fact_orders_order_id' AND object_id = OBJECT_ID('gold.fact_orders'))
    DROP INDEX idx_fact_orders_order_id ON gold.fact_orders;
CREATE INDEX idx_fact_orders_order_id ON gold.fact_orders (order_id);

-- customer_key: used in JOIN with dim_customers and customer-level analysis
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_fact_orders_customer_key' AND object_id = OBJECT_ID('gold.fact_orders'))
    DROP INDEX idx_fact_orders_customer_key ON gold.fact_orders;
CREATE INDEX idx_fact_orders_customer_key ON gold.fact_orders (customer_key);

-- date_key: used in WHERE filters for time-based analysis (monthly, quarterly)
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_fact_orders_date_key' AND object_id = OBJECT_ID('gold.fact_orders'))
    DROP INDEX idx_fact_orders_date_key ON gold.fact_orders;
CREATE INDEX idx_fact_orders_date_key ON gold.fact_orders (date_key);

-- ============================================================
-- dim_customers: business keys used in ETL joins and reporting
-- Note: customer_key is PRIMARY KEY — clustered index already exists
-- ============================================================

-- customer_unique_id: used in ETL JOIN from silver, and in customer-level reporting
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_dim_customers_unique_id' AND object_id = OBJECT_ID('gold.dim_customers'))
    DROP INDEX idx_dim_customers_unique_id ON gold.dim_customers;
CREATE INDEX idx_dim_customers_unique_id ON gold.dim_customers (customer_unique_id);

-- ============================================================
-- dim_products: business keys used in ETL joins
-- Note: product_key is PRIMARY KEY — clustered index already exists
-- ============================================================

-- product_id: used in ETL JOIN from silver.orders_order_items
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_dim_products_product_id' AND object_id = OBJECT_ID('gold.dim_products'))
    DROP INDEX idx_dim_products_product_id ON gold.dim_products;
CREATE INDEX idx_dim_products_product_id ON gold.dim_products (product_id);

-- ============================================================
-- dim_sellers: business keys used in ETL joins
-- Note: seller_key is PRIMARY KEY — clustered index already exists
-- ============================================================

-- seller_id: used in ETL JOIN from silver.orders_order_items
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_dim_sellers_seller_id' AND object_id = OBJECT_ID('gold.dim_sellers'))
    DROP INDEX idx_dim_sellers_seller_id ON gold.dim_sellers;
CREATE INDEX idx_dim_sellers_seller_id ON gold.dim_sellers (seller_id);

-- ============================================================
-- dim_date: business keys used in ETL joins
-- Note: date_key is PRIMARY KEY — clustered index already exists
-- ============================================================

-- full_date: used in ETL JOIN when mapping order dates to date_key
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_dim_date_full_date' AND object_id = OBJECT_ID('gold.dim_date'))
    DROP INDEX idx_dim_date_full_date ON gold.dim_date;
CREATE INDEX idx_dim_date_full_date ON gold.dim_date (full_date);
