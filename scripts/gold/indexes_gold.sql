/*
===============================================================================
Indexes - Gold Layer
===============================================================================
Script Purpose:
    This script creates indexes on Gold layer tables to optimize query
    performance for analytical workloads. Indexes are created after
    data load to avoid overhead during INSERT operations.

Usage Notes:
    - Run this script after executing EXEC gold.load_gold;
    - Re-run after any full reload if indexes are dropped and recreated.
===============================================================================
*/

-- fact_orders: most frequently joined and filtered columns
CREATE INDEX idx_fact_orders_order_id
    ON gold.fact_orders (order_id);

CREATE INDEX idx_fact_orders_customer_key
    ON gold.fact_orders (customer_key);

CREATE INDEX idx_fact_orders_date_key
    ON gold.fact_orders (date_key);

-- dim tables: surrogate keys used in fact table joins
CREATE INDEX idx_dim_customers_key
    ON gold.dim_customers (customer_key);

CREATE INDEX idx_dim_products_key
    ON gold.dim_products (product_key);

CREATE INDEX idx_dim_sellers_key
    ON gold.dim_sellers (seller_key);

CREATE INDEX idx_dim_date_key
    ON gold.dim_date (date_key);