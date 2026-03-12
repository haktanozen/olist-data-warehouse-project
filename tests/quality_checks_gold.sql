/*
===============================================================================
Quality Checks - Gold Layer
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and integrity across the 'gold' layer. It includes checks for:
    - NULL or duplicate surrogate keys.
    - Referential integrity between fact and dimension tables.
    - NULL or invalid values in derived columns.
    - Data consistency between related columns.
    - Row count validation against Silver layer.

Usage Notes:
    - Run these checks after loading the Gold Layer (EXEC gold.load_gold).
    - Expectation is noted on each check. Any results returned indicate an issue
      that should be investigated before proceeding to the Mart layer.
===============================================================================
*/


-- ====================================================================
-- Checking 'gold.dim_date'
-- ====================================================================

-- Check for NULLs or duplicates in primary key
-- Expectation: No Results
SELECT date_key, COUNT(*)
FROM gold.dim_date
GROUP BY date_key
HAVING COUNT(*) > 1 OR date_key IS NULL;

-- Check date range coverage
-- Expectation: 2016-01-01 to 2018-12-31
SELECT MIN(full_date) AS min_date, MAX(full_date) AS max_date
FROM gold.dim_date;

-- Check for gaps in date sequence
-- Expectation: No Results
SELECT d1.full_date, DATEADD(DAY, 1, d1.full_date) AS expected_next,
       d2.full_date AS actual_next
FROM gold.dim_date d1
LEFT JOIN gold.dim_date d2
    ON d2.full_date = DATEADD(DAY, 1, d1.full_date)
WHERE d2.full_date IS NULL
  AND d1.full_date < '2018-12-31';

-- Check is_weekend values
-- Expectation: only 0 and 1
SELECT DISTINCT is_weekend
FROM gold.dim_date
ORDER BY is_weekend;


-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================

-- Check for NULLs or duplicates in surrogate key
-- Expectation: No Results
SELECT customer_key, COUNT(*)
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1 OR customer_key IS NULL;

-- Check for NULLs or duplicates in customer_unique_id (natural key)
-- Expectation: No Results
SELECT customer_unique_id, COUNT(*)
FROM gold.dim_customers
GROUP BY customer_unique_id
HAVING COUNT(*) > 1 OR customer_unique_id IS NULL;

-- Row count validation against Silver
-- Expectation: counts should match
SELECT COUNT(DISTINCT customer_unique_id) AS silver_count
FROM silver.orders_customers;

SELECT COUNT(*) AS gold_count
FROM gold.dim_customers;


-- ====================================================================
-- Checking 'gold.dim_products'
-- ====================================================================

-- Check for NULLs or duplicates in surrogate key
-- Expectation: No Results
SELECT product_key, COUNT(*)
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1 OR product_key IS NULL;

-- Check for NULLs or duplicates in product_id (natural key)
-- Expectation: No Results
SELECT product_id, COUNT(*)
FROM gold.dim_products
GROUP BY product_id
HAVING COUNT(*) > 1 OR product_id IS NULL;

-- Check unknown category count
-- Expectation: 623 (known from Silver QC)
SELECT COUNT(*) AS unknown_category_count
FROM gold.dim_products
WHERE product_category_name_english = 'unknown';


-- ====================================================================
-- Checking 'gold.dim_sellers'
-- ====================================================================

-- Check for NULLs or duplicates in surrogate key
-- Expectation: No Results
SELECT seller_key, COUNT(*)
FROM gold.dim_sellers
GROUP BY seller_key
HAVING COUNT(*) > 1 OR seller_key IS NULL;

-- Check for NULLs or duplicates in seller_id (natural key)
-- Expectation: No Results
SELECT seller_id, COUNT(*)
FROM gold.dim_sellers
GROUP BY seller_id
HAVING COUNT(*) > 1 OR seller_id IS NULL;


-- ====================================================================
-- Checking 'gold.fact_orders'
-- ====================================================================

-- Check for NULLs in key columns
-- Expectation: order_id should never be NULL.
-- order_item_id: 775 NULL records expected — orders with no matching entry
-- in orders_order_items (statuses: canceled, unavailable, shipped, invoiced,
-- created). Source data issue carried through from Silver, retained as-is.
SELECT *
FROM gold.fact_orders
WHERE order_id IS NULL
   OR order_item_id IS NULL;



-- Check for invalid derived column: total_order_value negative
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
WHERE total_order_value < 0;

-- Check for invalid derived column: delivery_days negative
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
WHERE delivery_days < 0;

-- Check is_late values
-- Expectation: only 0, 1, or NULL
SELECT DISTINCT is_late
FROM gold.fact_orders
ORDER BY is_late;

-- Check referential integrity: customer_key
-- Expectation: No Results (all customer_keys should exist in dim_customers)
SELECT DISTINCT f.customer_key
FROM gold.fact_orders f
LEFT JOIN gold.dim_customers dc ON f.customer_key = dc.customer_key
WHERE f.customer_key IS NOT NULL
  AND dc.customer_key IS NULL;

-- Check referential integrity: product_key
-- Expectation: No Results
SELECT DISTINCT f.product_key
FROM gold.fact_orders f
LEFT JOIN gold.dim_products dp ON f.product_key = dp.product_key
WHERE f.product_key IS NOT NULL
  AND dp.product_key IS NULL;

-- Check referential integrity: seller_key
-- Expectation: No Results
SELECT DISTINCT f.seller_key
FROM gold.fact_orders f
LEFT JOIN gold.dim_sellers ds ON f.seller_key = ds.seller_key
WHERE f.seller_key IS NOT NULL
  AND ds.seller_key IS NULL;

-- Check referential integrity: date_key
-- Expectation: No Results
SELECT DISTINCT f.date_key
FROM gold.fact_orders f
LEFT JOIN gold.dim_date dd ON f.date_key = dd.date_key
WHERE f.date_key IS NOT NULL
  AND dd.date_key IS NULL;

-- Check NULL foreign keys (orders with no matching dim record)
-- Expectation: Some NULLs acceptable for orphan sellers
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_key,
    SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END)  AS null_product_key,
    SUM(CASE WHEN seller_key IS NULL THEN 1 ELSE 0 END)   AS null_seller_key,
    SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END)     AS null_date_key
FROM gold.fact_orders;
-- ============================================================
-- FINDING: NULL product_key and seller_key in fact_orders
-- ============================================================
-- 775 rows have NULL product_key, 778 rows have NULL seller_key.
-- Root cause: These rows belong to canceled or unavailable orders
-- that have no corresponding records in silver.orders_order_items.
-- No product or seller was ever assigned to these orders in the
-- source system. This is expected behavior, not a data pipeline error.
-- Action: No fix required.
-- ============================================================

-- Check payment_installments — no zeros expected (handled in load)
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
WHERE payment_installments = 0;

-- Check is_late consistency with delivery dates
-- Expectation: No Results (if delivered before estimated, is_late must be 0)
SELECT *
FROM gold.fact_orders
WHERE order_delivered_customer_date <= order_estimated_delivery_date
  AND is_late = 1;


-- ====================================================================
-- Checking 'gold.fact_order_reviews'
-- ====================================================================

-- Check for NULLs in key columns
-- Expectation: No Results
SELECT *
FROM gold.fact_order_reviews
WHERE review_id IS NULL
   OR order_id IS NULL;

-- Check for out-of-range review scores
-- Expectation: No Results
SELECT *
FROM gold.fact_order_reviews
WHERE review_score NOT IN (1, 2, 3, 4, 5)
   OR review_score IS NULL;

-- Check referential integrity: date_key
-- Expectation: No Results
SELECT DISTINCT f.date_key
FROM gold.fact_order_reviews f
LEFT JOIN gold.dim_date dd ON f.date_key = dd.date_key
WHERE f.date_key IS NOT NULL
  AND dd.date_key IS NULL;

-- Row count validation against Silver
-- Expectation: counts should match
SELECT COUNT(*) AS silver_count FROM silver.orders_order_reviews;
SELECT COUNT(*) AS gold_count FROM gold.fact_order_reviews;

