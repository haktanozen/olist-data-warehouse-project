/*
===============================================================================
Quality Checks - Silver Layer
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' layer. It includes checks for:
    - NULL or duplicate primary keys.
    - Unwanted spaces or residual quote characters in string fields.
    - Data standardization and consistency.
    - Invalid or out-of-range values.
    - Invalid date logic and ordering (e.g. purchase after delivery).
    - Referential integrity between related tables.

Usage Notes:
    - Run these checks after loading the Silver Layer (EXEC silver.load_silver).
    - Expectation is noted on each check. Any results returned indicate an issue
      that should be investigated before proceeding to the Gold layer.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.catalog_category_translation'
-- ====================================================================

-- Check for NULLs in primary key
-- Expectation: No Results
SELECT *
FROM silver.catalog_category_translation
WHERE product_category_name IS NULL;

-- Check for unwanted spaces
-- Expectation: No Results
SELECT *
FROM silver.catalog_category_translation
WHERE product_category_name != TRIM(product_category_name)
   OR product_category_name_english != TRIM(product_category_name_english);

-- Spot check: distinct count
-- Expectation: ~71 categories
SELECT COUNT(*) AS category_count
FROM silver.catalog_category_translation;


-- ====================================================================
-- Checking 'silver.catalog_sellers'
-- ====================================================================

-- Check for NULLs or duplicates in primary key
-- Expectation: No Results
SELECT seller_id, COUNT(*)
FROM silver.catalog_sellers
GROUP BY seller_id
HAVING COUNT(*) > 1 OR seller_id IS NULL;

-- Check for residual quote characters
-- Expectation: No Results
SELECT *
FROM silver.catalog_sellers
WHERE seller_id LIKE '%"%'
   OR seller_zip_code_prefix LIKE '%"%';

-- Check for unwanted spaces
-- Expectation: No Results
SELECT *
FROM silver.catalog_sellers
WHERE seller_city != TRIM(seller_city)
   OR seller_state != TRIM(seller_state);

-- Data standardization: city should be lowercase, state should be uppercase
-- Expectation: No Results
SELECT DISTINCT seller_city
FROM silver.catalog_sellers
WHERE seller_city != LOWER(seller_city);

SELECT DISTINCT seller_state
FROM silver.catalog_sellers
WHERE seller_state != UPPER(seller_state);


-- ====================================================================
-- Checking 'silver.catalog_products'
-- ====================================================================

-- Check for NULLs or duplicates in primary key
-- Expectation: No Results
SELECT product_id, COUNT(*)
FROM silver.catalog_products
GROUP BY product_id
HAVING COUNT(*) > 1 OR product_id IS NULL;

-- Check for residual quote characters
-- Expectation: No Results
SELECT *
FROM silver.catalog_products
WHERE product_id LIKE '%"%';

-- Check for NULL or negative values in numeric measurement columns
-- Expectation: No negative values (NULLs are acceptable for missing data)
SELECT *
FROM silver.catalog_products
WHERE product_weight_g < 0
   OR product_length_cm < 0
   OR product_height_cm < 0
   OR product_width_cm < 0
   OR product_name_length < 0
   OR product_description_length < 0
   OR product_photos_qty < 0;

-- Check that English category name defaulted correctly
-- Expectation: No 'unknown' values ideally, but some are acceptable
-- for categories that had no match in the translation table
SELECT COUNT(*) AS unknown_category_count
FROM silver.catalog_products
WHERE product_category_name_english = 'unknown';
--623 products returned 'unknown' for product_category_name_english: NULL category (no match possible),
--'pc_gamer' and 'portateis_cozinha_e_preparadores_de_alimentos' have no entry in translation table. 
--Behavior is expected — ISNULL default applied correctly.


-- ====================================================================
-- Checking 'silver.orders_customers'
-- ====================================================================

-- Check for NULLs or duplicates in primary key (customer_id)
-- Expectation: No Results
SELECT customer_id, COUNT(*)
FROM silver.orders_customers
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL;

-- Check for residual quote characters
-- Expectation: No Results
SELECT *
FROM silver.orders_customers
WHERE customer_id LIKE '%"%'
   OR customer_unique_id LIKE '%"%';

-- Check for unwanted spaces
-- Expectation: No Results
SELECT *
FROM silver.orders_customers
WHERE customer_city != TRIM(customer_city)
   OR customer_state != TRIM(customer_state);

-- Data standardization: city lowercase, state uppercase
-- Expectation: No Results
SELECT DISTINCT customer_city
FROM silver.orders_customers
WHERE customer_city != LOWER(customer_city);

SELECT DISTINCT customer_state
FROM silver.orders_customers
WHERE customer_state != UPPER(customer_state);

-- Check for unmapped regions
-- Expectation: No Results (all 27 Brazilian states should map to a known region)
SELECT DISTINCT customer_state, customer_region
FROM silver.orders_customers
WHERE customer_region = 'Unknown';

-- Spot check: distinct regions
-- Expectation: Southeast, South, Northeast, Central-West, North
SELECT DISTINCT customer_region
FROM silver.orders_customers
ORDER BY customer_region;


-- ====================================================================
-- Checking 'silver.orders_orders'
-- ====================================================================

-- Check for NULLs or duplicates in primary key
-- Expectation: No Results
SELECT order_id, COUNT(*)
FROM silver.orders_orders
GROUP BY order_id
HAVING COUNT(*) > 1 OR order_id IS NULL;

-- Check for residual quote characters
-- Expectation: No Results
SELECT *
FROM silver.orders_orders
WHERE order_id LIKE '%"%'
   OR customer_id LIKE '%"%';

-- Data standardization: distinct order statuses
-- Expectation: known statuses only (delivered, shipped, canceled, etc.)
SELECT DISTINCT order_status
FROM silver.orders_orders
ORDER BY order_status;

-- Check for NULL purchase timestamps (critical field)
-- Expectation: No Results
SELECT *
FROM silver.orders_orders
WHERE order_purchase_timestamp IS NULL;

-- Check for invalid date order: purchase after approval
-- Expectation: No Results
SELECT *
FROM silver.orders_orders
WHERE order_purchase_timestamp > order_approved_at;

-- Check for invalid date order: approval after carrier handoff
-- Expectation: No Results
SELECT *
FROM silver.orders_orders
WHERE order_approved_at > order_delivered_carrier_date;

-- Check for invalid date order: carrier handoff after customer delivery
-- Expectation: No Results
SELECT *
FROM silver.orders_orders
WHERE order_delivered_carrier_date > order_delivered_customer_date;

-- Check for invalid date order: purchase after estimated delivery date
-- Expectation: No Results
SELECT *
FROM silver.orders_orders
WHERE order_purchase_timestamp > order_estimated_delivery_date;

-- Check for invalid date order: customer delivery before purchase
-- Expectation: No Results
SELECT *
FROM silver.orders_orders
WHERE order_purchase_timestamp > order_delivered_customer_date;


-- ====================================================================
-- Checking 'silver.orders_order_items'
-- ====================================================================

-- Check for NULLs in key columns
-- Expectation: No Results
SELECT *
FROM silver.orders_order_items
WHERE order_id IS NULL
   OR order_item_id IS NULL
   OR product_id IS NULL
   OR seller_id IS NULL;

-- Check for residual quote characters
-- Expectation: No Results
SELECT *
FROM silver.orders_order_items
WHERE order_id LIKE '%"%'
   OR product_id LIKE '%"%'
   OR seller_id LIKE '%"%';

-- Check for negative or zero price/freight values
-- Expectation: No Results
SELECT *
FROM silver.orders_order_items
WHERE price <= 0
   OR freight_value < 0;

-- Check for orphan seller_ids (no match in catalog_sellers)
-- Expectation: 2 known orphan sellers — documented in load_silver header
SELECT DISTINCT oi.seller_id
FROM silver.orders_order_items oi
LEFT JOIN silver.catalog_sellers s ON oi.seller_id = s.seller_id
WHERE s.seller_id IS NULL;

-- Check for invalid date order: shipping_limit_date before order purchase
-- Expectation: No Results
SELECT oi.*
FROM silver.orders_order_items oi
JOIN silver.orders_orders o ON oi.order_id = o.order_id
WHERE oi.shipping_limit_date < o.order_purchase_timestamp;


-- ====================================================================
-- Checking 'silver.orders_order_payments'
-- ====================================================================

-- Check for NULLs in key columns
-- Expectation: No Results
SELECT *
FROM silver.orders_order_payments
WHERE order_id IS NULL
   OR payment_sequential IS NULL
   OR payment_type IS NULL;

-- Check for residual quote characters
-- Expectation: No Results
SELECT *
FROM silver.orders_order_payments
WHERE order_id LIKE '%"%';

-- Data standardization: distinct payment types
-- Expectation: credit_card, boleto, voucher, debit_card, n/a
SELECT DISTINCT payment_type
FROM silver.orders_order_payments
ORDER BY payment_type;

-- Check for negative payment values
-- Expectation: No Results
SELECT *
FROM silver.orders_order_payments
WHERE payment_value < 0;

-- Check for invalid installment counts
-- Expectation: No Results
-- NOTE: 2 records with payment_installments = 0 and valid payment_value.
-- No sequential = 1 counterpart exists for these orders — source data entry issue.
-- Retained as-is in Silver. Consider handling as NULL or 1 at Gold layer if needed.
SELECT *
FROM silver.orders_order_payments
WHERE payment_installments <= 0;


-- ====================================================================
-- Checking 'silver.orders_order_reviews'
-- ====================================================================

-- Check for NULLs in key columns
-- Expectation: No Results
SELECT *
FROM silver.orders_order_reviews
WHERE review_id IS NULL
   OR order_id IS NULL;

-- Check for residual quote characters
-- Expectation: No Results
SELECT *
FROM silver.orders_order_reviews
WHERE review_id LIKE '%"%'
   OR order_id LIKE '%"%';

-- Check for out-of-range review scores
-- Expectation: No Results (valid range: 1-5)
SELECT *
FROM silver.orders_order_reviews
WHERE review_score NOT IN (1, 2, 3, 4, 5)
   OR review_score IS NULL;

-- Data standardization: distinct review scores
-- Expectation: 1, 2, 3, 4, 5 only
SELECT DISTINCT review_score
FROM silver.orders_order_reviews
ORDER BY review_score;

-- Check for invalid date order (creation after answer)
-- Expectation: No Results
SELECT *
FROM silver.orders_order_reviews
WHERE review_creation_date > CAST(review_answer_timestamp AS DATE);

-- Check for NULL dates in critical columns
-- Expectation: No Results
SELECT COUNT(*) AS null_creation_date
FROM silver.orders_order_reviews
WHERE review_creation_date IS NULL;

SELECT COUNT(*) AS null_answer_timestamp
FROM silver.orders_order_reviews
WHERE review_answer_timestamp IS NULL;
