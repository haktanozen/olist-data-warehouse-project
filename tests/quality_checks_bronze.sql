/*
===============================================================================
Quality Checks: Bronze Layer
===============================================================================
Script Purpose:
    This script performs quality checks on the bronze layer tables to validate
    data completeness, consistency, and referential integrity after loading.

    Checks include:
    - Row count validation against expected source counts
    - NULL / empty field detection
    - Duplicate key detection
    - Referential integrity between related tables
    - Distinct value inspection for categorical columns
    - Min / Max range checks for numeric columns
    - Cross-table orphan detection

Usage:
    Run after executing EXEC bronze.load_bronze and manually loading
    bronze.orders_order_reviews via SSMS Import Flat File Wizard.
===============================================================================
*/

-- =============================================================================
-- 1. ROW COUNT VALIDATION
-- Expected counts based on source CSV files
-- =============================================================================
SELECT 'orders_orders'               AS table_name, COUNT(*) AS actual_count, 99441  AS expected_count FROM bronze.orders_orders             UNION ALL
SELECT 'orders_order_items',                         COUNT(*),                 112650                  FROM bronze.orders_order_items         UNION ALL
SELECT 'orders_order_payments',                      COUNT(*),                 103886                  FROM bronze.orders_order_payments      UNION ALL
SELECT 'orders_order_reviews',                       COUNT(*),                 99224                   FROM bronze.orders_order_reviews       UNION ALL
SELECT 'orders_customers',                           COUNT(*),                 99441                   FROM bronze.orders_customers           UNION ALL
SELECT 'catalog_products',                           COUNT(*),                 32951                   FROM bronze.catalog_products           UNION ALL
SELECT 'catalog_sellers',                            COUNT(*),                 3093                    FROM bronze.catalog_sellers            UNION ALL
SELECT 'catalog_category_translation',               COUNT(*),                 71                      FROM bronze.catalog_category_translation;


-- =============================================================================
-- 2. ORDERS_ORDERS
-- =============================================================================

-- 2a. NULL / Empty Check
SELECT
    SUM(CASE WHEN order_id = '' OR order_id IS NULL THEN 1 ELSE 0 END)                             AS null_order_id,
    SUM(CASE WHEN customer_id = '' OR customer_id IS NULL THEN 1 ELSE 0 END)                       AS null_customer_id,
    SUM(CASE WHEN order_status = '' OR order_status IS NULL THEN 1 ELSE 0 END)                     AS null_order_status,
    SUM(CASE WHEN order_purchase_timestamp = '' OR order_purchase_timestamp IS NULL THEN 1 ELSE 0 END) AS null_purchase_ts,
    SUM(CASE WHEN order_approved_at = '' OR order_approved_at IS NULL THEN 1 ELSE 0 END)           AS null_approved_at,
    SUM(CASE WHEN order_delivered_carrier_date = '' OR order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END) AS null_carrier_date,
    SUM(CASE WHEN order_delivered_customer_date = '' OR order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) AS null_delivered_date,
    SUM(CASE WHEN order_estimated_delivery_date = '' OR order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END) AS null_estimated_date
FROM bronze.orders_orders;

-- 2b. Duplicate order_id Check (expected: 0 duplicates)
SELECT order_id, COUNT(*) AS cnt
FROM bronze.orders_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 2c. Distinct order_status Values
SELECT order_status, COUNT(*) AS cnt
FROM bronze.orders_orders
GROUP BY order_status
ORDER BY cnt DESC;

-- 2d. dwh_create_date Check
SELECT
    SUM(CASE WHEN dwh_create_date IS NULL THEN 1 ELSE 0 END) AS null_create_date,
    MIN(dwh_create_date) AS first_load,
    MAX(dwh_create_date) AS last_load
FROM bronze.orders_orders;


-- =============================================================================
-- 3. ORDERS_ORDER_ITEMS
-- =============================================================================

-- 3a. NULL / Empty Check
SELECT
    SUM(CASE WHEN order_id = '' OR order_id IS NULL THEN 1 ELSE 0 END)             AS null_order_id,
    SUM(CASE WHEN order_item_id = '' OR order_item_id IS NULL THEN 1 ELSE 0 END)   AS null_order_item_id,
    SUM(CASE WHEN product_id = '' OR product_id IS NULL THEN 1 ELSE 0 END)         AS null_product_id,
    SUM(CASE WHEN seller_id = '' OR seller_id IS NULL THEN 1 ELSE 0 END)           AS null_seller_id,
    SUM(CASE WHEN shipping_limit_date = '' OR shipping_limit_date IS NULL THEN 1 ELSE 0 END) AS null_shipping_date,
    SUM(CASE WHEN price = '' OR price IS NULL THEN 1 ELSE 0 END)                   AS null_price,
    SUM(CASE WHEN freight_value = '' OR freight_value IS NULL THEN 1 ELSE 0 END)   AS null_freight_value
FROM bronze.orders_order_items;

-- 3b. Price and Freight Min / Max
SELECT
    MIN(CAST(price AS DECIMAL(10,2)))         AS min_price,
    MAX(CAST(price AS DECIMAL(10,2)))         AS max_price,
    MIN(CAST(freight_value AS DECIMAL(10,2))) AS min_freight,
    MAX(CAST(freight_value AS DECIMAL(10,2))) AS max_freight
FROM bronze.orders_order_items
WHERE ISNUMERIC(price) = 1 AND ISNUMERIC(freight_value) = 1;

-- 3c. order_item_id Range (how many items per order at most?)
SELECT order_item_id, COUNT(*) AS cnt
FROM bronze.orders_order_items
GROUP BY order_item_id
ORDER BY CAST(order_item_id AS INT) DESC;

-- 3d. Referential Integrity — order_id must exist in orders_orders
SELECT COUNT(*) AS orphan_order_items
FROM bronze.orders_order_items oi
WHERE NOT EXISTS (
    SELECT 1 FROM bronze.orders_orders o
    WHERE o.order_id = oi.order_id
);


-- =============================================================================
-- 4. ORDERS_ORDER_PAYMENTS
-- =============================================================================

-- 4a. NULL / Empty Check
SELECT
    SUM(CASE WHEN order_id = '' OR order_id IS NULL THEN 1 ELSE 0 END)                     AS null_order_id,
    SUM(CASE WHEN payment_sequential = '' OR payment_sequential IS NULL THEN 1 ELSE 0 END) AS null_payment_sequential,
    SUM(CASE WHEN payment_type = '' OR payment_type IS NULL THEN 1 ELSE 0 END)             AS null_payment_type,
    SUM(CASE WHEN payment_installments = '' OR payment_installments IS NULL THEN 1 ELSE 0 END) AS null_installments,
    SUM(CASE WHEN payment_value = '' OR payment_value IS NULL THEN 1 ELSE 0 END)           AS null_payment_value
FROM bronze.orders_order_payments;

-- 4b. Distinct payment_type Values (check for 'not_defined')
SELECT payment_type, COUNT(*) AS cnt
FROM bronze.orders_order_payments
GROUP BY payment_type
ORDER BY cnt DESC;

-- 4c. Payment Value Min / Max
SELECT
    MIN(CAST(payment_value AS DECIMAL(10,2))) AS min_value,
    MAX(CAST(payment_value AS DECIMAL(10,2))) AS max_value
FROM bronze.orders_order_payments
WHERE ISNUMERIC(payment_value) = 1;

-- 4d. Referential Integrity — order_id must exist in orders_orders
SELECT COUNT(*) AS orphan_payments
FROM bronze.orders_order_payments op
WHERE NOT EXISTS (
    SELECT 1 FROM bronze.orders_orders o
    WHERE o.order_id = op.order_id
);


-- =============================================================================
-- 5. ORDERS_ORDER_REVIEWS
-- =============================================================================

-- 5a. NULL / Empty Check
SELECT
    SUM(CASE WHEN review_id = '' OR review_id IS NULL THEN 1 ELSE 0 END)                       AS null_review_id,
    SUM(CASE WHEN order_id = '' OR order_id IS NULL THEN 1 ELSE 0 END)                         AS null_order_id,
    SUM(CASE WHEN review_score = '' OR review_score IS NULL THEN 1 ELSE 0 END)                 AS null_review_score,
    SUM(CASE WHEN review_comment_title = '' OR review_comment_title IS NULL THEN 1 ELSE 0 END) AS null_comment_title,
    SUM(CASE WHEN review_comment_message = '' OR review_comment_message IS NULL THEN 1 ELSE 0 END) AS null_comment_message,
    SUM(CASE WHEN review_creation_date = '' OR review_creation_date IS NULL THEN 1 ELSE 0 END) AS null_creation_date,
    SUM(CASE WHEN review_answer_timestamp = '' OR review_answer_timestamp IS NULL THEN 1 ELSE 0 END) AS null_answer_ts
FROM bronze.orders_order_reviews;

-- 5b. Distinct review_score Values (expected: 1 to 5 only)
SELECT review_score, COUNT(*) AS cnt
FROM bronze.orders_order_reviews
GROUP BY review_score
ORDER BY cnt DESC;

-- 5c. Referential Integrity — order_id must exist in orders_orders
SELECT COUNT(*) AS orphan_reviews
FROM bronze.orders_order_reviews r
WHERE NOT EXISTS (
    SELECT 1 FROM bronze.orders_orders o
    WHERE o.order_id = r.order_id
);


-- =============================================================================
-- 6. ORDERS_CUSTOMERS
-- =============================================================================

-- 6a. NULL / Empty Check
SELECT
    SUM(CASE WHEN customer_id = '' OR customer_id IS NULL THEN 1 ELSE 0 END)                   AS null_customer_id,
    SUM(CASE WHEN customer_unique_id = '' OR customer_unique_id IS NULL THEN 1 ELSE 0 END)     AS null_unique_id,
    SUM(CASE WHEN customer_zip_code_prefix = '' OR customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip,
    SUM(CASE WHEN customer_city = '' OR customer_city IS NULL THEN 1 ELSE 0 END)               AS null_city,
    SUM(CASE WHEN customer_state = '' OR customer_state IS NULL THEN 1 ELSE 0 END)             AS null_state
FROM bronze.orders_customers;

-- 6b. customer_id vs customer_unique_id
-- customer_unique_id < customer_id means some customers placed multiple orders
SELECT
    COUNT(customer_id)              AS total_customer_ids,
    COUNT(DISTINCT customer_unique_id) AS unique_customers
FROM bronze.orders_customers;

-- 6c. Distinct States
SELECT customer_state, COUNT(*) AS cnt
FROM bronze.orders_customers
GROUP BY customer_state
ORDER BY cnt DESC;


-- =============================================================================
-- 7. CATALOG_PRODUCTS
-- =============================================================================

-- 7a. NULL / Empty Check
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN product_id = '' OR product_id IS NULL THEN 1 ELSE 0 END)                         AS null_product_id,
    SUM(CASE WHEN product_category_name = '' OR product_category_name IS NULL THEN 1 ELSE 0 END)   AS null_category,
    SUM(CASE WHEN product_name_lenght = '' OR product_name_lenght IS NULL THEN 1 ELSE 0 END)       AS null_name_length,
    SUM(CASE WHEN product_description_lenght = '' OR product_description_lenght IS NULL THEN 1 ELSE 0 END) AS null_desc_length,
    SUM(CASE WHEN product_photos_qty = '' OR product_photos_qty IS NULL THEN 1 ELSE 0 END)         AS null_photos,
    SUM(CASE WHEN product_weight_g = '' OR product_weight_g IS NULL THEN 1 ELSE 0 END)             AS null_weight,
    SUM(CASE WHEN product_length_cm = '' OR product_length_cm IS NULL THEN 1 ELSE 0 END)           AS null_length,
    SUM(CASE WHEN product_height_cm = '' OR product_height_cm IS NULL THEN 1 ELSE 0 END)           AS null_height,
    SUM(CASE WHEN product_width_cm = '' OR product_width_cm IS NULL THEN 1 ELSE 0 END)             AS null_width
FROM bronze.catalog_products;

-- 7b. Top Categories by Product Count
SELECT product_category_name, COUNT(*) AS product_count
FROM bronze.catalog_products
GROUP BY product_category_name
ORDER BY product_count DESC;

-- 7c. Products with No Category Translation Mapping
SELECT COUNT(*) AS unmapped_categories
FROM bronze.catalog_products p
WHERE p.product_category_name NOT IN (
    SELECT product_category_name
    FROM bronze.catalog_category_translation
)
AND p.product_category_name IS NOT NULL
AND p.product_category_name != '';

-- 7d. Non-Numeric Values in Numeric Columns
SELECT * FROM bronze.catalog_products
WHERE
    (ISNUMERIC(product_name_lenght) = 0        AND product_name_lenght != '')        OR
    (ISNUMERIC(product_photos_qty) = 0         AND product_photos_qty != '')         OR
    (ISNUMERIC(product_height_cm) = 0          AND product_height_cm != '')          OR
    (ISNUMERIC(product_width_cm) = 0           AND product_width_cm != '');


-- =============================================================================
-- 8. CATALOG_SELLERS
-- =============================================================================

-- 8a. NULL / Empty Check
SELECT
    COUNT(*) AS total_sellers,
    SUM(CASE WHEN seller_id = '' OR seller_id IS NULL THEN 1 ELSE 0 END)                       AS null_seller_id,
    SUM(CASE WHEN seller_zip_code_prefix = '' OR seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip,
    SUM(CASE WHEN seller_city = '' OR seller_city IS NULL THEN 1 ELSE 0 END)                   AS null_city,
    SUM(CASE WHEN seller_state = '' OR seller_state IS NULL THEN 1 ELSE 0 END)                 AS null_state
FROM bronze.catalog_sellers;

-- 8b. Seller Count by State
SELECT seller_state, COUNT(*) AS seller_count
FROM bronze.catalog_sellers
GROUP BY seller_state
ORDER BY seller_count DESC;

-- 8c. City Name Inconsistencies
-- Same zip code with different city names indicates typos
SELECT TOP 20
    seller_zip_code_prefix,
    COUNT(DISTINCT seller_city) AS unique_city_names
FROM bronze.catalog_sellers
GROUP BY seller_zip_code_prefix
HAVING COUNT(DISTINCT seller_city) > 1
ORDER BY unique_city_names DESC;


-- =============================================================================
-- 9. CATALOG_CATEGORY_TRANSLATION
-- =============================================================================

-- 9a. Missing Translations
SELECT
    product_category_name,
    product_category_name_english
FROM bronze.catalog_category_translation
WHERE product_category_name_english IS NULL
   OR product_category_name_english = '';

-- 9b. Duplicate Mapping Check (same Portuguese name mapped to multiple English names)
SELECT product_category_name, COUNT(*) AS mapping_count
FROM bronze.catalog_category_translation
GROUP BY product_category_name
HAVING COUNT(*) > 1;
