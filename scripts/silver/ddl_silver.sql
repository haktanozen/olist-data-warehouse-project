/*
===============================================================================
DDL Script - Silver Layer
===============================================================================
Script Purpose:
    This script creates the tables in the 'silver' schema.
    Each table is dropped if it already exists before being recreated.
    Run this script before executing the Silver load procedure.

Schema  : silver
Tables  :
    Catalog System
        - silver.catalog_category_translation
        - silver.catalog_sellers
        - silver.catalog_products
    Orders System
        - silver.orders_customers
        - silver.orders_orders
        - silver.orders_order_items
        - silver.orders_order_payments
        - silver.orders_order_reviews

Notes   :
    - All tables include a 'dwh_create_date' audit column (DATETIME2, DEFAULT GETDATE()).
    - No primary key constraints are defined at this layer. Data integrity
      is validated via quality_checks_silver.sql after each load.
    - String columns use NVARCHAR to support Unicode characters present
      in Brazilian city and category names.

Usage Example:
    Run this script once to initialize the Silver layer schema.
    Then execute: EXEC silver.load_silver;
===============================================================================
*/


-- ============================================================
-- CATALOG SYSTEM TABLES
-- ============================================================

-- ------------------------------------------------------------
-- silver.catalog_category_translation
-- Maps Portuguese product category names to English equivalents.
-- Source: bronze.catalog_category_translation
-- ------------------------------------------------------------
IF OBJECT_ID('silver.catalog_category_translation', 'U') IS NOT NULL
    DROP TABLE silver.catalog_category_translation;
GO
CREATE TABLE silver.catalog_category_translation (
    product_category_name           NVARCHAR(100),
    product_category_name_english   NVARCHAR(100),
    dwh_create_date                 DATETIME2 DEFAULT GETDATE()
);
GO


-- ------------------------------------------------------------
-- silver.catalog_sellers
-- Seller location information. City normalized to lowercase,
-- state to uppercase. zip_code_prefix stored as NVARCHAR
-- to preserve leading zeros.
-- Source: bronze.catalog_sellers
-- ------------------------------------------------------------
IF OBJECT_ID('silver.catalog_sellers', 'U') IS NOT NULL
    DROP TABLE silver.catalog_sellers;
GO
CREATE TABLE silver.catalog_sellers (
    seller_id               NVARCHAR(50),
    seller_zip_code_prefix  NVARCHAR(10),
    seller_city             NVARCHAR(50),
    seller_state            NVARCHAR(10),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO


-- ------------------------------------------------------------
-- silver.catalog_products
-- Product catalog enriched with English category names via
-- LEFT JOIN to catalog_category_translation. Missing translations
-- default to 'unknown'. Numeric measurement columns cast to INT.
-- Source: bronze.catalog_products
-- ------------------------------------------------------------
IF OBJECT_ID('silver.catalog_products', 'U') IS NOT NULL
    DROP TABLE silver.catalog_products;
GO
CREATE TABLE silver.catalog_products (
    product_id                      NVARCHAR(50),
    product_category_name           NVARCHAR(100),
    product_category_name_english   NVARCHAR(100),
    product_name_length             INT,
    product_description_length      INT,
    product_photos_qty              INT,
    product_weight_g                INT,
    product_length_cm               INT,
    product_height_cm               INT,
    product_width_cm                INT,
    dwh_create_date                 DATETIME2 DEFAULT GETDATE()
);
GO


-- ============================================================
-- ORDERS SYSTEM TABLES
-- ============================================================

-- ------------------------------------------------------------
-- silver.orders_customers
-- Customer records with derived customer_region column mapping
-- all 27 Brazilian state codes to five geographic regions.
-- Source: bronze.orders_customers
-- ------------------------------------------------------------
IF OBJECT_ID('silver.orders_customers', 'U') IS NOT NULL
    DROP TABLE silver.orders_customers;
GO
CREATE TABLE silver.orders_customers (
    customer_id                 NVARCHAR(50),
    customer_unique_id          NVARCHAR(50),
    customer_zip_code_prefix    NVARCHAR(10),
    customer_city               NVARCHAR(50),
    customer_state              NVARCHAR(10),
    customer_region             NVARCHAR(20),
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO


-- ------------------------------------------------------------
-- silver.orders_orders
-- Core order records. Timestamp columns cast to DATETIME2(0).
-- order_estimated_delivery_date cast to DATE (no time component).
-- 603 'unavailable' orders with no order_items are expected behavior.
-- Source: bronze.orders_orders
-- ------------------------------------------------------------
IF OBJECT_ID('silver.orders_orders', 'U') IS NOT NULL
    DROP TABLE silver.orders_orders;
GO
CREATE TABLE silver.orders_orders (
    order_id                        NVARCHAR(50),
    customer_id                     NVARCHAR(50),
    order_status                    NVARCHAR(50),
    order_purchase_timestamp        DATETIME2(0),
    order_approved_at               DATETIME2(0),
    order_delivered_carrier_date    DATETIME2(0),
    order_delivered_customer_date   DATETIME2(0),
    order_estimated_delivery_date   DATE,
    dwh_create_date                 DATETIME2 DEFAULT GETDATE()
);
GO


-- ------------------------------------------------------------
-- silver.orders_order_items
-- Line items per order. price and freight_value cast to
-- DECIMAL(10,2). 3 orphan seller_ids (no match in catalog_sellers)
-- are intentionally retained — documented in load_silver header.
-- Source: bronze.orders_order_items
-- ------------------------------------------------------------
IF OBJECT_ID('silver.orders_order_items', 'U') IS NOT NULL
    DROP TABLE silver.orders_order_items;
GO
CREATE TABLE silver.orders_order_items (
    order_id                NVARCHAR(50),
    order_item_id           INT,
    product_id              NVARCHAR(50),
    seller_id               NVARCHAR(50),
    shipping_limit_date     DATETIME2(0),
    price                   DECIMAL(10,2),
    freight_value           DECIMAL(10,2),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO


-- ------------------------------------------------------------
-- silver.orders_order_payments
-- Payment records per order. payment_type 'not_defined'
-- normalized to 'n/a' during load. One order can have
-- multiple payment rows (payment_sequential distinguishes them).
-- Source: bronze.orders_order_payments
-- ------------------------------------------------------------
IF OBJECT_ID('silver.orders_order_payments', 'U') IS NOT NULL
    DROP TABLE silver.orders_order_payments;
GO
CREATE TABLE silver.orders_order_payments (
    order_id                NVARCHAR(50),
    payment_sequential      INT,
    payment_type            NVARCHAR(50),
    payment_installments    INT,
    payment_value           DECIMAL(10,2),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO


-- ------------------------------------------------------------
-- silver.orders_order_reviews
-- Customer reviews. Loaded via SSMS Flat File Wizard in Bronze
-- (BULK INSERT failed due to embedded commas/newlines in comment
-- fields). review_score cast to INT; invalid values become NULL.
-- Date columns include CHAR(13)/CHAR(10) cleanup during load
-- as a defensive measure against Flat File Wizard artifacts.
-- Source: bronze.orders_order_reviews
-- ------------------------------------------------------------
IF OBJECT_ID('silver.orders_order_reviews', 'U') IS NOT NULL
    DROP TABLE silver.orders_order_reviews;
GO
CREATE TABLE silver.orders_order_reviews (
    review_id                   NVARCHAR(50),
    order_id                    NVARCHAR(50),
    review_score                INT,
    review_comment_title        NVARCHAR(100),
    review_comment_message      NVARCHAR(MAX),
    review_creation_date        DATE,
    review_answer_timestamp     DATETIME2,
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO
