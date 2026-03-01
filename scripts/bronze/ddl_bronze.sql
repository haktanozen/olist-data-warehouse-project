/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
Design Decisions:
    - All columns are defined as NVARCHAR to preserve raw data exactly as-is.
    - No type casting, no business rules, no data quality enforcement.
    - NULL values, typos, inconsistent formats are intentionally kept intact.
    - Type conversions and data quality rules are handled in the Silver layer.
Source Systems:
    - Orders System  : olist_orders, order_items, order_payments,
                       order_reviews, customers
    - Catalog System : products, sellers, category_name_translation
Load Method:
    - BULK INSERT via stored procedure bronze.load_bronze
    - Exception: orders_order_reviews loaded manually via
                 SSMS Import Flat File Wizard due to embedded
                 commas and newline characters in comment fields.
Usage:
    Run this script once before executing bronze.load_bronze.
    Re-running this script will DROP and recreate all bronze tables.
    WARNING: All existing data in bronze tables will be lost.
===============================================================================
*/

IF OBJECT_ID('bronze.orders_orders', 'U') IS NOT NULL
    DROP TABLE bronze.orders_orders;
GO

CREATE TABLE bronze.orders_orders (
    order_id                        NVARCHAR(50),
    customer_id                     NVARCHAR(50),
    order_status                    NVARCHAR(50),
    order_purchase_timestamp        NVARCHAR(50),
    order_approved_at               NVARCHAR(50),
    order_delivered_carrier_date    NVARCHAR(50),
    order_delivered_customer_date   NVARCHAR(50),
    order_estimated_delivery_date   NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.orders_order_reviews', 'U') IS NOT NULL
    DROP TABLE bronze.orders_order_reviews;
GO

CREATE TABLE bronze.orders_order_reviews (
    review_id                        NVARCHAR(50),
    order_id                         NVARCHAR(50),
    review_score                     NVARCHAR(50),
    review_comment_title             NVARCHAR(100),
    review_comment_message           NVARCHAR(MAX),
    review_creation_date             NVARCHAR(50),
    review_answer_timestamp          NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.orders_order_payments', 'U') IS NOT NULL
    DROP TABLE bronze.orders_order_payments;
GO

CREATE TABLE bronze.orders_order_payments (
    order_id                         NVARCHAR(50),
    payment_sequential               NVARCHAR(50),
    payment_type                     NVARCHAR(50),
    payment_installments             NVARCHAR(50),
    payment_value                    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.orders_order_items', 'U') IS NOT NULL
    DROP TABLE bronze.orders_order_items;
GO

CREATE TABLE bronze.orders_order_items (
    order_id                         NVARCHAR(50),
    order_item_id                    NVARCHAR(50),
    product_id                       NVARCHAR(50),
    seller_id                        NVARCHAR(50),
    shipping_limit_date              NVARCHAR(50),
    price                            NVARCHAR(50),
    freight_value                    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.orders_customers', 'U') IS NOT NULL
    DROP TABLE bronze.orders_customers;
GO

CREATE TABLE bronze.orders_customers (
    customer_id                         NVARCHAR(50),
    customer_unique_id                  NVARCHAR(50),
    customer_zip_code_prefix            NVARCHAR(10),
    customer_city                       NVARCHAR(100),
    customer_state                      NVARCHAR(10)
);
GO

IF OBJECT_ID('bronze.catalog_products', 'U') IS NOT NULL
    DROP TABLE bronze.catalog_products;
GO

CREATE TABLE bronze.catalog_products (
    product_id                          NVARCHAR(50),
    product_category_name               NVARCHAR(100),
    product_name_lenght                 NVARCHAR(50),
    product_description_lenght          NVARCHAR(50),
    product_photos_qty                  NVARCHAR(50),
    product_weight_g                    NVARCHAR(50),
    product_length_cm                   NVARCHAR(50),
    product_height_cm                   NVARCHAR(50),
    product_width_cm                    NVARCHAR(50)
);
GO


IF OBJECT_ID('bronze.catalog_sellers', 'U') IS NOT NULL 
   DROP TABLE bronze.catalog_sellers;

GO

CREATE TABLE bronze.catalog_sellers (
    seller_id               NVARCHAR(50),
    seller_zip_code_prefix  NVARCHAR(50),
    seller_city             NVARCHAR(100),
    seller_state            NVARCHAR(10)
);


IF OBJECT_ID('bronze.catalog_category_translation', 'U') IS NOT NULL 
   DROP TABLE bronze.catalog_category_translation;

GO

CREATE TABLE bronze.catalog_category_translation (
    product_category_name          NVARCHAR(100),
    product_category_name_english  NVARCHAR(100)
);