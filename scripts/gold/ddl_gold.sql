/*
===============================================================================
DDL Script - Gold Layer
===============================================================================
Script Purpose:
    This script creates the tables in the 'gold' schema (Star Schema).
    Each table is dropped if it already exists before being recreated.
    Run this script before executing the Gold load procedure.

Schema  : gold
Tables  :
    Dimensions
        - gold.dim_date
        - gold.dim_customers
        - gold.dim_products
        - gold.dim_sellers
    Facts
        - gold.fact_orders
        - gold.fact_order_reviews

Notes   :
    - Surrogate keys (INT IDENTITY) are used on all dim tables.
    - fact_orders references dim tables via surrogate keys (foreign key logic
      is enforced at the application/ETL level, not via constraints).
    - dim_date is a physical table populated by a date generation script,
      not derived from any Silver table.
    - dim_customers grain is customer_unique_id (not customer_id) —
      one row per unique customer across all orders.
    - SCD Type 0 applied — static dataset, no historization required.
    - fact_order_reviews is a separate fact table by design. Multiple reviews
      per order exist in the dataset (confirmed in Silver QC). Adding review_score
      to fact_orders would break the order-level grain or require aggregation.
      Separate fact tables allow both grains to coexist and be joined on order_id.
    - dwh_create_date audit column included on all tables.

Usage Example:
    Run this script once to initialize the Gold layer schema.
    Then execute: EXEC gold.load_gold;
===============================================================================
*/


-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- ------------------------------------------------------------
-- gold.dim_date
-- Calendar dimension covering the full Olist dataset range
-- (2016-2018). Populated via a date generation script — not
-- derived from any Silver source table.
-- Grain: one row per calendar day.
-- ------------------------------------------------------------
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO
CREATE TABLE gold.dim_date (
    date_key        INT             NOT NULL,   -- format: YYYYMMDD e.g. 20180101
    full_date       DATE            NOT NULL,
    year            INT             NOT NULL,
    quarter         INT             NOT NULL,
    month           INT             NOT NULL,
    month_name      NVARCHAR(20)    NOT NULL,
    week            INT             NOT NULL,
    day_of_month    INT             NOT NULL,
    day_of_week     INT             NOT NULL,   -- 1=Monday ... 7=Sunday
    day_name        NVARCHAR(20)    NOT NULL,
    is_weekend      BIT             NOT NULL,   -- 1=Weekend, 0=Weekday
    dwh_create_date DATETIME2       DEFAULT GETDATE()
);
GO


-- ------------------------------------------------------------
-- gold.dim_customers
-- Customer dimension. Grain is customer_unique_id — one row
-- per unique customer regardless of how many orders they placed.
-- Enriched with customer_region derived in Silver layer.
-- Source: silver.orders_customers
-- ------------------------------------------------------------
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL
    DROP TABLE gold.dim_customers;
GO
CREATE TABLE gold.dim_customers (
    customer_key            INT             NOT NULL IDENTITY(1,1),  -- surrogate key
    customer_unique_id      NVARCHAR(50)    NOT NULL,
    customer_zip_code_prefix NVARCHAR(10),
    customer_city           NVARCHAR(50),
    customer_state          NVARCHAR(10),
    customer_region         NVARCHAR(20),
    dwh_create_date         DATETIME2       DEFAULT GETDATE()
);
GO


-- ------------------------------------------------------------
-- gold.dim_products
-- Product dimension enriched with English category names.
-- Missing translations defaulted to 'unknown' in Silver.
-- Source: silver.catalog_products
-- ------------------------------------------------------------
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL
    DROP TABLE gold.dim_products;
GO
CREATE TABLE gold.dim_products (
    product_key                     INT             NOT NULL IDENTITY(1,1),  -- surrogate key
    product_id                      NVARCHAR(50)    NOT NULL,
    product_category_name           NVARCHAR(100),
    product_category_name_english   NVARCHAR(100),
    product_name_length             INT,
    product_description_length      INT,
    product_photos_qty              INT,
    product_weight_g                INT,
    product_length_cm               INT,
    product_height_cm               INT,
    product_width_cm                INT,
    dwh_create_date                 DATETIME2       DEFAULT GETDATE()
);
GO


-- ------------------------------------------------------------
-- gold.dim_sellers
-- Seller dimension with location information.
-- 2 orphan seller_ids exist in fact data with no match here —
-- documented in Silver quality checks.
-- Source: silver.catalog_sellers
-- ------------------------------------------------------------
IF OBJECT_ID('gold.dim_sellers', 'U') IS NOT NULL
    DROP TABLE gold.dim_sellers;
GO
CREATE TABLE gold.dim_sellers (
    seller_key              INT             NOT NULL IDENTITY(1,1),  -- surrogate key
    seller_id               NVARCHAR(50)    NOT NULL,
    seller_zip_code_prefix  NVARCHAR(10),
    seller_city             NVARCHAR(50),
    seller_state            NVARCHAR(10),
    dwh_create_date         DATETIME2       DEFAULT GETDATE()
);
GO


-- ============================================================
-- FACT TABLE
-- ============================================================

-- ------------------------------------------------------------
-- gold.fact_orders
-- Central fact table at order level. Joins to all four dim
-- tables via surrogate keys. Derived columns calculated at
-- load time: delivery_days, is_late, total_order_value.
-- Sources: silver.orders_orders + silver.orders_order_items
--          + silver.orders_order_payments
-- ------------------------------------------------------------
IF OBJECT_ID('gold.fact_orders', 'U') IS NOT NULL
    DROP TABLE gold.fact_orders;
GO
CREATE TABLE gold.fact_orders (
    order_key                   INT             NOT NULL IDENTITY(1,1),  -- surrogate key
    order_id                    NVARCHAR(50)    NOT NULL,
    order_item_id               INT,    
    customer_key                INT,                                      -- FK to dim_customers
    product_key                 INT,                                      -- FK to dim_products
    seller_key                  INT,                                      -- FK to dim_sellers
    date_key                    INT,                                      -- FK to dim_date (purchase date)
    order_status                NVARCHAR(50),
    order_purchase_timestamp    DATETIME2(0),
    order_approved_at           DATETIME2(0),
    order_delivered_carrier_date    DATETIME2(0),
    order_delivered_customer_date   DATETIME2(0),
    order_estimated_delivery_date   DATE,
    payment_type                NVARCHAR(MAX),
    payment_installments        INT,
    payment_value               DECIMAL(10,2),
    price                       DECIMAL(10,2),
    freight_value               DECIMAL(10,2),
    total_order_value           DECIMAL(10,2),   -- derived: price + freight_value
    delivery_days               INT,             -- derived: order_delivered_customer_date - order_purchase_timestamp
    is_late                     BIT,             -- derived: 1 if delivered after estimated date
    dwh_create_date             DATETIME2        DEFAULT GETDATE()
);
GO


-- ------------------------------------------------------------
-- gold.fact_order_reviews
-- Separate fact table for customer reviews.
--
-- Design decision: review_score was intentionally NOT added to
-- fact_orders. During Silver quality checks it was confirmed that
-- multiple reviews can exist for a single order (up to 3 in this
-- dataset). Adding review_score directly to fact_orders would
-- require aggregation (e.g. AVG or latest review) and lose detail,
-- or would break the order-level grain by producing duplicate rows.
--
-- Keeping reviews in a separate fact table preserves both grains:
--   - fact_orders grain : one row per order item
--   - fact_order_reviews grain: one row per review
-- The two facts can be joined on order_id when needed.
--
-- Grain  : one row per review (review_id)
-- Source : silver.orders_order_reviews
-- ------------------------------------------------------------
IF OBJECT_ID('gold.fact_order_reviews', 'U') IS NOT NULL
    DROP TABLE gold.fact_order_reviews;
GO
CREATE TABLE gold.fact_order_reviews (
    review_key                  INT             NOT NULL IDENTITY(1,1),  -- surrogate key
    review_id                   NVARCHAR(50)    NOT NULL,
    order_id                    NVARCHAR(50)    NOT NULL,                 -- FK to fact_orders.order_id
    date_key                    INT,                                      -- FK to dim_date (review_creation_date)
    review_score                INT,                                      -- range: 1-5
    review_comment_title        NVARCHAR(100),
    review_comment_message      NVARCHAR(MAX),
    review_creation_date        DATE,
    review_answer_timestamp     DATETIME2,
    dwh_create_date             DATETIME2       DEFAULT GETDATE()
);
GO