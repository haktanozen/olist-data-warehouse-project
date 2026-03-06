/*
================================================================================
STORED PROCEDURE : silver.load_silver
DATABASE         : OlistDW
SCHEMA           : silver
CREATED BY       : Hako
================================================================================

SCRIPT PURPOSE
--------------
This stored procedure performs the ETL (Extract, Transform, Load) process to
populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;

--------------------------------------------------------------------------------

OVERVIEW
--------
This procedure loads and transforms data from the Bronze layer into the Silver
layer following the Medallion Architecture pattern. It uses a full-load strategy
(TRUNCATE + INSERT) on every execution. No incremental or delta logic is applied,
as the Olist dataset is static (SCD Type 0).

TABLES LOADED (in order)
-------------------------
 1. silver.catalog_category_translation
 2. silver.catalog_sellers
 3. silver.catalog_products
 4. silver.orders_customers
 5. silver.orders_orders
 6. silver.orders_order_items
 7. silver.orders_order_payments
 8. silver.orders_order_reviews

TRANSFORMATIONS APPLIED
-----------------------
 - TRIM()                  : Removes leading/trailing whitespace from all string columns
 - REPLACE(col, '"', '')   : Strips embedded double-quote characters introduced
                             during CSV ingestion via BULK INSERT or SSMS Flat File Wizard
 - NULLIF(col, '')         : Converts empty strings to NULL for semantic correctness
 - TRY_CAST()              : Safely casts string columns to target data types (INT,
                             DECIMAL, DATE, DATETIME2). Returns NULL on failure instead
                             of raising an error — avoids row rejections on dirty data
 - LOWER() / UPPER()       : Standardizes city names to lowercase, state codes to uppercase
 - ISNULL(col, 'unknown')  : Fills missing category translations with a default value
 - CASE (region mapping)   : Derives customer_region from customer_state using
                             Brazil's five official geographic regions
 - REPLACE(payment_type, 'not_defined', 'n/a')
                           : Normalizes ambiguous payment type label to 'n/a'
 - CHAR(13) / CHAR(10) cleanup on date columns of orders_order_reviews:
                             Flat File Wizard (used as a workaround for BULK INSERT
                             failure on this table) can inject carriage-return (\r)
                             and line-feed (\n) characters into the last column of
                             each row. These invisible characters cause TRY_CAST to
                             return NULL silently. Explicit REPLACE for CHAR(13) and
                             CHAR(10) is applied defensively on both date columns.

KNOWN DATA QUALITY FINDINGS (documented, not fixed here)
---------------------------------------------------------
 - 3 orphan seller_id values exist in bronze.orders_order_items with no matching
   record in bronze.catalog_sellers. These are intentionally retained in Silver
   as-is for traceability. They will be handled at the Gold/Mart layer if needed.
 - bronze.orders_order_reviews could not be loaded via BULK INSERT due to embedded
   commas and newline characters in free-text comment fields. It was loaded manually
   using the SSMS Import Flat File Wizard as a one-time workaround.
 - 603 orders with status 'unavailable' have no corresponding rows in
   orders_order_items. This is confirmed expected business behavior, not a data error.

TEST RESULTS (pre-load validation queries)
------------------------------------------
 - review_score       : All values in range 1–5 after TRY_CAST. No unexpected NULLs.
 - order_id           : Embedded quotes successfully removed via REPLACE.
 - review_answer_timestamp : No NULL inflation observed after CHAR(13)/CHAR(10)
                             cleanup — data was already clean, but cleanup retained
                             as a defensive measure for production robustness.
 - payment_type       : 'not_defined' values normalized to 'n/a' before load.
 - customer_region    : All 27 Brazilian state codes mapped to one of five regions.
                        No 'Unknown' values detected in test run.

LOAD STRATEGY
-------------
 - Pattern     : TRUNCATE + INSERT (full reload on every run)
 - Trigger     : Manual / scheduled batch
 - SCD Type    : Type 0 (static dataset, no historization required)
 - Error handling : TRY/CATCH block wraps entire procedure. On failure, error
                    message, number, and state are printed to the console.
================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, 
            @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        ------------------------------------------------
        -- CATALOG SYSTEM TABLES
        ------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT 'Loading Catalog System Tables';
        PRINT '------------------------------------------------';

        -- 1. catalog_category_translation
        -- Transformation: TRIM on both columns to remove whitespace.
        -- No type casting needed — all values are plain strings.
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.catalog_category_translation';
        TRUNCATE TABLE silver.catalog_category_translation;
        PRINT '>> Inserting Data Into: silver.catalog_category_translation';
        INSERT INTO silver.catalog_category_translation (
            product_category_name,
            product_category_name_english
        )
        SELECT
            TRIM(product_category_name),
            TRIM(product_category_name_english)
        FROM bronze.catalog_category_translation;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- 2. catalog_sellers
        -- Transformation: TRIM + REPLACE to remove embedded quotes from seller_id
        -- and zip code (introduced during CSV ingestion). City normalized to lowercase.
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.catalog_sellers';
        TRUNCATE TABLE silver.catalog_sellers;
        PRINT '>> Inserting Data Into: silver.catalog_sellers';
        INSERT INTO silver.catalog_sellers (
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        )
        SELECT
            REPLACE(TRIM(seller_id), '"', ''),
            REPLACE(TRIM(seller_zip_code_prefix), '"', ''),
            LOWER(TRIM(seller_city)),
            TRIM(seller_state)
        FROM bronze.catalog_sellers;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- 3. catalog_products
        -- Transformation: TRIM + REPLACE on product_id. TRY_CAST on all numeric
        -- measurement columns (stored as NVARCHAR in Bronze) — invalid values become NULL.
        -- Defensive guard: all numeric measurement columns — negative values
        -- converted to NULL (CASE WHEN < 0 THEN NULL). No negatives in current dataset
        -- but guard ensures correctness if source data changes.
        -- LEFT JOIN to category_translation to enrich with English category name;
        -- missing translations default to 'unknown' via ISNULL().
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.catalog_products';
        TRUNCATE TABLE silver.catalog_products;
        PRINT '>> Inserting Data Into: silver.catalog_products';
        INSERT INTO silver.catalog_products (
            product_id,
            product_category_name,
            product_category_name_english,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        )
        SELECT
            REPLACE(TRIM(product_id), '"', ''),
            TRIM(p.product_category_name),
            ISNULL(t.product_category_name_english, 'unknown'),
            CASE WHEN TRY_CAST(NULLIF(product_name_lenght, '') AS INT) < 0 THEN NULL
                 ELSE TRY_CAST(NULLIF(product_name_lenght, '') AS INT) END,
            CASE WHEN TRY_CAST(NULLIF(product_description_lenght, '') AS INT) < 0 THEN NULL
                 ELSE TRY_CAST(NULLIF(product_description_lenght, '') AS INT) END,
            CASE WHEN TRY_CAST(NULLIF(product_photos_qty, '') AS INT) < 0 THEN NULL
                 ELSE TRY_CAST(NULLIF(product_photos_qty, '') AS INT) END,
            CASE WHEN TRY_CAST(NULLIF(product_weight_g, '') AS INT) < 0 THEN NULL
                 ELSE TRY_CAST(NULLIF(product_weight_g, '') AS INT) END,
            CASE WHEN TRY_CAST(NULLIF(product_length_cm, '') AS INT) < 0 THEN NULL
                 ELSE TRY_CAST(NULLIF(product_length_cm, '') AS INT) END,
            CASE WHEN TRY_CAST(NULLIF(product_height_cm, '') AS INT) < 0 THEN NULL
                 ELSE TRY_CAST(NULLIF(product_height_cm, '') AS INT) END,
            CASE WHEN TRY_CAST(NULLIF(product_width_cm, '') AS INT) < 0 THEN NULL
                 ELSE TRY_CAST(NULLIF(product_width_cm, '') AS INT) END
        FROM bronze.catalog_products p
        LEFT JOIN bronze.catalog_category_translation t
            ON TRIM(p.product_category_name) = TRIM(t.product_category_name);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- 4. orders_customers
        -- Transformation: TRIM + REPLACE on ID and zip code columns.
        -- City lowercased, state uppercased for consistency.
        -- customer_region derived via CASE mapping all 27 Brazilian state codes
        -- to Brazil's five official geographic regions (Southeast, South, Northeast,
        -- Central-West, North). No 'Unknown' values expected for this dataset.
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.orders_customers';
        TRUNCATE TABLE silver.orders_customers;
        PRINT '>> Inserting Data Into: silver.orders_customers';
        INSERT INTO silver.orders_customers (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            customer_region
        )
        SELECT
            REPLACE(TRIM(customer_id), '"', ''),
            REPLACE(TRIM(customer_unique_id), '"', ''),
            REPLACE(TRIM(customer_zip_code_prefix), '"', ''),
            LOWER(TRIM(customer_city)),
            UPPER(TRIM(customer_state)),
            CASE
                WHEN UPPER(TRIM(customer_state)) IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
                WHEN UPPER(TRIM(customer_state)) IN ('PR', 'RS', 'SC') THEN 'South'
                WHEN UPPER(TRIM(customer_state)) IN ('BA', 'CE', 'PE', 'RN', 'PB', 'AL', 'SE', 'PI', 'MA') THEN 'Northeast'
                WHEN UPPER(TRIM(customer_state)) IN ('DF', 'GO', 'MT', 'MS') THEN 'Central-West'
                WHEN UPPER(TRIM(customer_state)) IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO') THEN 'North'
                ELSE 'Unknown'
            END
        FROM bronze.orders_customers; 

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- 5. orders_orders
        -- Transformation: TRIM + REPLACE on order_id and customer_id (embedded quotes
        -- confirmed during Bronze QA). NULLIF on order_status for empty string handling.
        -- TRY_CAST to DATETIME2(0) on all timestamp columns.
        -- Note: order_estimated_delivery_date cast to DATE (no time component needed).
        -- 603 'unavailable' orders have no order_items — confirmed expected behavior.
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.orders_orders';
        TRUNCATE TABLE silver.orders_orders;
        PRINT '>> Inserting Data Into: silver.orders_orders';
        INSERT INTO silver.orders_orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        )
        SELECT
            REPLACE(TRIM(order_id), '"', ''), 
		    REPLACE(TRIM(customer_id), '"', ''), 
		    NULLIF(TRIM(order_status), ''), 
		    TRY_CAST(NULLIF(order_purchase_timestamp, '') AS DATETIME2(0)), 
		    TRY_CAST(NULLIF(order_approved_at, '') AS DATETIME2(0)) AS order_approved_at, 
		    TRY_CAST(NULLIF(order_delivered_carrier_date, '') AS DATETIME2(0)), 
		    TRY_CAST(NULLIF(order_delivered_customer_date, '') AS DATETIME2(0)), 
		    TRY_CAST(NULLIF(order_estimated_delivery_date, '') AS DATE) 
        FROM bronze.orders_orders; 

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- 6. orders_order_items
        -- Transformation: TRIM + REPLACE + NULLIF on ID columns.
        -- TRY_CAST to INT for order_item_id, DATETIME2(0) for shipping_limit_date,
        -- DECIMAL(10,2) for price and freight_value.
        -- Note: 3 orphan seller_id values (no match in catalog_sellers) are loaded
        -- as-is. This is intentional — see known data quality findings in header.
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.orders_order_items';
        TRUNCATE TABLE silver.orders_order_items;
        PRINT '>> Inserting Data Into: silver.orders_order_items';
        INSERT INTO silver.orders_order_items (
           order_id,
           order_item_id,
           product_id,
           seller_id,
           shipping_limit_date,
           price,
           freight_value
        )
        SELECT
           NULLIF(TRIM(REPLACE(order_id,'"','')),''), 
           TRY_CAST(NULLIF(order_item_id,'') AS INT),
           NULLIF(TRIM(REPLACE(product_id, '"', '')), ''),
           NULLIF(TRIM(REPLACE(seller_id, '"', '')), ''),
           TRY_CAST(NULLIF(shipping_limit_date, '') AS DATETIME2(0)), 
           TRY_CAST(NULLIF(price,'') AS DECIMAL(10,2)),
           TRY_CAST(NULLIF(freight_value,'') AS DECIMAL(10,2))
        FROM bronze.orders_order_items; 

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
       

        -- 7. orders_order_payments
        -- Transformation: TRIM + REPLACE + NULLIF on all columns.
        -- payment_type 'not_defined' normalized to 'n/a' for clarity.
        -- TRY_CAST to INT for sequential and installments, DECIMAL(10,2) for value.
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.orders_order_payments';
        TRUNCATE TABLE silver.orders_order_payments;
        PRINT '>> Inserting Data Into: silver.orders_order_payments';
        INSERT INTO silver.orders_order_payments (
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        )
        SELECT
           NULLIF(TRIM(REPLACE(order_id,'"','')),''),
           TRY_CAST(NULLIF(TRIM(REPLACE(payment_sequential,'"','')),'') AS INT),
           NULLIF(TRIM(REPLACE(REPLACE(payment_type,'not_defined','n/a'),'"','')),''),
           TRY_CAST(NULLIF(TRIM(REPLACE(payment_installments,'"','')),'') AS INT),
           TRY_CAST(NULLIF(TRIM(REPLACE(payment_value,'"','')),'') AS DECIMAL(10,2))
        FROM bronze.orders_order_payments; 

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- 8. orders_order_reviews
        -- Special case: this table could not be loaded via BULK INSERT in Bronze due
        -- to embedded commas and newlines in free-text comment fields. It was loaded
        -- manually via SSMS Import Flat File Wizard as a one-time workaround.
        -- Transformation: TRIM + REPLACE + NULLIF on all string columns.
        -- review_score cast to INT via TRY_CAST — handles any non-numeric values safely.
        -- review_creation_date cast to DATE (no time component needed).
        -- review_answer_timestamp cast to DATETIME2(0).
        -- Both date columns include explicit CHAR(13) and CHAR(10) cleanup:
        --   Flat File Wizard can inject carriage-return (\r = CHAR(13)) and
        --   line-feed (\n = CHAR(10)) into the last column of each CSV row.
        --   TRY_CAST would silently return NULL for affected rows without this cleanup.
        --   Test result: no NULL inflation observed — data was already clean.
        --   Cleanup retained as a defensive measure consistent with production standards.
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.orders_order_reviews';
        TRUNCATE TABLE silver.orders_order_reviews;
        PRINT '>> Inserting Data Into: silver.orders_order_reviews';
        INSERT INTO silver.orders_order_reviews (
           review_id,
           order_id,
           review_score,
           review_comment_title,
           review_comment_message,
           review_creation_date,
           review_answer_timestamp
        )
        SELECT
           NULLIF(TRIM(REPLACE(review_id,'"','')),''),
           NULLIF(TRIM(REPLACE(order_id,'"','')),''),
           CASE WHEN TRY_CAST(NULLIF(TRIM(REPLACE(review_score,'"','')),'') AS INT) 
           BETWEEN 1 AND 5 
           THEN TRY_CAST(NULLIF(TRIM(REPLACE(review_score,'"','')),'') AS INT)
           ELSE NULL 
           END,
           NULLIF(TRIM(REPLACE(review_comment_title,'"','')),''),
           NULLIF(TRIM(REPLACE(review_comment_message,'"','')),''),
           TRY_CAST(NULLIF(TRIM(REPLACE(REPLACE(REPLACE(review_creation_date, '"', ''), CHAR(13), ''), CHAR(10), '')), '') AS DATE),
           TRY_CAST(NULLIF(TRIM(REPLACE(REPLACE(REPLACE(review_answer_timestamp, '"', ''), CHAR(13), ''), CHAR(10), '')), '') AS DATETIME2(0))
        FROM bronze.orders_order_reviews; 

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

       
        SET @batch_end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='
    END TRY
    BEGIN CATCH
        PRINT '=========================================='
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================='
    END CATCH
END
GO

