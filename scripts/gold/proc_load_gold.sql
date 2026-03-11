/*
===============================================================================
Stored Procedure: Load Gold Layer (Silver -> Gold)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL process to populate the 'gold'
    schema tables from the 'silver' schema (Star Schema).
Actions Performed:
    - Truncates Gold tables.
    - Inserts transformed and enriched data from Silver into Gold tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC gold.load_gold;
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();

        -- ======================================================
        -- 1. DIMENSIONS
        -- ======================================================

        -- dim_date: generated via recursive CTE, not sourced from Silver.
        -- Covers full Olist dataset range: 2016-01-01 to 2018-12-31.
        SET @start_time = GETDATE();
        TRUNCATE TABLE gold.dim_date;
        WITH date_cte AS (
            SELECT CAST('2016-01-01' AS DATE) AS full_date
            UNION ALL
            SELECT DATEADD(DAY, 1, full_date)
            FROM date_cte
            WHERE full_date < '2018-12-31'
        )
        INSERT INTO gold.dim_date (
            date_key,
            full_date,
            year,
            quarter,
            month,
            month_name,
            week,
            day_of_month,
            day_of_week,
            day_name,
            is_weekend
        )
        SELECT 
            CAST(FORMAT(full_date, 'yyyyMMdd') AS INT),
            full_date,
            YEAR(full_date),
            DATEPART(QUARTER, full_date),
            MONTH(full_date),
            DATENAME(MONTH, full_date),
            DATEPART(WEEK, full_date),
            DAY(full_date),
            DATEPART(WEEKDAY, full_date),
            DATENAME(WEEKDAY, full_date),
            CASE WHEN DATEPART(WEEKDAY, full_date) IN (1,7) THEN 1 ELSE 0 END
        FROM date_cte
        OPTION (MAXRECURSION 0);
        SET @end_time = GETDATE();
        PRINT '>> dim_date Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';
        PRINT '=================================================='


        -- dim_customers: grain is customer_unique_id.
        -- Same customer_unique_id can have multiple addresses in Silver (different
        -- zip codes/cities across orders). DISTINCT alone is insufficient.
        -- ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp DESC)
        -- selects the most recent address per customer. rn = 1 ensures one row per customer.
        SET @start_time = GETDATE();
            WITH latest_customers AS (
                SELECT 
                    oc.customer_unique_id,
                    oc.customer_zip_code_prefix,
                    oc.customer_city,
                    oc.customer_state,
                    oc.customer_region,
                    ROW_NUMBER() OVER (
                        PARTITION BY oc.customer_unique_id 
                        ORDER BY o.order_purchase_timestamp DESC
                    ) AS rn
                FROM silver.orders_customers oc
                JOIN silver.orders_orders o 
                    ON oc.customer_id = o.customer_id
            )
            INSERT INTO gold.dim_customers (           
                customer_unique_id,      
                customer_zip_code_prefix,
                customer_city,           
                customer_state,          
                customer_region         
            )
            SELECT 
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state,
                customer_region
            FROM latest_customers
            WHERE rn = 1;
        SET @end_time = GETDATE();
        PRINT '>> dim_customers Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';
        PRINT '=================================================='


        -- dim_sellers: direct load from silver.catalog_sellers.
        -- No deduplication needed — seller_id is already unique in Silver.
        SET @start_time = GETDATE();
        TRUNCATE TABLE gold.dim_sellers;
        INSERT INTO gold.dim_sellers (                    
            seller_id,         
            seller_zip_code_prefix,
            seller_city,          
            seller_state                
        )
        SELECT
            seller_id,         
            seller_zip_code_prefix,
            seller_city,          
            seller_state   
        FROM silver.catalog_sellers;
        SET @end_time = GETDATE();
        PRINT '>> dim_sellers Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';
        PRINT '=================================================='


        -- dim_products: enriched with English category name via LEFT JOIN.
        -- Missing translations default to 'unknown' via ISNULL.
        SET @start_time = GETDATE();
        TRUNCATE TABLE gold.dim_products;
        INSERT INTO gold.dim_products (                    
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
            cp.product_id,                   
            cp.product_category_name,        
            ISNULL(ct.product_category_name_english, 'unknown'),
            cp.product_name_length,          
            cp.product_description_length,   
            cp.product_photos_qty,           
            cp.product_weight_g,             
            cp.product_length_cm,            
            cp.product_height_cm,            
            cp.product_width_cm              
        FROM silver.catalog_products cp
        LEFT JOIN silver.catalog_category_translation ct
            ON cp.product_category_name = ct.product_category_name;
        SET @end_time = GETDATE();
        PRINT '>> dim_products Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';
        PRINT '=================================================='


        -- ======================================================
        -- 2. FACTS
        -- ======================================================

        -- fact_order_reviews: separate fact table to preserve grain.
        -- Multiple reviews per order confirmed in Silver QC.
        -- date_key derived via JOIN to dim_date on review_creation_date.
        SET @start_time = GETDATE();
        TRUNCATE TABLE gold.fact_order_reviews;
        INSERT INTO gold.fact_order_reviews (                    
            review_id,              
            order_id,               
            date_key,              
            review_score,           
            review_comment_title,   
            review_comment_message, 
            review_creation_date,   
            review_answer_timestamp
        )
        SELECT
            r.review_id,              
            r.order_id,               
            d.date_key,              
            r.review_score,           
            r.review_comment_title,  
            r.review_comment_message, 
            r.review_creation_date,   
            r.review_answer_timestamp
        FROM silver.orders_order_reviews r
        LEFT JOIN gold.dim_date d 
            ON d.full_date = r.review_creation_date;
        SET @end_time = GETDATE();
        PRINT '>> fact_order_reviews Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';
        PRINT '=================================================='


        -- fact_orders: grain is order_id + order_item_id (one row per order line item).
        -- payment_distinct CTE: deduplicates payment_type per order before aggregation.
        -- payments CTE: aggregates payment data to order level.
        -- Derived columns: total_order_value, delivery_days, is_late.
        -- customer_key resolved via two-step JOIN through orders_customers.
        -- payment_installments = 0 handled as 1 (source data issue, documented in Silver).
        -- is_late = NULL for undelivered orders (not 0) to avoid misleading aggregations.
        SET @start_time = GETDATE();
        TRUNCATE TABLE gold.fact_orders;
        WITH payment_distinct AS (
            SELECT DISTINCT 
                order_id, 
                payment_type
            FROM silver.orders_order_payments
        ),
        payments AS (
            SELECT 
                op.order_id,
                STRING_AGG(pd.payment_type, ', ')              AS payment_type,
                SUM(CASE WHEN op.payment_installments = 0 THEN 1 
                         ELSE op.payment_installments END)     AS payment_installments,
                SUM(op.payment_value)                          AS payment_value
            FROM silver.orders_order_payments op
            JOIN payment_distinct pd
                ON op.order_id = pd.order_id
                AND op.payment_type = pd.payment_type
            GROUP BY op.order_id
        )
        INSERT INTO gold.fact_orders (                    
            order_id,                   
            order_item_id,
            customer_key,               
            product_key,                
            seller_key,                 
            date_key,                   
            order_status,               
            order_purchase_timestamp,   
            order_approved_at,          
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            payment_type,               
            payment_installments,       
            payment_value,              
            price,                      
            freight_value,              
            total_order_value,          
            delivery_days,              
            is_late            
        )
        SELECT
            o.order_id,
            oi.order_item_id,
            dc.customer_key,
            dp.product_key,                
            ds.seller_key,                 
            dd.date_key,                   
            o.order_status,               
            o.order_purchase_timestamp,   
            o.order_approved_at,          
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            p.payment_type,               
            p.payment_installments,       
            p.payment_value,              
            oi.price,                      
            oi.freight_value,              
            ISNULL(oi.price, 0) + ISNULL(oi.freight_value, 0)          AS total_order_value,          
            DATEDIFF(DAY, o.order_purchase_timestamp, 
                     o.order_delivered_customer_date)                   AS delivery_days,              
            CASE 
                WHEN o.order_delivered_customer_date IS NULL THEN NULL
                WHEN o.order_delivered_customer_date > 
                     o.order_estimated_delivery_date THEN 1 
                ELSE 0 
            END                                                         AS is_late            
        FROM silver.orders_orders o
        LEFT JOIN payments p
            ON o.order_id = p.order_id
        LEFT JOIN silver.orders_order_items oi
            ON o.order_id = oi.order_id
        LEFT JOIN silver.orders_customers c
            ON o.customer_id = c.customer_id
        LEFT JOIN gold.dim_customers dc
            ON c.customer_unique_id = dc.customer_unique_id
        LEFT JOIN gold.dim_products dp
            ON oi.product_id = dp.product_id
        LEFT JOIN gold.dim_sellers ds
            ON oi.seller_id = ds.seller_id
        LEFT JOIN gold.dim_date dd
            ON dd.full_date = CAST(o.order_purchase_timestamp AS DATE);
        SET @end_time = GETDATE();
        PRINT '>> fact_orders Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';
        PRINT '=================================================='


        SET @batch_end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Loading Gold Layer Completed in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 's';
        PRINT '=========================================='

    END TRY
    BEGIN CATCH
        PRINT '=========================================='
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '=========================================='
    END CATCH
END
GO
