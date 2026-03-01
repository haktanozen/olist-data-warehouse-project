/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external Olist CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.

Parameters:
    None.

Usage Example:
    EXEC bronze.load_bronze;

-- NOTE: The table 'bronze.orders_order_reviews' was loaded manually via SSMS Import Flat File Wizard
         instead of BULK INSERT due to embedded commas and newline characters
         in review_comment_message and review_comment_title fields,
         which caused BULK INSERT to misparse row/field terminators.

===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        ------------------------------------------------
        -- ORDERS SYSTEM TABLES
        ------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT 'Loading Orders System Tables';
        PRINT '------------------------------------------------';

        -- 1. Orders
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.orders_orders';
        TRUNCATE TABLE bronze.orders_orders;
        PRINT '>> Inserting Data Into: bronze.orders_orders';
        BULK INSERT bronze.orders_orders
        FROM 'D:\olist\olist_orders_dataset.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 2. Order Items
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.orders_order_items';
        TRUNCATE TABLE bronze.orders_order_items;
        PRINT '>> Inserting Data Into: bronze.orders_order_items';
        BULK INSERT bronze.orders_order_items
        FROM 'D:\olist\olist_order_items_dataset.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 3. Order Payments
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.orders_order_payments';
        TRUNCATE TABLE bronze.orders_order_payments;
        PRINT '>> Inserting Data Into: bronze.orders_order_payments';
        BULK INSERT bronze.orders_order_payments
        FROM 'D:\olist\olist_order_payments_dataset.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        /*-- 4. Order Reviews
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.orders_order_reviews';
        TRUNCATE TABLE bronze.orders_order_reviews;
        PRINT '>> Inserting Data Into: bronze.orders_order_reviews';
        BULK INSERT bronze.orders_order_reviews
        FROM 'D:\olist\olist_order_reviews_dataset.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\r\n', FIELDQUOTE = '"', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; */

        -- 5. Customers
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.orders_customers';
        TRUNCATE TABLE bronze.orders_customers;
        PRINT '>> Inserting Data Into: bronze.orders_customers';
        BULK INSERT bronze.orders_customers
        FROM 'D:\olist\olist_customers_dataset.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        ------------------------------------------------
        -- CATALOG SYSTEM TABLES
        ------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT 'Loading Catalog System Tables';
        PRINT '------------------------------------------------';

        -- 6. Products
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.catalog_products';
        TRUNCATE TABLE bronze.catalog_products;
        PRINT '>> Inserting Data Into: bronze.catalog_products';
        BULK INSERT bronze.catalog_products
        FROM 'D:\olist\olist_products_dataset.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 7. Sellers
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.catalog_sellers';
        TRUNCATE TABLE bronze.catalog_sellers;
        PRINT '>> Inserting Data Into: bronze.catalog_sellers';
        BULK INSERT bronze.catalog_sellers
        FROM 'D:\olist\olist_sellers_dataset.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 8. Category Translation
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.catalog_category_translation';
        TRUNCATE TABLE bronze.catalog_category_translation;
        PRINT '>> Inserting Data Into: bronze.catalog_category_translation';
        BULK INSERT bronze.catalog_category_translation
        FROM 'D:\olist\product_category_name_translation.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='
    END TRY
    BEGIN CATCH
        PRINT '=========================================='
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================='
    END CATCH
END
GO