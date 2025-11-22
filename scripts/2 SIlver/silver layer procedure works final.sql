CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        ---------------------------------------------------------------------
        -- Timing Variables
        ---------------------------------------------------------------------
        DECLARE 
            @start_time        DATETIME2(3),
            @end_time          DATETIME2(3),
            @batch_start_time  DATETIME2(3),
            @batch_end_time    DATETIME2(3),
            @elapsed_ms        BIGINT,
            @batch_elapsed_ms  BIGINT,
            @rc                INT;   -- row count

        ---------------------------------------------------------------------
        -- Start Batch
        ---------------------------------------------------------------------
        SET @batch_start_time = SYSDATETIME();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM + ERP Tables into SILVER';
        PRINT '------------------------------------------------';



        /**************************************************************
         *  silver.crm_cust_info
         **************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';

        INSERT INTO silver.crm_cust_info
        (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE WHEN UPPER(TRIM(t.cst_marital_status)) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM(t.cst_marital_status)) = 'M' THEN 'Married'
                 ELSE 'n/a' END,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                 ELSE 'n/a' END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
        ) t
        WHERE t.flag_last = 1
          AND t.cst_id IS NOT NULL;

        SET @rc = @@ROWCOUNT;
        PRINT '>> silver.crm_cust_info Rows Inserted: ' + CAST(@rc AS NVARCHAR(20));

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND,@start_time,@end_time);
        PRINT '>> silver.crm_cust_info Load Duration: ' 
              + CAST(@elapsed_ms/1000.0 AS NVARCHAR(30)) + ' seconds (' 
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

        print(' ')

        /**************************************************************
         *  silver.crm_prd_info
         **************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';

        INSERT INTO silver.crm_prd_info
        (
            prd_id, cat_id, prd_key, prd_nm,
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
            SUBSTRING(prd_key,7,LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost,0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a' END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
        FROM bronze.crm_prd_info;

        SET @rc = @@ROWCOUNT;
        PRINT '>> silver.crm_prd_info Rows Inserted: ' + CAST(@rc AS NVARCHAR(20));

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND,@start_time,@end_time);
        PRINT '>> silver.crm_prd_info Load Duration: ' 
              + CAST(@elapsed_ms/1000.0 AS NVARCHAR(30)) + ' seconds (' 
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

 print(' ')

        /**************************************************************
         *  silver.crm_sales_details
         **************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';

        INSERT INTO silver.crm_sales_details
        (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE) END,
            CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE),
            CAST(CAST(sls_due_dt  AS VARCHAR(8)) AS DATE),
            CASE
                WHEN sls_sales IS NULL
                     OR sls_sales <= 0
                     OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales END,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price END
        FROM bronze.crm_sales_details;

        SET @rc = @@ROWCOUNT;
        PRINT '>> silver.crm_sales_details Rows Inserted: ' + CAST(@rc AS NVARCHAR(20));

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND,@start_time,@end_time);
        PRINT '>> silver.crm_sales_details Load Duration: ' 
              + CAST(@elapsed_ms/1000.0 AS NVARCHAR(30)) + ' seconds (' 
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

 print(' ')

        /**************************************************************
         *  silver.erp_cust_az12
         **************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';

        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
                 ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                 ELSE 'n/a' END
        FROM bronze.erp_cust_az12;

        SET @rc = @@ROWCOUNT;
        PRINT '>> silver.erp_cust_az12 Rows Inserted: ' + CAST(@rc AS NVARCHAR(20));

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND,@start_time,@end_time);
        PRINT '>> silver.erp_cust_az12 Load Duration: ' 
              + CAST(@elapsed_ms/1000.0 AS NVARCHAR(30)) + ' seconds (' 
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';
 print(' ')


        /**************************************************************
         *  silver.erp_loc_a101
         **************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT 
            REPLACE(cid,'-',''),
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE cntry END
        FROM bronze.erp_loc_a101;

        SET @rc = @@ROWCOUNT;
        PRINT '>> silver.erp_loc_a101 Rows Inserted: ' + CAST(@rc AS NVARCHAR(20));

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND,@start_time,@end_time);
        PRINT '>> silver.erp_loc_a101 Load Duration: ' 
              + CAST(@elapsed_ms/1000.0 AS NVARCHAR(30)) + ' seconds (' 
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

 print(' ')

        /**************************************************************
         *  silver.erp_px_cat_g1v2
         **************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @rc = @@ROWCOUNT;
        PRINT '>> silver.erp_px_cat_g1v2 Rows Inserted: ' + CAST(@rc AS NVARCHAR(20));

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND,@start_time,@end_time);
        PRINT '>> silver.erp_px_cat_g1v2 Load Duration: ' 
              + CAST(@elapsed_ms/1000.0 AS NVARCHAR(30)) + ' seconds (' 
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';


 print(' ')
        ---------------------------------------------------------------------
        -- Final Batch Timing
        ---------------------------------------------------------------------
        SET @batch_end_time = SYSDATETIME();
        SET @batch_elapsed_ms = DATEDIFF_BIG(MILLISECOND, @batch_start_time, @batch_end_time);

        PRINT '------------------------------------------------';
        PRINT '>> Total Silver Load Duration: ' 
              + CAST(@batch_elapsed_ms/1000.0 AS NVARCHAR(30)) + ' seconds (' 
              + CAST(@batch_elapsed_ms AS NVARCHAR(30)) + ' ms)';
        PRINT '================================================';

    END TRY
    BEGIN CATCH

        PRINT '================================================';
        PRINT 'Error occurred during loading SILVER layer';
        PRINT '================================================';
        PRINT ERROR_MESSAGE();

    END CATCH;
END;
GO
