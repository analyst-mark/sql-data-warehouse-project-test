CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        ---------------------------------------------------------------------
        -- 1. Declare timing variables
        ---------------------------------------------------------------------
        DECLARE 
            @start_time        DATETIME2(3),
            @end_time          DATETIME2(3),
            @batch_start_time  DATETIME2(3),
            @batch_end_time    DATETIME2(3),
            @elapsed_ms        BIGINT,
            @batch_elapsed_ms  BIGINT;

        ---------------------------------------------------------------------
        -- 2. Start overall batch timer
        ---------------------------------------------------------------------
        SET @batch_start_time = SYSDATETIME();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM + ERP Tables into SILVER';
        PRINT '------------------------------------------------';

        /*********************************************************************
         * 3. SILVER.CRM_CUST_INFO
         *    - Deduplicate customers by cst_id (keep latest by cst_create_date)
         *    - Clean names, marital status, and gender
         *********************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';

        INSERT INTO silver.crm_cust_info
        (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname)  AS cst_lastname,
            CASE
                WHEN UPPER(TRIM(t.cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(t.cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC
                ) AS flag_last
            FROM bronze.crm_cust_info
        ) AS t
        WHERE
            t.flag_last = 1
            AND t.cst_id IS NOT NULL;

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> silver.crm_cust_info Load Duration: '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';


        /*********************************************************************
         * 4. SILVER.CRM_PRD_INFO
         *    - Derive category ID from prd_key
         *    - Derive cleaned prd_key (remove leading segment)
         *    - Map product line codes to descriptions
         *    - Create effective-dated product rows (start/end dates)
         *********************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';

        INSERT INTO silver.crm_prd_info
        (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,   -- e.g. "AB-12" -> "AB_12"
            SUBSTRING(prd_key, 7, LEN(prd_key))         AS prd_key,  -- drop first 6 chars + dash pattern
            prd_nm,
            ISNULL(prd_cost, 0)                         AS prd_cost, -- null-safe cost
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END                                         AS prd_line,
            CAST(prd_start_dt AS DATE)                  AS prd_start_dt,
            CAST(
                LEAD(prd_start_dt) OVER
                (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) - 1 AS DATE
            )                                           AS prd_end_dt -- day before next start
        FROM bronze.crm_prd_info;

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> silver.crm_prd_info Load Duration: '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';


        /*********************************************************************
         * 5. SILVER.CRM_SALES_DETAILS
         *    - Validate and convert date fields from numeric YYYYMMDD
         *    - Fix inconsistent sales and price:
         *        * If sales is null/<=0 or != quantity * abs(price),
         *          recompute as quantity * abs(price)
         *        * If price is null/<=0, recompute as sales / quantity
         *********************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';

        INSERT INTO silver.crm_sales_details
        (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE
                WHEN sls_order_dt = 0
                     OR LEN(sls_order_dt) != 8
                     THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
            END AS sls_order_dt,
            CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) AS sls_ship_dt,
            CAST(CAST(sls_due_dt  AS VARCHAR(8)) AS DATE) AS sls_due_dt,
            CASE
                WHEN sls_sales IS NULL
                     OR sls_sales <= 0
                     OR sls_sales <> sls_quantity * ABS(sls_price)
                     THEN sls_quantity * ABS(sls_price)
                -- corrected sales: quantity × abs(price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL
                     OR sls_price <= 0
                     THEN sls_sales / NULLIF(sls_quantity, 0)
                -- corrected price: sales ÷ quantity (avoid div by zero)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> silver.crm_sales_details Load Duration: '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';


        /*********************************************************************
         * 6. SILVER.ERP_CUST_AZ12
         *    - Strip leading "NAS" from cid
         *    - Null out future birthdates
         *    - Normalise gender to 'Male'/'Female'/'n/a'
         *********************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';

        INSERT INTO silver.erp_cust_az12
        (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- drop "NAS" prefix
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > GETDATE() THEN NULL  -- invalid: birthdate in future
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> silver.erp_cust_az12 Load Duration: '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';


        /*********************************************************************
         * 7. SILVER.ERP_LOC_A101
         *    - Remove dashes from cid
         *    - Map country codes to friendly names
         *      * DE   -> Germany
         *      * US/USA -> United States
         *      * ''/NULL -> 'n/a'
         *********************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';

        INSERT INTO silver.erp_loc_a101
        (
            cid,
            cntry
        )
        SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN TRIM(cntry) = 'DE'               THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA')     THEN 'United States'
                WHEN TRIM(cntry) = ''
                     OR cntry IS NULL                 THEN 'n/a'
                ELSE cntry
            END AS cntry
        FROM bronze.erp_loc_a101 AS ela;

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> silver.erp_loc_a101 Load Duration: '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';


        /*********************************************************************
         * 8. SILVER.ERP_PX_CAT_G1V2
         *    - Simple 1:1 copy from bronze to silver
         *********************************************************************/
        SET @start_time = SYSDATETIME();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

        INSERT INTO silver.erp_px_cat_g1v2
        (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> silver.erp_px_cat_g1v2 Load Duration: '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';


        ---------------------------------------------------------------------
        -- 9. End overall batch timer and print total duration
        ---------------------------------------------------------------------
        SET @batch_end_time = SYSDATETIME();
        SET @batch_elapsed_ms = DATEDIFF_BIG(MILLISECOND, @batch_start_time, @batch_end_time);

        PRINT '------------------------------------------------';
        PRINT '>> Total Silver Load Duration: '
              + CAST(@batch_elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@batch_elapsed_ms AS NVARCHAR(30)) + ' ms)';
        PRINT '================================================';

    END TRY
    BEGIN CATCH

        ---------------------------------------------------------------------
        -- Error handling
        ---------------------------------------------------------------------
        PRINT '================================================';
        PRINT 'Error occurred during loading SILVER layer';
        PRINT '================================================';

        PRINT 'Error Number:   ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(20));
        PRINT 'Error State:    ' + CAST(ERROR_STATE() AS NVARCHAR(20));
        PRINT 'Error Line:     ' + CAST(ERROR_LINE() AS NVARCHAR(20));
        PRINT 'Error Message:  ' + ERROR_MESSAGE();

        -- Optional: rethrow error to caller
        -- THROW;

    END CATCH;
END;
GO
