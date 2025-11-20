
-- exec bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    -------------------------------------------------------------------------
    -- Procedure: bronze.load_bronze
    --
    -- Purpose:
    --   - Full refresh of all Bronze layer tables from CSV source files.
    --   - Loads CRM and ERP source data into staging/bronze schema.
    --
    -- Key behaviours:
    --   - TRUNCATE each target table (destructive full reload).
    --   - BULK INSERT from CSV files on disk.
    --   - Prints row counts and load durations per table + total batch time.
    --   - Basic error handling with TRY...CATCH.
    --
    -- Usage:
    --   EXEC bronze.load_bronze;
    --
    -- Notes:
    --   - SQL Server service account (or the executing login) must have
    --     READ access to the CSV file locations.
    --   - File layout (column order / data types) must match table schema.
    --   - Designed for dev/demo/local scenarios; for production, consider:
    --       * Using UNC paths (\\server\folder\file.csv)
    --       * Centralised logging tables instead of PRINT only
    --       * Optional incremental loads instead of TRUNCATE
    -------------------------------------------------------------------------

    BEGIN TRY
        ---------------------------------------------------------------------
        -- Declare timing variables used for:
        --   - per-table load time
        --   - total batch duration
        ---------------------------------------------------------------------
        DECLARE 
            @start_time        DATETIME2(3),
            @end_time          DATETIME2(3),
            @batch_start_time  DATETIME2(3),
            @batch_end_time    DATETIME2(3),
            @elapsed_ms        BIGINT;

        ---------------------------------------------------------------------
        -- Record the start of the entire bronze load batch
        ---------------------------------------------------------------------
        SET @batch_start_time = SYSDATETIME();

        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        ---------------------------------------------------------------------
        -- CRM: bronze.crm_cust_info
        --   - Full reload from source_crm\cust_info.csv
        --   - Represents customer master data from CRM
        ---------------------------------------------------------------------
        PRINT '>> Truncating Table: bronze.crm_cust_info';

        -- TRUNCATE used for fast, fully destructive reload (no WHERE allowed)
        TRUNCATE TABLE bronze.crm_cust_info;
        
        -- Start timing this specific load
        SET @start_time = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.crm_cust_info';

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,          -- Skip header row (row 1 contains column names)
            FIELDTERMINATOR = ',', -- CSV delimiter is comma
            ROWTERMINATOR = '\n',  -- Each row ends with newline
            TABLOCK                 -- Acquire table lock for faster bulk load
        );

        -- @@ROWCOUNT returns number of rows inserted by the BULK INSERT
        PRINT CAST(@@ROWCOUNT AS VARCHAR(20)) + ' rows imported';

        -- End timing for this table
        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> Load Duration (bronze.crm_cust_info): '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

        PRINT ' ';

        ---------------------------------------------------------------------
        -- CRM: bronze.crm_prd_info
        --   - Full reload from source_crm\prd_info.csv
        --   - Product master data from CRM
        ---------------------------------------------------------------------
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        SET @start_time = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.crm_prd_info';

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT CAST(@@ROWCOUNT AS VARCHAR(20)) + ' rows imported';

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> Load Duration (bronze.crm_prd_info): '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

        PRINT ' ';

        ---------------------------------------------------------------------
        -- CRM: bronze.crm_sales_details
        --   - Full reload from source_crm\sales_details.csv
        --   - Likely largest table (transaction-level sales data)
        ---------------------------------------------------------------------
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        SET @start_time = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.crm_sales_details';

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT CAST(@@ROWCOUNT AS VARCHAR(20)) + ' rows imported';

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> Load Duration (bronze.crm_sales_details): '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

        PRINT ' ';

        ---------------------------------------------------------------------
        -- Switch to ERP source tables
        ---------------------------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        ---------------------------------------------------------------------
        -- ERP: bronze.erp_cust_az12
        --   - Full reload from source_erp\CUST_AZ12.csv
        --   - Customer master data from ERP system
        ---------------------------------------------------------------------
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        SET @start_time = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT CAST(@@ROWCOUNT AS VARCHAR(20)) + ' rows imported';

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> Load Duration (bronze.erp_cust_az12): '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

        PRINT ' ';

        ---------------------------------------------------------------------
        -- ERP: bronze.erp_loc_a101
        --   - Full reload from source_erp\LOC_A101.csv
        --   - Location / branch / warehouse data from ERP
        ---------------------------------------------------------------------
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        SET @start_time = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT CAST(@@ROWCOUNT AS VARCHAR(20)) + ' rows imported';

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> Load Duration (bronze.erp_loc_a101): '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

        PRINT ' ';

        ---------------------------------------------------------------------
        -- ERP: bronze.erp_px_cat_g1v2
        --   - Full reload from source_erp\PX_CAT_G1V2.csv
        --   - Price / category mapping data from ERP
        ---------------------------------------------------------------------
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        SET @start_time = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT CAST(@@ROWCOUNT AS VARCHAR(20)) + ' rows imported';

        SET @end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @start_time, @end_time);

        PRINT '>> Load Duration (bronze.erp_px_cat_g1v2): '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

        ---------------------------------------------------------------------
        -- End of Bronze load - print total batch duration
        ---------------------------------------------------------------------
        PRINT '================================================';
        PRINT 'Finished loading Bronze Layer';
        PRINT '================================================';

        SET @batch_end_time = SYSDATETIME();
        SET @elapsed_ms = DATEDIFF_BIG(MILLISECOND, @batch_start_time, @batch_end_time);

        PRINT '>> Total Duration: '
              + CAST(@elapsed_ms / 1000.0 AS NVARCHAR(30)) + ' seconds ('
              + CAST(@elapsed_ms AS NVARCHAR(30)) + ' ms)';

    END TRY
    BEGIN CATCH
        ---------------------------------------------------------------------
        -- Error handling:
        --   - Any runtime error in TRY block jumps here.
        --   - Currently prints only the error message.
        --   - Can be extended to log details (number, line, procedure, etc.)
        ---------------------------------------------------------------------
        PRINT '================================================';
        PRINT 'Error occurred during loading bronze layer';
        PRINT '================================================';

        PRINT ERROR_MESSAGE();
    END CATCH
END;
GO
