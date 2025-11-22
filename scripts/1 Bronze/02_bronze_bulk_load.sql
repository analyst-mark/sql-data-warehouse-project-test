/*
=====================================================================
 Script Name   : 02_bronze_bulk_load.sql
 Description   : Bulk load raw CRM and ERP CSV data files into Bronze tables
 Layer         : Bronze (Raw Data Layer)
=====================================================================

 🔍 Purpose:
    This script truncates and reloads all Bronze-layer tables
    with the latest CSV files from the `datasets` folder.
    The Bronze layer stores raw, uncleaned data as received
    from source systems (CRM, ERP).

 ⚠️ Warning:
    - TRUNCATE removes all data from the table.
    - Ensure CSV paths are valid and SQL Server has read access.
    - 'FIRSTROW = 2' skips header lines in each CSV.
=====================================================================
*/

USE DataWarehouse;
GO


/* ================================================================
   1️⃣ Load CRM Source Files
   ================================================================ */


-- ----------------------------------------------------------------
-- Table: bronze.crm_cust_info
-- Purpose: Load raw CRM customer information
-- Notes: Source CSV contains 18,494 rows (1 header + 18,493 data rows)
-- ----------------------------------------------------------------
TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_crm\cust_info.csv'
WITH (
    FIRSTROW = 2,             -- skip header row
    FIELDTERMINATOR = ',',    -- CSV is comma-delimited
    TABLOCK                   -- table-level lock for performance
);

-- Validate data load
SELECT TOP (10) * FROM bronze.crm_cust_info;  -- preview sample rows
SELECT COUNT(*) AS total_rows FROM bronze.crm_cust_info;  -- expected: 18493
GO



-- ----------------------------------------------------------------
-- Table: bronze.crm_prd_info
-- Purpose: Load raw CRM product information
-- ----------------------------------------------------------------
TRUNCATE TABLE bronze.crm_prd_info;

BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_crm\prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

SELECT TOP (10) * FROM bronze.crm_prd_info;
SELECT COUNT(*) AS total_rows FROM bronze.crm_prd_info;
GO



-- ----------------------------------------------------------------
-- Table: bronze.crm_sales_details
-- Purpose: Load raw CRM sales transaction details
-- ----------------------------------------------------------------
TRUNCATE TABLE bronze.crm_sales_details;

BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_crm\sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

SELECT TOP (10) * FROM bronze.crm_sales_details;
SELECT COUNT(*) AS total_rows FROM bronze.crm_sales_details;
GO




/* ================================================================
   2️⃣ Load ERP Source Files
   ================================================================ */


-- ----------------------------------------------------------------
-- Table: bronze.erp_cust_az12
-- Purpose: Load raw ERP customer master data
-- ----------------------------------------------------------------
TRUNCATE TABLE bronze.erp_cust_az12;

BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_erp\CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

SELECT TOP (10) * FROM bronze.erp_cust_az12;
SELECT COUNT(*) AS total_rows FROM bronze.erp_cust_az12;
GO



-- ----------------------------------------------------------------
-- Table: bronze.erp_loc_a101
-- Purpose: Load raw ERP location data
-- ----------------------------------------------------------------
TRUNCATE TABLE bronze.erp_loc_a101;

BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_erp\LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

SELECT TOP (10) * FROM bronze.erp_loc_a101;
SELECT COUNT(*) AS total_rows FROM bronze.erp_loc_a101;
GO



-- ----------------------------------------------------------------
-- Table: bronze.erp_px_cat_g1v2
-- Purpose: Load raw ERP product category mapping
-- ----------------------------------------------------------------
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\mark\Documents\GitHub\sql-data-warehouse-project-test\datsets\source_erp\PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

SELECT TOP (10) * FROM bronze.erp_px_cat_g1v2;
SELECT COUNT(*) AS total_rows FROM bronze.erp_px_cat_g1v2;
GO



/* ================================================================
   ✅ Validation Summary
   ================================================================
   After running, verify:
   - Each SELECT COUNT(*) matches the expected row count.
   - Random data samples appear correctly formatted.
   - File access paths remain valid.
================================================================ */
