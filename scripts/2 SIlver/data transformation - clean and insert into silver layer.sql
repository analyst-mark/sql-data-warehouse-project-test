/* 
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'crm_cust_info'
  AND TABLE_SCHEMA = 'bronze';

*/

INSERT INTO silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
     CASE 
        WHEN upper(trim(t.cst_marital_status)) = 'S' THEN 'Single'
        WHEN upper(trim(t.cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS  cst_marital_status,
    CASE 
        WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
        WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
                PARTITION BY cst_id 
                ORDER BY cst_create_date DESC
           ) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE t.flag_last = 1
  AND t.cst_id IS NOT NULL;



