SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'crm_sales_details'
  AND TABLE_SCHEMA = 'bronze';

 
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
    WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
	CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) sls_ship_dt,
		CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) sls_due_dt,
	    CASE 
        WHEN sls_sales IS NULL 
             OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)   -- corrected sales: quantity × price
        ELSE sls_sales
    END AS sls_sales,
	sls_quantity,
	 CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)  -- corrected price: sales ÷ quantity
        ELSE sls_price
    END AS sls_price  
FROM
	bronze.crm_sales_details;




IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt date,
    sls_ship_dt  date,
    sls_due_dt   date,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
INSERT
	INTO
	silver.crm_sales_details (


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
		OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) sls_ship_dt,
		CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) sls_due_dt,
	    CASE
		WHEN sls_sales IS NULL
		OR sls_sales <= 0
		OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
		-- corrected sales: quantity × price
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	 CASE
		WHEN sls_price IS NULL
		OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
		-- corrected price: sales ÷ quantity
		ELSE sls_price
	END AS sls_price
FROM
	bronze.crm_sales_details;




SELECT * FROM silver.crm_sales_details csd 
