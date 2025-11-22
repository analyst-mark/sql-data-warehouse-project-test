


SELECT * 
FROM bronze.crm_prd_info;

-- query transformation
SELECT
	prd_id,
	prd_key,	
	replace(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) prd_key,
	prd_nm,
	isnull(prd_cost, 0) prd_cost,
		CASE
		upper(trim(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line,
	CAST(prd_start_dt AS date) prd_start_dt,
	CAST(lead(prd_start_dt) over( PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS date) prd_end_dt
FROM
	bronze.crm_prd_info;


--- update the schema
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);



--insert

INSERT
	INTO
	silver.crm_prd_info (prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)
SELECT
	prd_id,	
	replace(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) prd_key,
	prd_nm,
	isnull(prd_cost, 0) prd_cost,
		CASE
		upper(trim(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line,
	CAST(prd_start_dt AS date) prd_start_dt,
	CAST(lead(prd_start_dt) OVER( PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS date) prd_end_dt
FROM
	bronze.crm_prd_info;



SELECT * 
FROM silver.crm_prd_info;
