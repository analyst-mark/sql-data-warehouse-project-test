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
SELECT cst_id,
       cst_key,
       Trim(cst_firstname) AS cst_firstname,
       Trim(cst_lastname) AS cst_lastname,
       CASE
           WHEN Upper(Trim(t.cst_marital_status)) = 'S' THEN
               'Single'
           WHEN Upper(Trim(t.cst_marital_status)) = 'M' THEN
               'Married'
           ELSE
               'n/a'
       END AS cst_marital_status,
       CASE
           WHEN Upper(Trim(cst_gndr)) = 'F' THEN
               'Female'
           WHEN Upper(Trim(cst_gndr)) = 'M' THEN
               'Male'
           ELSE
               'n/a'
       END AS cst_gndr,
       cst_create_date
FROM
(
    SELECT *,
           Row_number() OVER (partition BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE t.flag_last = 1
      AND t.cst_id IS NOT NULL;


SELECT * FROM silver.crm_cust_info cci 