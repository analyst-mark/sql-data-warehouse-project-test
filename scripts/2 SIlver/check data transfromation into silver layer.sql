-- check for nulls or duplicates in primary key
-- expectation : no result

SELECT 
cst_id
FROM silver.crm_cust_info cci 
GROUP BY cci.cst_id 
HAVING count(*) > 1 OR cst_id IS NULL


--check for unwanted spaces
-- expectation : no result

SELECT 
cst_key
FROM silver.crm_cust_info cci 
WHERE cst_key != trim(cst_key)


--date standardization and consistency

SELECT cci.cst_marital_status 
FROM silver.crm_cust_info cci 

SELECT *
FROM silver.crm_cust_info cci 