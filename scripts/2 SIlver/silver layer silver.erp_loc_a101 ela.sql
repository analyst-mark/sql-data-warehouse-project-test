SELECT 
DISTINCT cntry
FROM bronze.erp_loc_a101 ela 


INSERT INTO silver.erp_loc_a101 (cid,cntry)

SELECT 
replace(cid,'-','') cid, 
CASE  WHEN trim(cntry) = 'DE' THEN 'Germany'
  WHEN trim(cntry)in ('US','USA') THEN  'United States'
  WHEN trim(cntry) ='' OR cntry IS NULL THEN  'n/a'
	  ELSE cntry
END AS  cntry
FROM bronze.erp_loc_a101 ela 


SELECT * FROM silver.erp_loc_a101 ela 