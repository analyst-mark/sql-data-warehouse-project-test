CREATE VIEW gold.dim_product
AS 
SELECT
ROW_NUMBER() OVER (ORDER  BY pn.prd_start_dt,pn.prd_key)  AS product_key,
    pn.prd_id product_id,
     pn.prd_key product_number,
    pn.prd_nm product_name, 
    pn.cat_id category_id,
     pc.cat category,
     pc.subcat subcategory,
         pc.maintenance ,
    pn.prd_cost cost,
    pn.prd_line product_line,
    pn.prd_start_dt start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id 
WHERE pn.prd_end_dt IS NULL -- filter out all  historical data

SELECT * FROM gold.dim_product;
