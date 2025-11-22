
CREATE VIEW gold.fact_sales as
SELECT
    sd.sls_ord_num order_number,
    pr.product_key,
ds.customer_key,
    sd.sls_order_dt order_date,
    sd.sls_ship_dt shipping_date,
    sd.sls_due_dt due_date,
    sd.sls_sales sales_amount,
    sd.sls_quantity quantity,
    sd.sls_price price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
ON sd.sls_prd_key = pr.product_number
LEFT join  gold.dim_customers ds
ON ds.customer_id = sd.sls_cust_id 

SELECT * FROM gold.fact_sales
