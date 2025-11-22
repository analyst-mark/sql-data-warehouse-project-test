-- Check for Invalid Dates
SELECT
    NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE   sls_order_dt <= 0
    OR  LEN(sls_order_dt) != 8
    OR  sls_order_dt > 20500101
    OR  sls_order_dt < 19000101;



SELECT DISTINCT
    sls_sales AS old_sls_sales,      -- original (possibly incorrect) sales value
    sls_quantity,                    -- original quantity
    sls_price AS old_sls_price,      -- original (possibly incorrect) price value

    -- Recalculate Sales if:
    --   • sls_sales is NULL
    --   • sls_sales is <= 0
    --   • sls_sales does NOT equal quantity × price
    CASE 
        WHEN sls_sales IS NULL 
             OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)   -- corrected sales: quantity × price
        ELSE sls_sales
    END AS sls_sales,

    -- Recalculate Price if:
    --   • sls_price is NULL
    --   • sls_price is <= 0
    -- Otherwise keep the original price
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)  -- corrected price: sales ÷ quantity
        ELSE sls_price
    END AS sls_price     

FROM bronze.crm_sales_details

-- Return only rows that violate business rules:
--   • Sales ≠ Quantity × Price
--   • Any NULL values
--   • Any zero or negative numbers
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0

ORDER BY sls_sales, sls_quantity, sls_price;
