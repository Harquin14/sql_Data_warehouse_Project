/*===========================================================================================
  QUALITY CHECKS & DATA CLEANING: From Bronze Layer to Silver Layer
  Author: [Your Name]
  Date: [Date]
  Description: This script performs data quality checks and cleaning on datasets in the Bronze Layer 
               before transforming and loading them into the Silver Layer of the Medallion Architecture.
=============================================================================================*/


/*-------------------------------------------------------------------------------------------
  TABLE 1: crm_cust_info
  Purpose: Check duplicates, trim spaces, standardize gender values
--------------------------------------------------------------------------------------------*/

-- 1. Check for duplicate customer IDs (cst_id) - keeping the latest based on cst_create_date
SELECT * 
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num
  FROM bronze.crm_cust_info
) AS duplicates
WHERE row_num > 1;

-- 2. Check for unwanted leading/trailing spaces in first names
SELECT cst_firstname 
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

-- 3. Standardize gender values: Replace M as Male, F as Female, NULL as n/a
SELECT DISTINCT cst_gender 
FROM silver.crm_cust_info;



/*-------------------------------------------------------------------------------------------
  TABLE 2: crm_prd_info
  Purpose: Check duplicates, extract cat_id and prd_key, clean product names, handle nulls, 
           transform prd_line, check dates
--------------------------------------------------------------------------------------------*/

-- 1. Check for duplicate product IDs (prd_id)
SELECT * 
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY prd_id ORDER BY prd_id DESC) AS row_num
  FROM bronze.crm_prd_info
) AS duplicates
WHERE row_num > 1;

-- 2. Extract cat_id and prd_key from prd_key, replace '-' with '_'
SELECT 
  prd_id, 
  prd_key,
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  REPLACE(SUBSTRING(prd_key, 7, LEN(prd_key)), '-', '_') AS new_prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt 
FROM bronze.crm_prd_info;

-- 3. Check for unwanted spaces in product names (prd_nm)
SELECT prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- 4. Handling null values in prd_cost (Use ISNULL/COALESCE in transformation step)

-- 5. Standardize prd_line values: 
-- R = ROAD, M = MOUNTAIN, T = TOURING, O = OTHER SALES (Transformation done during ETL)

-- 6. Check for invalid dates:
-- prd_start_dt should not be greater than prd_end_dt
SELECT * 
FROM silver.crm_prd_info 
WHERE prd_end_dt < prd_start_dt;

-- 7. Identify missing or null start dates and suggest deriving prd_end_dt as (Next prd_start_dt - 1)
SELECT *, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS suggested_prd_end_dt
FROM bronze.crm_prd_info;


/*-------------------------------------------------------------------------------------------
  TABLE 3: crm_sales_details
  Purpose: Trim spaces, validate and convert dates, check sales, quantity, and price consistency
--------------------------------------------------------------------------------------------*/

-- 1. Check for unwanted spaces in sales order numbers
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);

-- 2. Validate and clean sales_order_dt, sls_ship_dt, sls_due_date:
-- Check if values are zero or not 8 digits (Invalid)
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_order_dt) <> 8 OR sls_order_dt = 0;

SELECT sls_ship_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_ship_dt) <> 8 OR sls_ship_dt = 0;

SELECT sls_due_date
FROM bronze.crm_sales_details
WHERE LEN(sls_due_date) <> 8 OR sls_due_date = 0;

-- 3. Check if order date > ship date OR order date > due date (Invalid) 
-- (Handled in transformation logic)

-- 4. Validate sales, quantity, price:
-- Sales should be: sales = quantity * abs(price)
-- If sales <= 0 or NULL, recalculate
SELECT DISTINCT sls_price, sls_quantity, sls_sales,
  CASE 
    WHEN sls_price IS NULL OR sls_price < 0 THEN sls_sales / sls_quantity
    ELSE sls_price
  END AS corrected_sls_price,
  CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price) 
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END AS corrected_sls_sales
FROM bronze.crm_sales_details;


/*-------------------------------------------------------------------------------------------
  TABLE 4: erp_cust_az12
  Purpose: Derive cid, clean birthdates, normalize gender values
--------------------------------------------------------------------------------------------*/

-- 1. Derive cid: Remove 'NAS' prefix
-- 2. Convert invalid birthdates (future dates) to NULL
-- 3. Normalize gender: M/F or Male/Female -> Male/Female; else n/a
SELECT 
  CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cleaned_cid,
  CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS cleaned_bdate,
  CASE 
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
    ELSE 'n/a'
  END AS cleaned_gen
FROM bronze.erp_cust_az12;

-- Check distinct gender values before normalization
SELECT DISTINCT gen FROM bronze.erp_cust_az12;


/*-------------------------------------------------------------------------------------------
  TABLE 5: erp_loc_a101
  Purpose: Replace hyphens in cid, standardize country values
--------------------------------------------------------------------------------------------*/

-- Replace '-' with '' in cid and standardize cntry values
SELECT 
  REPLACE(cid, '-', '') AS cleaned_cid,
  CASE 
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
    ELSE TRIM(cntry) 
  END AS cleaned_cntry
FROM bronze.erp_loc_a101;


/*-------------------------------------------------------------------------------------------
  TABLE 6: erp_px_cat_g1v2
  Purpose: Trim spaces in maintenance field
--------------------------------------------------------------------------------------------*/

-- Check and remove unwanted spaces in maintenance field
SELECT DISTINCT maintenance 
FROM bronze.erp_px_cat_g1v2
WHERE maintenance <> TRIM(maintenance);

