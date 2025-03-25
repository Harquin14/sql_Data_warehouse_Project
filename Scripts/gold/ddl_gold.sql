/*
====================================================================================
Data Warehouse DDL Script - Gold Layer (Star Schema Design)
====================================================================================
Purpose:
This script defines the schema for a data warehouse designed using the **Star Schema** 
approach. It is built for analytics and reporting purposes.

### Schema Overview:
1. **gold.dim_customers** - Customer dimension table
2. **gold.dim_products** - Product dimension table
3. **gold.fact_sales** - Sales fact table

### Key Highlights:
- The data model follows a **Star Schema** to optimize performance for reporting.
- The **fact table** links to the **dimension tables** via surrogate keys.
- The **views** allow analysts to easily access clean, structured data.

====================================================================================
*/

--===================================================================================
-- BUILDING CUSTOMER DIMENSION (gold.dim_customers)
-- This view contains customer details used for analytics and reporting.
--===================================================================================
CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    l.cntry AS country,
    ca.bdate AS birthdate,
    CASE 
        WHEN ci.cst_gender <> 'n/a' THEN ci.cst_gender
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ci.cst_marital_status AS marital_status,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 l ON ci.cst_key = l.cid;

-- Query to verify the data
SELECT * FROM gold.dim_customers;

--===================================================================================
-- BUILDING PRODUCT DIMENSION (gold.dim_products)
-- This view provides structured product information for sales analysis.
--===================================================================================
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_id, pi.prd_start_dt) AS product_key,
    pi.prd_id AS product_id, 
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL; -- Exclude historical products

-- Query to verify the data
SELECT * FROM gold.dim_products;

--===================================================================================
-- BUILDING SALES FACT TABLE (gold.fact_sales)
-- This fact table records all sales transactions.
--===================================================================================
CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num AS order_number,
    p.product_key,
    cs.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_date AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products p ON sd.sls_prd_key = p.product_number
LEFT JOIN gold.dim_customers cs ON sd.sls_cust_id = cs.customer_id;

-- Query to verify the data
SELECT * FROM gold.fact_sales;
