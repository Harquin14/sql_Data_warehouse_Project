/*
=================================================================
Script: Silver Layer DDL for Medallion Data Warehouse
Description:
This script creates the necessary tables for the silver layer.
It ensures clean execution by dropping existing tables before re-creating them. 
The silver layer is responsible for holding cleaned and transformed data
from the bronze layer.

Execution:
Run this script to set up the silver layer tables. 
Safe for multiple executions without errors due to DROP IF EXISTS.

=================================================================
*/

-- Drop and Create silver.crm_cust_info
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(100),
    cst_lastname NVARCHAR(100),
    cst_marital_status NVARCHAR(50),
    cst_gender NVARCHAR(50),
    cst_create_date DATE
);

-- Drop and Create silver.crm_prd_info
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(100),
    prd_cost FLOAT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

-- Drop and Create silver.crm_sales_details
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_date DATE,
    sls_sales FLOAT,
    sls_quantity INT,
    sls_price FLOAT
);

-- Drop and Create silver.erp_cust_az12
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(20)
);

-- Drop and Create silver.erp_loc_a101
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

-- Drop and Create silver.erp_px_cat_g1v2
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id INT,
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance FLOAT
);
