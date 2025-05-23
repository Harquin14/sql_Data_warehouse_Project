/*
=============================================================
BULK INSERT Statements for Bronze Layer Data Loading
This script contains only the individual BULK INSERT statements
without wrapping them inside a stored procedure. Use this for 
manual or one-time data loading processes.
=============================================================
*/

-- Insert into bronze.crm_cust_info
TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_crm/cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- Insert into bronze.crm_prd_info
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_crm/prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- Insert into bronze.crm_sales_details
TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_crm/sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK,
    KEEPNULLS
);

-- Insert into bronze.erp_cust_az12
TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_erp/cust_az12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- Insert into bronze.erp_loc_a101
TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_erp/loc_a101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- Insert into bronze.erp_px_cat_g1v2
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_erp/px_cat_g1v2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
