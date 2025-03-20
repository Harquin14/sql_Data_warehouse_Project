/*
=============================================================
Stored Procedure: bronze.load_bronze
This procedure automates the data loading process by truncating 
and inserting data into the bronze layer tables. 
It also includes load duration tracking and error handling.
=============================================================
*/

GO
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY
        PRINT('Loading Bronze Layer');
        PRINT('======================================================');

        -- CRM Tables
        PRINT('Loading CRM Tables');
        PRINT('======================================================');

        -- crm_cust_info
        SET @start_time  = GETDATE();
        PRINT('>>>> Truncating table: bronze.crm_cust_info');
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT('>>>> Inserting Into: bronze.crm_cust_info');
        BULK INSERT bronze.crm_cust_info
        FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_crm/cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT('======================================================');

        -- crm_prd_info
        SET @start_time = GETDATE();
        PRINT('>>>> Truncating table: bronze.crm_prd_info');
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT('>>>> Inserting Into: bronze.crm_prd_info');
        BULK INSERT bronze.crm_prd_info
        FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_crm/prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT('======================================================');

        -- crm_sales_details
        SET @start_time = GETDATE();
        PRINT('>>>> Truncating table: bronze.crm_sales_details');
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT('>>>> Inserting Into: bronze.crm_sales_details');
        BULK INSERT bronze.crm_sales_details
        FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_crm/sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK, KEEPNULLS);
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT('======================================================');

        -- ERP Tables
        PRINT('Loading ERP Tables');
        PRINT('======================================================');

        -- erp_cust_az12
        SET @start_time = GETDATE();
        PRINT('>>>> Truncating table: bronze.erp_cust_az12');
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT('>>>> Inserting Into: bronze.erp_cust_az12');
        BULK INSERT bronze.erp_cust_az12
        FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_erp/cust_az12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT('======================================================');

        -- erp_loc_a101
        SET @start_time = GETDATE();
        PRINT('>>>> Truncating table: bronze.erp_loc_a101');
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT('>>>> Inserting Into: bronze.erp_loc_a101');
        BULK INSERT bronze.erp_loc_a101
        FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_erp/loc_a101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT('======================================================');

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT('>>>> Truncating table: bronze.erp_px_cat_g1v2');
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT('>>>> Inserting Into: bronze.erp_px_cat_g1v2');
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'F:\SQL PROJECTS\Medallion DataWarehouse\datasets\source_erp/px_cat_g1v2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT('======================================================');

    END TRY
    BEGIN CATCH
        PRINT('An Error Occurred: ' + ERROR_MESSAGE());
        PRINT('Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR));
    END CATCH
END
GO

-- Execute Stored Procedure
EXEC bronze.load_bronze;
