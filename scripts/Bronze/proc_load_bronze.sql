/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
-- Loading Data into Tables
CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '========================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '========================================================';

		PRINT '-------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------';
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE Bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT Bronze.crm_cust_info
		FROM 'C:\Users\sahan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'Seconds';
		PRINT'----------------------';

		PRINT '>> Truncating Table: bronze.crm_prd_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT Bronze.crm_prd_info
		FROM 'C:\Users\sahan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'Seconds';
		PRINT'----------------------';

		PRINT '>> Truncating Table: bronze.crm_sales_details';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT Bronze.crm_sales_details
		FROM 'C:\Users\sahan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'Seconds';
		PRINT'----------------------';

		PRINT '-------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------';

		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT Bronze.erp_cust_az12
		FROM 'C:\Users\sahan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'Seconds';
		PRINT'----------------------';

		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT Bronze.erp_loc_a101
		FROM 'C:\Users\sahan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'Seconds';
		PRINT'----------------------';


		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT Bronze.erp_px_cat_g1v2
		FROM 'C:\Users\sahan\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'Seconds';
		PRINT'----------------------';
		SET @batch_end_time=GETDATE();
		PRINT'========================================================';
		PRINT'Loading Bronze Layer is completed';
		PRINT 'Total Load Duration:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS NVARCHAR)+'Seconds';
		PRINT'========================================================';
	END TRY
	BEGIN CATCH
		PRINT'========================================================';
		PRINT'ERROR OCCURED WHILE LOADING BRONZE LAYER';
		PRINT'Error message'+ ERROR_MESSAGE();
		PRINT'Error message'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error message'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'========================================================';
	END CATCH
END

EXEC Bronze.load_bronze;
---------------------------------------
SELECT * FROM Bronze.crm_cust_info;

SELECT COUNT(*) FROM Bronze.crm_cust_info;

SELECT * FROM Bronze.crm_prd_info;

SELECT COUNT(*) FROM Bronze.crm_prd_info;

SELECT * FROM Bronze.crm_sales_details;

SELECT COUNT(*) FROM Bronze.crm_sales_details;

SELECT * FROM Bronze.erp_cust_az12;

SELECT COUNT(*) FROM Bronze.erp_cust_az12;

SELECT * FROM Bronze.erp_loc_a101;

SELECT COUNT(*) FROM Bronze.erp_loc_a101;
