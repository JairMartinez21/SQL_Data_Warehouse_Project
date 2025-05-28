CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS 
BEGIN
	BEGIN TRY
		IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
			DROP TABLE bronze.crm_cust_info;

		CREATE TABLE bronze.crm_cust_info (
			cst_id INT, 
			cst_key VARCHAR(20), 
			cst_firstname NVARCHAR(60), 
			cst_lastname NVARCHAR(60), 
			cst_maritalstatus CHAR(2), 
			cst_gndr CHAR(2), 
			cst_create_date DATE
		);

		IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
			DROP TABLE bronze.crm_prd_info;

		CREATE TABLE bronze.crm_prd_info (
			prd_id INT, 
			prd_key VARCHAR(40), 
			prd_nm NVARCHAR(40), 
			prd_cost INT, 
			prd_line CHAR(2), 
			prd_start_dt DATE, 
			prd_end_dt DATE
		);

		IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
			DROP TABLE bronze.crm_sales_details;

		CREATE TABLE bronze.crm_sales_details (
			sls_ord_num VARCHAR(20), 
			sls_prd_key NVARCHAR(30), 
			sls_cust_id INT, 
			sls_order_dt INT, 
			sls_ship_dt INT, 
			sls_due_dt INT, 
			sls_sales INT, 
			sls_quantity INT, 
			sls_price INT
		);

		IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
			DROP TABLE bronze.erp_cust_az12;

		CREATE TABLE bronze.erp_cust_az12 (
			CID VARCHAR(40), 
			BDATE DATE, 
			GEN VARCHAR(20)
		);

		IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
			DROP TABLE bronze.erp_loc_a101;

		CREATE TABLE bronze.erp_loc_a101 (
			CID VARCHAR(20),
			CNTRY VARCHAR(50)
		);

		IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
			DROP TABLE bronze.erp_px_cat_g1v2;

		CREATE TABLE bronze.erp_px_cat_g1v2 (
			ID VARCHAR(30), 
			CAT VARCHAR(60), 
			SUBCAT VARCHAR(60), 
			MAINTENANCE CHAR(3)
		);

		DECLARE @start_time DATETIME, @end_time DATETIME;
		DECLARE @start_time_batch DATETIME, @end_time_batch DATETIME;
		SET @start_time_batch = GETDATE();
		-- Load data into tables
		SET @start_time = GETDATE()
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\Jair Martinez\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'


		SET @start_time = GETDATE()
		BULK INSERT bronze.crm_prd_info 
		FROM 'C:\Users\Jair Martinez\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

		SET @start_time = GETDATE()
		BULK INSERT bronze.crm_sales_details 
		FROM 'C:\Users\Jair Martinez\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

		SET @start_time = GETDATE()
		BULK INSERT bronze.erp_cust_az12 
		FROM 'C:\Users\Jair Martinez\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

		SET @start_time = GETDATE()
		BULK INSERT bronze.erp_loc_a101 
		FROM 'C:\Users\Jair Martinez\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

		SET @start_time = GETDATE()
		BULK INSERT bronze.erp_px_cat_g1v2 
		FROM 'C:\Users\Jair Martinez\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

	END TRY
	BEGIN CATCH
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Procedure' + ISNULL(ERROR_PROCEDURE(), 'N/A');
		PRINT 'Error Line' + CAST(ERROR_LINE() AS VARCHAR);
	END CATCH
	SET @end_time_batch = GETDATE()
	PRINT '>> Load Duration of the whole batch: ' + CAST(DATEDIFF(SECOND, @start_time_batch, @end_time_batch) AS VARCHAR) + ' seconds';

END

EXEC bronze.load_bronze;
