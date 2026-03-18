/*
===================================================================================
Script    : 01_etl_load_bronze_cbs
Location  : scripts/01_bronze/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-17
Version   : 1.2
===================================================================================
Script Purpose:
    Loads all records from the source system (CBS) into the bronze layer. It has
	an in-buit logging system designed to track and monitor every ETL step, and
	thus enabling easy debugging.

	Tables Loaded:
		bronze.cbs_accounts
		bronze.cbs_branches
		bronze.cbs_transactions
===================================================================================
  Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-03-17 |  Initial creation                                |
	|   1.1   |  2026-03-18 |  Separated batch and step timing variables,      |
	|         |             |  added NULL guards in CATCH block                |
	|   1.2   |  2026-03-18 |  Fixed @source_object truncation, increased      |
	|         |             |  NVARCHAR(50) to NVARCHAR(200)                   |          
===================================================================================
*/
USE BankingDW;
GO

CREATE OR ALTER PROCEDURE etl.load_bronze_cbs AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;
	
	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE 
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_bronze_cbs',
	@source_system NVARCHAR(50) = 'CBS',
	@layer NVARCHAR(50) = 'Bronze',
	@batch_start_time DATETIME2,
	@batch_end_time DATETIME2,
	@batch_duration_seconds INT,
	@batch_status NVARCHAR(50) = 'Running',
	@total_rows INT = 0,
	@executed_by NVARCHAR(100) = SUSER_NAME(),
	@err_message NVARCHAR(MAX),

	-- Step-Level Variables (reused for each table)
	@step_id INT = NULL,
	@step_name NVARCHAR(50),
	@load_type NVARCHAR(50),
	@source_object NVARCHAR(200),
	@target_object NVARCHAR(50),
	@start_time DATETIME2,
	@end_time DATETIME2,
	@step_duration_seconds INT,
	@step_status NVARCHAR(50),
	@rows_extracted INT,
	@rows_inserted INT,
	@rows_rejected INT,
    
	-- Last loaded (Retrieved from etl.watermark)
	@wm_accounts DATETIME2,
	@wm_transactions DATETIME2,

	-- File paths (Update to match environment)
	@path_accounts NVARCHAR(200) = 'C:\SQLData\Finance Datasets\CBS\accounts.csv',
	@path_branches NVARCHAR(200) = 'C:\SQLData\Finance Datasets\CBS\branches.csv',
	@path_transactions NVARCHAR(200) = 'C:\SQLData\Finance Datasets\CBS\transactions.csv',

	-- target tables
	@target_accounts NVARCHAR(50) = 'bronze.cbs_accounts',
	@target_branches NVARCHAR(50) = 'bronze.cbs_branches',
	@target_transactions NVARCHAR(50) = 'bronze.cbs_transactions',

	-- Holds SQL queries (BULK INSERT)
	@sql NVARCHAR(MAX);

	-- =======================================================================================
	-- SECTION 2: OPEN BATCH — Log the start of this pipeline run
	-- =======================================================================================

	-- Retrieve batch start time
	SET @batch_start_time = SYSDATETIME();

	-- Load log details at batch-level
	INSERT INTO etl.batch_log
	(
		batch_name,
		source_system,
		layer,
		start_time,
		load_status,
		total_rows_processed,
		executed_by
	)
	VALUES
	(
		@batch_name,
		@source_system,
		@layer,
		@batch_start_time,
		@batch_status,
		@total_rows,
		@executed_by
	);
	-- Retrieve recently generated batch_id
	SET @batch_id = SCOPE_IDENTITY();

	-- =======================================================================================
	-- SECTION 3: READ WATERMARKS — Get last successful load point per table
	-- =======================================================================================
	
	SELECT @wm_accounts = last_loaded FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_accounts;

	SELECT @wm_transactions = last_loaded FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_transactions;

	-- =======================================================================================
	-- SECTION 4: LOAD ALL BRONZE TABLES
	-- =======================================================================================
	
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD bronze.cbs_accounts
	-- ===============================================

		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @source_object = @path_accounts;
		SET @target_object = @target_accounts;
		SET @step_name = 'Load bronze.cbs_accounts';
		SET @step_status = 'Running';
		SET @load_type = 'Incremental: Append-Only';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			step_name,
			batch_id,
			load_type,
			source_object,
			target_object,
			start_time,
			step_status,
			rows_extracted,
			rows_inserted,
			rows_rejected
		)
		VALUES
		(
			@step_name,
			@batch_id,
			@load_type,
			@source_object,
			@target_object,
			@start_time,
			@step_status,
			@rows_extracted,
			@rows_inserted,
			@rows_rejected
		);
		-- Retrieve recently generated step_id
		SET @step_id = SCOPE_IDENTITY();

		-- Create a temporary staging table
		CREATE TABLE #stg_accounts
		(
			account_id NVARCHAR(50),
			customer_id NVARCHAR(50),
			account_type NVARCHAR(50),
			account_status NVARCHAR(50),
			open_date DATE,
			close_date DATE,
			currency_code NVARCHAR(50),
			current_balance DECIMAL(18, 2),
			available_balance DECIMAL(18, 2),
			overdraft_limit DECIMAL(18, 2),
			interest_rate DECIMAL(12, 4),
			branch_id NVARCHAR(50),
			assigned_employee_id NVARCHAR(50),
			is_primary BIT,
			created_at DATETIME2,
			updated_at DATETIME2
		);

		-- Map SQL query to variable
		SET @sql = 'BULK INSERT #stg_accounts FROM ''' + @source_object + ''' WITH (FIRSTROW = 2, FORMAT = ''CSV'', FIELDTERMINATOR = '','', 
		ROWTERMINATOR = ''0x0A'', CODEPAGE = ''65001'', TABLOCK, KEEPNULLS);';

		-- Execute SQL query
		EXEC (@sql);

		-- Load bronze.cbs_accounts
		INSERT INTO bronze.cbs_accounts
		(
			account_id,
			customer_id,
			account_type,
			account_status,
			open_date,
			close_date,
			currency_code,
			current_balance,
			available_balance,
			overdraft_limit,
			interest_rate,
			branch_id,
			assigned_employee_id,
			is_primary,
			created_at,
			updated_at,
			
			-- Metadata columns
			_source_system,
			_source_file,
			_batch_id,
			_load_timestamp
		)
		SELECT
			account_id,
			customer_id,
			account_type,
			account_status,
			open_date,
			close_date,
			currency_code,
			current_balance,
			available_balance,
			overdraft_limit,
			interest_rate,
			branch_id,
			assigned_employee_id,
			is_primary,
			created_at,
			updated_at,

			-- Map values to metadata columns
			@source_system,
			@source_object,
			@batch_id,
			@start_time
		FROM #stg_accounts
		WHERE updated_at > @wm_accounts;

		-- Map values to variables on success
		SET @rows_inserted = @@ROWCOUNT;
		SET @rows_extracted = @rows_inserted;
		SET @total_rows = @total_rows + @rows_inserted;
		SET @end_time = SYSDATETIME();
		SET @step_duration_seconds = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'Success';

		-- Update log details at watermark-level on success
		UPDATE etl.watermark
			SET
				source_object = @source_object,
				last_batch_id = @batch_id,
				last_loaded = @end_time
			WHERE source_system = @source_system AND target_object = @target_object;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET 
				end_time = @end_time,
				load_duration_seconds = @step_duration_seconds,
				step_status = 'Success',
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;
		
		-- Drop staging table
		DROP TABLE IF EXISTS #stg_accounts;

	-- ===============================================
	-- STEP 2: LOAD bronze.cbs_branches
	-- ===============================================

		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @source_object = @path_branches;
		SET @target_object = @target_branches;
		SET @step_name = 'Load bronze.cbs_branches';
		SET @step_status = 'Running';
		SET @load_type = 'Full: Truncate & Insert';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			step_name,
			batch_id,
			load_type,
			source_object,
			target_object,
			start_time,
			step_status,
			rows_extracted,
			rows_inserted,
			rows_rejected
		)
		VALUES
		(
			@step_name,
			@batch_id,
			@load_type,
			@source_object,
			@target_object,
			@start_time,
			@step_status,
			@rows_extracted,
			@rows_inserted,
			@rows_rejected
		);
		-- Retrieve recently generated step_id
		SET @step_id = SCOPE_IDENTITY();

		-- Create temporary staging table
		CREATE TABLE #stg_branches
		(
			branch_id NVARCHAR(50),
			branch_name NVARCHAR(200),
			branch_type NVARCHAR(50),
			address_line_1 NVARCHAR(200),
			city NVARCHAR(50),
			[state] NVARCHAR(50),
			zip_code NVARCHAR(50),
			country NVARCHAR(50),
			phone_number NVARCHAR(50),
			email NVARCHAR(250),
			opened_date DATE,
			is_active BIT,
			region NVARCHAR(50),
			manager_employee_id NVARCHAR(50)
		);

		-- Map SQL query to variable
		SET @sql = 'BULK INSERT #stg_branches FROM ''' + @source_object + ''' WITH (FIRSTROW = 2, FORMAT = ''CSV'', FIELDTERMINATOR = '','', 
		ROWTERMINATOR = ''0x0A'', CODEPAGE = ''65001'', TABLOCK, KEEPNULLS);';

		-- Execute SQL query
		EXEC (@sql);

		-- Delete data from bronze.cbs_branches
		TRUNCATE TABLE bronze.cbs_branches;

		-- Load into bronze.cbs_branches
		INSERT INTO bronze.cbs_branches
		(
			branch_id,
			branch_name,
			branch_type,
			address_line_1,
			city,
			[state],
			zip_code,
			country,
			phone_number,
			email,
			opened_date,
			is_active,
			region,
			manager_employee_id,

			-- Metadata columns
			_source_system,
			_source_file,
			_batch_id,
			_load_timestamp
		)
		SELECT
			branch_id,
			branch_name,
			branch_type,
			address_line_1,
			city,
			[state],
			zip_code,
			country,
			phone_number,
			email,
			opened_date,
			is_active,
			region,
			manager_employee_id,

			-- Map values to metadata columns
			@source_system,
			@source_object,
			@batch_id,
			@start_time
		FROM #stg_branches;

		-- Map values to variables on successs
		SET @rows_inserted = @@ROWCOUNT;
		SET @rows_extracted = @rows_inserted;
		SET @total_rows = @total_rows + @rows_inserted;
		SET @end_time = SYSDATETIME();
		SET @step_duration_seconds = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'Success';

		-- Update log details at watermark-level on success
		UPDATE etl.watermark
			SET
				source_object = @source_object,
				last_batch_id = @batch_id,
				last_loaded = @end_time
			WHERE source_system = @source_system AND target_object = @target_object;
		
		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET 
				end_time = @end_time,
				load_duration_seconds = @step_duration_seconds,
				step_status = 'Success',
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;
		
		-- Drop staging table
		DROP TABLE IF EXISTS #stg_branches;

	-- ===============================================
	-- STEP 3: LOAD bronze.cbs_transactions
	-- ===============================================
		
		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @source_object = @path_transactions;
		SET @target_object = @target_transactions;
		SET @step_name = 'Load bronze.cbs_transactions';
		SET @step_status = 'Running';
		SET @load_type = 'Incremental: Append-Only';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			step_name,
			batch_id,
			load_type,
			source_object,
			target_object,
			start_time,
			step_status,
			rows_extracted,
			rows_inserted,
			rows_rejected
		)
		VALUES
		(
			@step_name,
			@batch_id,
			@load_type,
			@source_object,
			@target_object,
			@start_time,
			@step_status,
			@rows_extracted,
			@rows_inserted,
			@rows_rejected
		);
		-- Retrieve recently generated step_id
		SET @step_id = SCOPE_IDENTITY();

		-- Create temporary staging table
		CREATE TABLE #stg_transactions
		(
			transaction_id NVARCHAR(50),
			account_id NVARCHAR(50),
			transaction_type NVARCHAR(50),
			amount DECIMAL(18, 2),
			debit_credit NVARCHAR(50),
			currency NVARCHAR(50),
			transaction_date DATE,
			transaction_time TIME,
			transaction_date_time DATETIME2,
			channel NVARCHAR(50),
			[status] NVARCHAR(50),
			balance_after DECIMAL(18, 2),
			counterpart_account_id NVARCHAR(50),
			merchant_name NVARCHAR(50),
			merchant_category NVARCHAR(50),
			reference_number NVARCHAR(50),
			[description] NVARCHAR(500),
			branch_id NVARCHAR(50),
			is_flagged BIT,
			created_at DATETIME2
		);

		-- Map SQL query to variable
		SET @sql = 'BULK INSERT #stg_transactions FROM ''' + @source_object + ''' WITH (FIRSTROW = 2, FORMAT = ''CSV'', FIELDTERMINATOR = '','', 
		ROWTERMINATOR = ''0x0A'', CODEPAGE = ''65001'', TABLOCK, KEEPNULLS);';

		-- Execute SQL query
		EXEC (@sql);

		-- Load into bronze.cbs_transactions
		INSERT INTO bronze.cbs_transactions
		(
			transaction_id,
			account_id,
			transaction_type,
			amount,
			debit_credit,
			currency,
			transaction_date,
			transaction_time,
			transaction_date_time,
			channel,
			[status],
			balance_after,
			counterpart_account_id,
			merchant_name,
			merchant_category,
			reference_number,
			[description],
			branch_id,
			is_flagged,
			created_at,

			-- Metadata columns
			_source_system,
			_source_file,
			_batch_id,
			_load_timestamp
		)
		SELECT
			transaction_id,
			account_id,
			transaction_type,
			amount,
			debit_credit,
			currency,
			transaction_date,
			transaction_time,
			transaction_date_time,
			channel,
			[status],
			balance_after,
			counterpart_account_id,
			merchant_name,
			merchant_category,
			reference_number,
			[description],
			branch_id,
			is_flagged,
			created_at,

			-- Map values to metadata columns
			@source_system,
			@source_object,
			@batch_id,
			@start_time
		FROM #stg_transactions
		WHERE created_at > @wm_transactions;

		-- Map values to variables on success
		SET @rows_inserted = @@ROWCOUNT;
		SET @rows_extracted = @rows_inserted;
		SET @total_rows = @total_rows + @rows_inserted;
		SET @end_time = SYSDATETIME();
		SET @step_duration_seconds = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'Success';

		-- Update log details at watermark-level on success
		UPDATE etl.watermark
			SET
				source_object = @source_object,
				last_batch_id = @batch_id,
				last_loaded = @end_time
			WHERE source_system = @source_system AND target_object = @target_object;
		
		-- Update log details at step-level
		UPDATE etl.step_log
			SET 
				end_time = @end_time,
				load_duration_seconds = @step_duration_seconds,
				step_status = 'Success',
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;
		
		-- Drop staging table
		DROP TABLE IF EXISTS #stg_transactions;

	-- =======================================================================================
	-- SECTION 5: CLOSE BATCH ON SUCCESS
	-- =======================================================================================
		
		-- Map values to variables
		SET @batch_end_time = SYSDATETIME();
		SET @batch_duration_seconds = DATEDIFF(second, @batch_start_time, @batch_end_time);
		SET @batch_status = 'Success';

		-- Update log details at batch-level
		UPDATE etl.batch_log
			SET
				end_time = @batch_end_time,
				load_duration_seconds = @batch_duration_seconds,
				load_status = @batch_status,
				total_rows_processed = @total_rows
			WHERE batch_id = @batch_id;
	END TRY

	-- =======================================================================================
	-- SECTION 6: ERROR HANDLING
	-- =======================================================================================

	BEGIN CATCH
		-- Map values to variables on failure
		SET @step_status = 'Failed';
		SET @batch_status = 'Failed';
		IF @start_time IS NULL SET @start_time = SYSDATETIME();
		SET @end_time = SYSDATETIME();
		SET @step_duration_seconds = DATEDIFF(second, @start_time, @end_time);
		SET @batch_duration_seconds = DATEDIFF(second, @batch_start_time, @end_time);

		IF @rows_inserted IS NULL SET @rows_inserted = 0;
		IF @rows_extracted IS NULL SET @rows_extracted = 0;
		IF @total_rows IS NULL SET @total_rows = 0;

		SET @rows_rejected = 0;

		-- Update log details at batch-level on failure
		UPDATE etl.batch_log
			SET
				end_time = @end_time,
				load_duration_seconds = @batch_duration_seconds,
				load_status = @batch_status,
				total_rows_processed = @total_rows,
				err_message = ERROR_MESSAGE()
			WHERE batch_id = @batch_id;

		-- iF step_id is not NULL, update step log on failure
		IF @step_id IS NOT NULL
			BEGIN
				UPDATE etl.step_log
					SET
						end_time = @end_time,
						load_duration_seconds = @step_duration_seconds,
						step_status = @step_status,
						rows_extracted = @rows_extracted,
						rows_inserted = @rows_inserted,
						err_message = ERROR_MESSAGE()
					WHERE step_id = @step_id;
			END;
		
		-- Else insert new records into step log on failure
		ELSE
			BEGIN	
				INSERT INTO etl.step_log
				(
					step_name,
					batch_id,
					load_type,
					source_object,
					target_object,
					start_time,
					end_time,
					load_duration_seconds,
					step_status,
					rows_extracted,
					rows_inserted,
					rows_rejected,
					err_message
				)
				VALUES
				(
					COALESCE(@step_name, 'Unknown'),
					@batch_id,
					COALESCE(@load_type, 'Unknown'),
					COALESCE(@source_object, 'Unknown'),
					COALESCE(@target_object, 'Unknown'),
					@start_time,
					@end_time,
					@step_duration_seconds,
					@step_status,
					@rows_extracted,
					@rows_inserted,
					@rows_rejected,
					ERROR_MESSAGE()
				);
				-- Capture newly generated step_id on failure
				SET @step_id = SCOPE_IDENTITY();
			END;

		-- Insert into error log 
		INSERT INTO etl.error_log
		(
			batch_id,
			step_id,
			source_system,
			layer,
			source_object,
			target_object,
			error_description,
			rejected_at
		)
		VALUES
		(
			@batch_id,
			@step_id,
			@source_system,
			@layer,
			COALESCE(@source_object, 'Unknown'),
			COALESCE(@target_object, 'Unknown'),
			ERROR_MESSAGE(),
			@end_time
		);
		THROW;
	END CATCH;
END;
