/*
===================================================================================
Script    : 04_etl_load_bronze_los
Location  : scripts/01_bronze/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-18
Version   : 1.2
===================================================================================
Script Purpose:
    Loads all records from the source system (LOS) into the bronze layer. It has
	an in-buit logging system designed to track and monitor every ETL step, and
	thus enabling easy debugging.

Tables Loaded:
	bronze.los_loan_applications

Usage: EXEC etl.load_bronze_los
	
Parameter: None
===================================================================================
Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-03-18 |  Initial creation                                |
	|   1.1   |  2026-03-18 |  Fixed @source_object truncation, increased      |
	|         |             |  NVARCHAR(50) to NVARCHAR(200)                   |     
	|   1.2   |  2026-03-23 |  Mapped NULL to step_id across all steps         | 
===================================================================================
*/
USE BankingDW;
GO

CREATE OR ALTER PROCEDURE etl.load_bronze_los AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;
	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_bronze_los',
	@source_system NVARCHAR(50) = 'LOS',
	@layer NVARCHAR(50) = 'Bronze',
	@batch_start_time DATETIME2,
	@batch_end_time DATETIME2,
	@batch_duration_seconds INT,
	@batch_status NVARCHAR(50) = 'Running',
	@total_rows INT = 0,
	@executed_by NVARCHAR(50) = SUSER_NAME(),

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

	-- File path (Update to match environment)
	@path_loan_applications NVARCHAR(200) = 'C:\SQLData\Finance Datasets\LOS\loan_applications.csv',

	-- target table
	@target_loan_applications NVARCHAR(50) = 'bronze.los_loan_applications',

	-- Last loaded (Retrieved from etl.watermark)
	@wm_loan_applications DATETIME2,

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

	SELECT @wm_loan_applications = last_loaded FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_loan_applications;

	-- =======================================================================================
	-- SECTION 4: LOAD ALL BRONZE TABLES
	-- =======================================================================================
	
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD bronze.los_loan_applications
	-- ===============================================

		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load bronze.los_loan_applications';
		SET @load_type = 'Incremental: Append-Only';
		SET @source_object = @path_loan_applications;
		SET @target_object = @target_loan_applications;
		SET @step_status = 'Running';
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
		CREATE TABLE #stg_loan_applications
		(
			loan_id NVARCHAR(50),
			customer_id NVARCHAR(50),
			branch_id NVARCHAR(50),
			loan_officer_employee_id NVARCHAR(50),
			loan_type NVARCHAR(50),
			loan_status NVARCHAR(50),
			application_date DATE,
			decision_date DATE,
			disbursement_date DATE,
			requested_amount DECIMAL(18, 2),
			approved_amount DECIMAL(18, 2),
			disbursed_amount DECIMAL(18, 2),
			outstanding_balance DECIMAL(18, 2),
			interest_rate DECIMAL(12, 4),
			term_months INT,
			monthly_payment DECIMAL(18, 2),
			days_delinquent INT,
			collateral_type NVARCHAR(50),
			collateral_value DECIMAL(12, 2),
			purpose_description NVARCHAR(500),
			rejection_reason NVARCHAR(500),
			created_at DATETIME2,
			updated_at DATETIME2
		);

		-- Map SQL query to variable
		SET @sql = 'BULK INSERT #stg_loan_applications FROM ''' + @source_object + ''' WITH (FIRSTROW = 2, FORMAT = ''CSV'', FIELDTERMINATOR = '','', 
		ROWTERMINATOR = ''0x0A'', CODEPAGE = ''65001'', TABLOCK, KEEPNULLS);';

		-- Execute SQL query
		EXEC (@sql);

		-- Retrieve rows extracted from source
		SELECT @rows_extracted = COUNT(*) FROM #stg_loan_applications;

		-- Load into bronze.los_loan_applications
		INSERT INTO bronze.los_loan_applications
		(
			loan_id,
			customer_id,
			branch_id,
			loan_officer_employee_id,
			loan_type,
			loan_status,
			application_date,
			decision_date,
			disbursement_date,
			requested_amount,
			approved_amount,
			disbursed_amount,
			outstanding_balance,
			interest_rate,
			term_months,
			monthly_payment,
			days_delinquent,
			collateral_type,
			collateral_value,
			purpose_description,
			rejection_reason,
			created_at,
			updated_at,
			
			-- Metadata columns
			_source_system,
			_source_file,
			_batch_id,
			_load_timestamp
		)
		SELECT
			loan_id,
			customer_id,
			branch_id,
			loan_officer_employee_id,
			loan_type,
			loan_status,
			application_date,
			decision_date,
			disbursement_date,
			requested_amount,
			approved_amount,
			disbursed_amount,
			outstanding_balance,
			interest_rate,
			term_months,
			monthly_payment,
			days_delinquent,
			collateral_type,
			collateral_value,
			purpose_description,
			rejection_reason,
			created_at,
			updated_at,

			-- Map values to metadata columns
			@source_system,
			@source_object,
			@batch_id,
			@start_time
		FROM #stg_loan_applications
		WHERE updated_at > @wm_loan_applications;

		-- Map values to variables on success
		SET @rows_inserted = @@ROWCOUNT;
		SET @rows_rejected = @rows_extracted - @rows_inserted;
		SET @total_rows = @total_rows + @rows_extracted;
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
				step_status = @step_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #stg_loan_applications;

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

	BEGIN CATCH
		-- Map values to variables on failure
		SET @step_status = 'Failed';
		SET @batch_status = 'Failed';
		SET @end_time = SYSDATETIME();
		SET @step_duration_seconds = DATEDIFF(second, @start_time, @end_time);
		SET @batch_duration_seconds = DATEDIFF(second, @batch_start_time, @end_time);

		IF @rows_extracted IS NULL SET @rows_extracted = 0;
		IF @rows_inserted IS NULL SET @rows_inserted = 0;
		IF @total_rows IS NULL SET @total_rows = 0;

		SET @rows_rejected = @rows_extracted - @rows_inserted;
		
		-- Update log details at batch-level on failure
		UPDATE etl.batch_log
			SET
				end_time = @end_time,
				load_duration_seconds = @batch_duration_seconds,
				load_status = @batch_status,
				total_rows_processed = @total_rows,
				err_message = ERROR_MESSAGE()
			WHERE batch_id = @batch_id;
		
		-- If step_id is not NULL, update step log on failure
		IF @step_id IS NOT NULL
			BEGIN
				UPDATE etl.step_log
					SET
						end_time = @end_time,
						load_duration_seconds = @step_duration_seconds,
						step_status = @step_status,
						rows_extracted = @rows_extracted,
						rows_inserted = @rows_inserted,
						rows_rejected = @rows_rejected,
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
					COALESCE(@step_name, 'N/A'),
					@batch_id,
					COALESCE(@load_type, 'N/A'),
					COALESCE(@source_object, 'N/A'),
					COALESCE(@target_object, 'N/A'),
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
			COALESCE(@source_object, 'N/A'),
			COALESCE(@target_object, 'N/A'),
			ERROR_MESSAGE(),
			@end_time
		);
		THROW;
	END CATCH;
END;
