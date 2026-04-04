/*
===================================================================================
Script    : 03_etl_load_bronze_hrms
Location  : scripts/01_bronze/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-18
Version   : 1.2
===================================================================================
Script Purpose:
    Loads all records from the source system (HRMS) into the bronze layer. It has
	an in-buit logging system designed to track and monitor every ETL step, and
	thus enabling easy debugging.

Tables Loaded:
	bronze.hrms_employees

Usage: EXEC etl.load_bronze_hrms
	
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

CREATE OR ALTER PROCEDURE etl.load_bronze_hrms AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;
	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_bronze_hrms',
	@source_system NVARCHAR(50) = 'HRMS',
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
	@path_employees NVARCHAR(200) = 'C:\SQLData\Finance Datasets\HRMS\employees.csv',

	-- target table
	@target_employees NVARCHAR(50) = 'bronze.hrms_employees',

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
	-- SECTION 4: LOAD ALL BRONZE TABLES
	-- =======================================================================================
	
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD bronze.hrms_employees
	-- ===============================================

		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load bronze.hrms_employees';
		SET @load_type = 'Full: Truncate & Insert';
		SET @source_object = @path_employees;
		SET @target_object = @target_employees;
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
		CREATE TABLE #stg_employees
		(
			employee_id NVARCHAR(50),
			first_name NVARCHAR(50),
			last_name NVARCHAR(50),
			email NVARCHAR(250),
			phone_number NVARCHAR(50),
			department NVARCHAR(50),
			job_title NVARCHAR(50),
			branch_id NVARCHAR(50),
			hire_date DATE,
			termination_date DATE,
			salary DECIMAL(18, 2),
			is_active BIT,
			manager_id NVARCHAR(50)
		);

		-- Map SQL query to variable
		SET @sql = 'BULK INSERT #stg_employees FROM ''' + @source_object + ''' WITH (FIRSTROW = 2, FORMAT = ''CSV'', FIELDTERMINATOR = '','', 
		ROWTERMINATOR = ''0x0A'', CODEPAGE = ''65001'', TABLOCK, KEEPNULLS);';

		-- Execute SQL query
		EXEC (@sql);

		-- Retrieve rows extracted from source
		SELECT @rows_extracted = COUNT(*) FROM #stg_employees;
		
		-- Delete data from bronze.hrms_employees
		TRUNCATE TABLE bronze.hrms_employees;

		-- Load bronze.hrms_employees
		INSERT INTO bronze.hrms_employees
		(
			employee_id,
			first_name,
			last_name,
			email,
			phone_number,
			department,
			job_title,
			branch_id,
			hire_date,
			termination_date,
			salary,
			is_active,
			manager_id,
			
			-- Metadata columns
			_source_system,
			_source_file,
			_batch_id,
			_load_timestamp
		)
		SELECT
			employee_id,
			first_name,
			last_name,
			email,
			phone_number,
			department,
			job_title,
			branch_id,
			hire_date,
			termination_date,
			salary,
			is_active,
			manager_id,

			-- Map values to metadata columns
			@source_system,
			@source_object,
			@batch_id,
			@start_time
		FROM #stg_employees;

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
		DROP TABLE IF EXISTS #stg_employees;

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
