/*
===================================================================================
Script    : 02_etl_load_bronze_crm
Location  : scripts/01_bronze/proc/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-18
Version   : 1.0
===================================================================================
Script Purpose:
    Loads all records from the source system (CRM) into the bronze layer. It has
	an in-buit logging system designed to track and monitor every ETL step, and
	thus enabling easy debugging.

	Tables Loaded:
		bronze.crm_customers
===================================================================================
  Change Log:
	 
	 | Version |     Date    |    Description                                   |
	 |---------|-------------|--------------------------------------------------|
	 | 	 1.0   |  2026-03-18 |  Initial creation                                |
===================================================================================
*/
USE BankingDW;
GO

CREATE OR ALTER PROCEDURE etl.load_bronze_crm AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;
	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_bronze_crm',
	@source_system NVARCHAR(50) = 'CRM',
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
	@source_object NVARCHAR(50),
	@target_object NVARCHAR(50),
	@start_time DATETIME2,
	@end_time DATETIME2,
	@step_duration_seconds INT,
	@step_status NVARCHAR(50),
	@rows_extracted INT,
	@rows_inserted INT,
	@rows_rejected INT,

	-- File path (Update to match environment)
	@path_customers NVARCHAR(200) = 'C:\SQLData\Finance Datasets\CRM\customers.csv',

	-- target table
	@target_customers NVARCHAR(50) = 'bronze.crm_customers',

	-- Last loaded (Retrieved from etl.watermark)
	@wm_customers DATETIME2,

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

	SELECT @wm_customers = last_loaded FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_customers;

	-- =======================================================================================
	-- SECTION 4: LOAD ALL BRONZE TABLES
	-- =======================================================================================
	
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD bronze.crm_customers
	-- ===============================================

		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @step_name = 'Load bronze.crm_customers';
		SET @load_type = 'Incremental: Append-Only';
		SET @source_object = @path_customers;
		SET @target_object = @target_customers;
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
		CREATE TABLE #stg_customers
		(
			customer_id NVARCHAR(50),
			first_name NVARCHAR(50),
			last_name NVARCHAR(50),
			company_name NVARCHAR(50),
			segment NVARCHAR(50),
			risk_band NVARCHAR(50),
			date_of_birth DATE,
			gender NVARCHAR(50),
			national_id NVARCHAR(50),
			email NVARCHAR(250),
			phone_number NVARCHAR(50),
			address_line_1 NVARCHAR(200),
			city NVARCHAR(50),
			[state] NVARCHAR(50),
			zip_code NVARCHAR(50),
			country NVARCHAR(50),
			onboard_date DATE,
			onboarding_branch_id NVARCHAR(50),
			customer_since DECIMAL(8, 1),
			is_active BIT,
			marketing_opt_in BIT,
			preferred_language NVARCHAR(50),
			annual_income DECIMAL(18, 2),
			credit_score DECIMAL(8, 1),
			created_at DATETIME2,
			updated_at DATETIME2
		);

		-- Map SQL query to variable
		SET @sql = 'BULK INSERT #stg_customers FROM ''' + @source_object + ''' WITH (FIRSTROW = 2, FORMAT = ''CSV'', FIELDTERMINATOR = '','', 
		ROWTERMINATOR = ''0x0A'', CODEPAGE = ''65001'', TABLOCK, KEEPNULLS);';

		-- Execute SQL query
		EXEC (@sql);

		-- Load bronze.crm_customers
		INSERT INTO bronze.crm_customers
		(
			customer_id,
			first_name,
			last_name,
			company_name,
			segment,
			risk_band,
			date_of_birth,
			gender,
			national_id,
			email,
			phone_number,
			address_line_1,
			city,
			[state],
			zip_code,
			country,
			onboard_date,
			onboarding_branch_id,
			customer_since,
			is_active,
			marketing_opt_in,
			preferred_language,
			annual_income,
			credit_score,
			created_at,
			updated_at,

			-- Metadata columns
			_source_system,
			_source_file,
			_batch_id,
			_load_timestamp
		)
		SELECT
			customer_id,
			first_name,
			last_name,
			company_name,
			segment,
			risk_band,
			date_of_birth,
			gender,
			national_id,
			email,
			phone_number,
			address_line_1,
			city,
			[state],
			zip_code,
			country,
			onboard_date,
			onboarding_branch_id,
			customer_since,
			is_active,
			marketing_opt_in,
			preferred_language,
			annual_income,
			credit_score,
			created_at,
			updated_at,

			-- Map values to metadata columns
			@source_system,
			@source_object,
			@batch_id,
			@start_time
		FROM #stg_customers
		WHERE updated_at > @wm_customers;

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
				step_status = @step_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #stg_customers;

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
						rows_inserted = @rows_inserted
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
					rows_rejected
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
					@rows_rejected
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
			COALESCE(@source_system, 'Unknown'),
			COALESCE(@layer, 'Unknown'),
			COALESCE(@source_object, 'Unknown'),
			COALESCE(@target_object, 'Unknown'),
			ERROR_MESSAGE(),
			@end_time
		);
		THROW;
	END CATCH;
END;
