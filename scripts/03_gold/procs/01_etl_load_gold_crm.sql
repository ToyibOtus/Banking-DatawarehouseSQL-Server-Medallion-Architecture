/*
===================================================================================
Script    : 01_etl_load_gold_crm
Location  : scripts/03_gold/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-30
Version   : 1.0
===================================================================================
Script Purpose:
    Loads all CRM records from the silver layer into corresponding gold layer.
	It performs data integrations where necessary. Additionally, it has an in-buit 
	logging system designed to track and monitor every ETL step, and thus enabling 
	easy debugging.

Tables Loaded:
	gold.dim_customers

Usage: EXEC etl.load_gold_crm
	
Parameter: None

===================================================================================
Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-03-30 |  Initial creation                                |
===================================================================================
*/
USE BankingDW;
GO

CREATE OR ALTER PROCEDURE etl.load_gold_crm AS
BEGIN
	-- Suppress number of rows affected	
	SET NOCOUNT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_gold_crm',
	@source_system NVARCHAR(50) = 'CRM',
	@layer NVARCHAR(50) = 'Gold',
	@batch_start_time DATETIME2,
	@batch_end_time DATETIME2,
	@batch_duration_seconds INT,
	@batch_status NVARCHAR(50) = 'Running',
	@total_rows INT = 0,
	@executed_by NVARCHAR(100) = SUSER_NAME(),

	-- Step-Level Variables (reused for each table)
	@step_id INT,
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
	@rows_updated INT,
	@rows_expired INT,
	@rows_rejected INT, 

	-- Last Batch ID (Retrieved from etl.watermark)
	@wm_customers INT,

	-- Source Name
	@source_customers NVARCHAR(50) = 'silver.crm_customers',

	-- target tables
	@target_customers NVARCHAR(50) = 'gold.dim_customers';


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
	SELECT @wm_customers = COALESCE(last_batch_id, 0) FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_customers;


	-- =======================================================================================
	-- SECTION 4: LOAD ALL GOLD TABLES
	-- =======================================================================================
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD gold.dim_customers
	-- ===============================================

		-- Map values to variables before transactions		
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load gold.dim_customers';
		SET @load_type = 'Incremental: SCD1 & SCD2';
		SET @source_object = @source_customers;
		SET @target_object = @target_customers;
		SET @step_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
		SET @rows_expired = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			batch_id,
			step_name,
			load_type,
			source_object,
			target_object,
			start_time,
			step_status,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_expired,
			rows_rejected
		)
		VALUES
		(
			@batch_id,
			@step_name,
			@load_type,
			@source_object,
			@target_object,
			@start_time,
			@step_status,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_expired,
			@rows_rejected
		);
		-- Retrieve recently generated step_id
		SET @step_id = SCOPE_IDENTITY();

		-- Load silver table into a temporary staging table
		SELECT
			customer_id,
			CONCAT(first_name, ' ', last_name) AS customer_name,
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
			onboarding_branch_id AS branch_id,
			customer_since,
			is_active,
			marketing_opt_in,
			preferred_language,
			annual_income,
			credit_score,
			created_at,
			updated_at
			INTO #stg_customers
		FROM silver.crm_customers
		WHERE _batch_id > @wm_customers;

		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;

		-- Overwrite outdated records, no new version created
		UPDATE tgt
			SET
				tgt.customer_name = src.customer_name,
				tgt.email = src.email,
				tgt.phone_number = src.phone_number,
				tgt.preferred_language = src.preferred_language,
				tgt.updated_at = src.updated_at,

				tgt._source_system = @source_system,
				tgt._batch_id = @batch_id,
				tgt._updated_at = @start_time
				FROM gold.dim_customers tgt
				INNER JOIN #stg_customers src
				ON tgt.customer_id = src.customer_id
				WHERE
					(COALESCE(tgt.customer_name, '') <> COALESCE(src.customer_name, '') OR
					COALESCE(tgt.email, '') <> COALESCE(src.email, '') OR
					COALESCE(tgt.phone_number, '') <> COALESCE(src.phone_number, '') OR
					COALESCE(tgt.preferred_language, '') <> COALESCE(src.preferred_language, ''))
					AND tgt._is_active = 1;

		-- Retrieve number of updated records
		SET @rows_updated = @@ROWCOUNT;
		
		
		-- Update metadata columns in outdated records
		UPDATE tgt
			SET
				tgt._source_system = @source_system,
				tgt._batch_id = @batch_id,
				tgt._updated_at = @start_time,
				tgt._valid_to = CAST(@start_time AS DATE),
				tgt._is_active = 0
				FROM gold.dim_customers tgt
				INNER JOIN #stg_customers src
				ON tgt.customer_id = src.customer_id
			WHERE 
				(COALESCE(tgt.company_name, '') <> COALESCE(src.company_name, '') OR
				COALESCE(tgt.segment, '') <> COALESCE(src.segment, '') OR
				COALESCE(tgt.risk_band, '') <> COALESCE(src.risk_band, '')  OR
				COALESCE(tgt.date_of_birth, '1900-01-01') <> COALESCE(src.date_of_birth, '1900-01-01') OR
				COALESCE(tgt.gender, '') <> COALESCE(src.gender, '') OR
				COALESCE(tgt.address_line_1, '') <> COALESCE(src.address_line_1, '') OR
				COALESCE(tgt.city, '') <> COALESCE(src.city, '') OR
				COALESCE(tgt.[state], '') <> COALESCE(src.[state], '') OR
				COALESCE(tgt.zip_code, '') <> COALESCE(src.zip_code, '') OR
				COALESCE(tgt.country, '') <> COALESCE(src.country, '') OR
				COALESCE(tgt.is_active, 0) <> COALESCE(src.is_active, 0) OR
				COALESCE(tgt.marketing_opt_in, 0) <> COALESCE(src.marketing_opt_in, 0) OR
				COALESCE(tgt.annual_income, 0) <> COALESCE(src.annual_income, 0) OR
				COALESCE(tgt.credit_score, 0) <> COALESCE(src.credit_score, 0))
				AND tgt._is_active = 1;


		-- Retrieve number of inactive records
		SET @rows_expired = @@ROWCOUNT;		

		-- Load updated records into gold table
		INSERT INTO gold.dim_customers
		(
			customer_id,
			customer_name,
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
			branch_id,
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
			_batch_id,
			_created_at,
			_valid_from,
			_is_active
		)
		SELECT
			src.customer_id,
			src.customer_name,
			src.company_name,
			src.segment,
			src.risk_band,
			src.date_of_birth,
			src.gender,
			src.national_id,
			src.email,
			src.phone_number,
			src.address_line_1,
			src.city,
			src.[state],
			src.zip_code,
			src.country,
			src.onboard_date,
			src.branch_id,
			src.customer_since,
			src.is_active,
			src.marketing_opt_in,
			src.preferred_language,
			src.annual_income,
			src.credit_score,
			src.created_at,
			src.updated_at,

			-- Map variables to metadata columns
			@source_system,
			@batch_id,
			@start_time,
			CAST(@start_time AS DATE),
			1
		FROM #stg_customers src
		WHERE EXISTS (SELECT 1 FROM gold.dim_customers tgt WHERE 
		tgt.customer_id = src.customer_id AND tgt._is_active = 0 AND tgt._batch_id = @batch_id);

		-- Retrive number of loaded records
		SET @rows_inserted = @@ROWCOUNT;

		-- Load new records into gold table
		INSERT INTO gold.dim_customers
		(
			customer_id,
			customer_name,
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
			branch_id,
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
			_batch_id,
			_created_at,
			_valid_from,
			_is_active
		)
		SELECT
			src.customer_id,
			src.customer_name,
			src.company_name,
			src.segment,
			src.risk_band,
			src.date_of_birth,
			src.gender,
			src.national_id,
			src.email,
			src.phone_number,
			src.address_line_1,
			src.city,
			src.[state],
			src.zip_code,
			src.country,
			src.onboard_date,
			src.branch_id,
			src.customer_since,
			src.is_active,
			src.marketing_opt_in,
			src.preferred_language,
			src.annual_income,
			src.credit_score,
			src.created_at,
			src.updated_at,

			-- Map variables to metadata columns
			@source_system,
			@batch_id,
			@start_time,
			CAST(@start_time AS DATE),
			1
		FROM #stg_customers src
		LEFT JOIN gold.dim_customers tgt
		ON src.customer_id = tgt.customer_id
		WHERE tgt.customer_id IS NULL;

		-- Map values to variables on success
		SET @rows_inserted = @rows_inserted + @@ROWCOUNT;
		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);
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
				rows_updated = @rows_updated,
				rows_expired = @rows_expired,
				rows_rejected = @rows_rejected
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
				end_time = @end_time,
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
		IF @start_time IS NULL SET @start_time = SYSDATETIME();
		SET @end_time = SYSDATETIME();
		SET @batch_duration_seconds = DATEDIFF(second, @batch_start_time, @end_time);
		SET @step_duration_seconds = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'Failed';
		SET @batch_status = 'Failed';

		IF @rows_extracted IS NULL SET @rows_extracted = 0;
		IF @rows_updated IS NULL SET @rows_updated = 0;
		IF @rows_expired IS NULL SET @rows_expired = 0;
		IF @rows_inserted IS NULL SET @rows_inserted = 0;

		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);

		SET @total_rows = @total_rows + @rows_extracted;

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
						rows_updated = @rows_updated,
						rows_expired = @rows_expired,
						rows_rejected = @rows_rejected,
						err_message = ERROR_MESSAGE()
					WHERE step_id = @step_id;
			END;
		-- Else insert new records into step log on failure
		ELSE
			BEGIN
				INSERT INTO etl.step_log
				(
					batch_id,
					step_name,
					load_type,
					source_object,
					target_object,
					start_time,
					end_time,
					load_duration_seconds,
					step_status,
					rows_extracted,
					rows_inserted,
					rows_updated,
					rows_expired,
					rows_rejected,
					err_message
				)
				VALUES
				(
					@batch_id,
					COALESCE(@step_name, 'N/A'),
					COALESCE(@load_type, 'N/A'),
					COALESCE(@source_object, 'N/A'),
					COALESCE(@target_object, 'N/A'),
					@start_time,
					@end_time,
					@step_duration_seconds,
					@step_status,
					@rows_extracted,
					@rows_inserted,
					@rows_updated,
					@rows_expired,
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
