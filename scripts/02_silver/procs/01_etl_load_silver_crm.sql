/*
===================================================================================
Script    : 02_etl_load_silver_crm
Location  : scripts/02_silver/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-24
Version   : 1.1
===================================================================================
Script Purpose:
    Loads all CRM records from the bronze layer into corresponding silver layer.
	It performs series of data transformations, and data quality checks on the
	transformed data before loading. Additionally, it has an in-buit logging 
	system designed to track and monitor every ETL step, and thus enabling 
	easy debugging.

Tables Loaded:
	silver.crm_customers

Usage: EXEC etl.load_silver_crm
	
Parameter: None

Note: 
	It is imperative that every critical data quality rule is passed else
	transactions are aborted.
===================================================================================
Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-03-24 |  Initial creation                                |
	|   1.1   |  2026-03-28 |  Checked for invalid values in primary key in    |
	|         |             |  DQ monitor                                      |
===================================================================================
*/
USE BankingDW;
GO

CREATE OR ALTER PROCEDURE etl.load_silver_crm AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_silver_crm',
	@source_system NVARCHAR(50) = 'CRM',
	@layer NVARCHAR(50) = 'Silver',
	@batch_start_time DATETIME2,
	@batch_end_time DATETIME2,
	@batch_duration_seconds INT,
	@batch_status NVARCHAR(50) = 'Running',
	@total_rows INT = 0,
	@executed_by NVARCHAR(100) = SUSER_NAME(),

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
	@rows_updated INT,
	@rows_rejected INT,

	-- Last Batch ID (Retrieved from etl.watermark)
	@wm_customers INT,

	-- Source Name
	@source_customers NVARCHAR(50) = 'bronze.crm_customers',

	-- target tables
	@target_customers NVARCHAR(50) = 'silver.crm_customers';


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
	-- SECTION 4: LOAD ALL SILVER TABLES
	-- =======================================================================================
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD silver.crm_customers
	-- ===============================================

		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load silver.crm_customers';
		SET @load_type = 'Incremental: Update & Insert';
		SET @source_object = @source_customers;
		SET @target_object = @target_customers;
		SET @step_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
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
			@rows_rejected
		);
		-- Retrieve recently generated step_id
		SET @step_id = SCOPE_IDENTITY();

		-- Retrieve records from bronze table
		WITH base_query AS
		(
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
			_batch_id,
			ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY updated_at DESC) AS record_recency
		FROM bronze.crm_customers
		)
		-- Filter outdated records & perform series of data transformations
		, data_transformations AS
		(
		SELECT
			NULLIF(TRIM(customer_id), '') AS customer_id,
			first_name,
			last_name,
			CASE
				WHEN company_name IS NULL THEN 'N/A'
				ELSE company_name
			END AS company_name,
			segment,
			risk_band,
			date_of_birth,
			CASE 
				WHEN gender IS NULL THEN 'N/A'
				WHEN TRIM(UPPER(gender)) = 'M' THEN 'Male'
				WHEN TRIM(UPPER(gender)) = 'F' THEN 'Female'
				ELSE TRIM(gender)
			END AS gender,
			NULLIF(TRIM(national_id), '') AS national_id,
			CASE
				WHEN email NOT LIKE ('%@%.%') THEN NULL
				ELSE email
			END AS email,
			CASE 
				WHEN phone_number IS NULL THEN 'N/A'
				WHEN UPPER(phone_number) = 'UNKNOWN' THEN 'N/A'
				ELSE UPPER(phone_number)
			END AS phone_number,
			CASE
				WHEN address_line_1 IS NULL THEN 'N/A'
				ELSE address_line_1
			END AS address_line_1,
			city,
			[state],
			NULLIF(TRIM(zip_code), '') AS zip_code,
			country,
			onboard_date,
			NULLIF(TRIM(onboarding_branch_id), '') AS onboarding_branch_id,
			CASE 
				WHEN customer_since IS NULL AND onboard_date IS NOT NULL THEN YEAR(onboard_date)
				ELSE CAST(customer_since AS INT)
			END AS customer_since,
			is_active,
			marketing_opt_in, 
			CASE
				WHEN preferred_language IS NULL THEN 'N/A'
				ELSE TRIM(preferred_language)
			END AS preferred_language,
			annual_income,
			CAST(credit_score AS INT) AS credit_score,
			CAST(created_at AS DATE) AS created_at,
			CASE
				WHEN updated_at > SYSDATETIME() THEN NULL
				ELSE CAST(updated_at AS DATE)
			END AS updated_at,
			_batch_id
		FROM base_query
		WHERE record_recency = 1
		)
		-- Load transformed bronze table into a temporary staging table
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
			updated_at
			INTO #stg_customers
		FROM data_transformations
		WHERE _batch_id > @wm_customers;

		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table
		WITH dq_checks AS
		(
		SELECT
			COUNT(*) AS total_records,
			COUNT(CASE WHEN customer_id IS NULL THEN 1 ELSE NULL END) AS pk_null,
			COUNT(CASE WHEN customer_id NOT LIKE ('CUST%') THEN 1 ELSE NULL END) AS invalid_pk,
			COUNT(CASE WHEN date_of_birth > GETDATE() THEN 1 ELSE NULL END) AS invalid_dob,
			COUNT(CASE WHEN country NOT IN ('USA') THEN 1 ELSE NULL END) AS new_country,
			COUNT(CASE WHEN credit_score IS NOT NULL AND NOT credit_score BETWEEN 300 AND 850 THEN 1 ELSE NULL END) AS credit_score_out_of_range,
			COUNT(CASE WHEN customer_since IS NOT NULL AND YEAR(onboard_date) <> customer_since THEN 1 ELSE NULL END) AS invalid_log_date,
			COUNT(CASE WHEN customer_since > YEAR(GETDATE()) THEN 1 ELSE NULL END) AS invalid_customer_since,
			COUNT(CASE WHEN created_at > GETDATE() THEN 1 ELSE NULL END) AS invalid_created_at
		FROM #stg_customers
		)
		-- Load DQ checks in dq log table
		INSERT INTO etl.dq_log
		(
		batch_id,
		step_id,
		checked_at,
		source_system,
		layer,
		source_object,
		target_object,
		check_name,
		severity,
		records_checked,
		records_failed,
		dq_status,
		dq_description
		)		
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'PK Null', CASE WHEN pk_null > 0
		THEN 'Critical' ELSE NULL END, total_records, pk_null, CASE WHEN pk_null > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN pk_null > 0
		THEN 'Critical DQ Rule Violated: NULL(s) detected in primary key, customer_id. Transactions aborted.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid PK', CASE WHEN invalid_pk > 0 THEN 
		'Critical' ELSE NULL END, total_records, invalid_pk, CASE WHEN invalid_pk > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_pk > 0 THEN
		'Critical DQ Rule Violated: Invalid value(s) detected in primary key, customer_id. Transactions aborted.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid DOB', CASE WHEN invalid_dob > 0
		THEN 'Warning' ELSE NULL END, total_records, invalid_dob, CASE WHEN invalid_dob > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_dob > 0 
		THEN 'Mild DQ Rule Violated: Field, date_of_birth, contains date value(s) greater than present date.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'New Country', CASE WHEN new_country > 0
		THEN 'Info' ELSE NULL END, total_records, new_country, CASE WHEN new_country > 0 THEN 'Warning' ELSE 'Passed' END, CASE WHEN 
		new_country > 0 THEN 'Minor DQ Rule Violated: Non-USA country detected in field, country' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Credit Score', CASE WHEN 
		credit_score_out_of_range > 0 THEN 'Warning' ELSE NULL END, total_records, credit_score_out_of_range, CASE WHEN credit_score_out_of_range > 0 
		THEN 'Failed' ELSE 'Passed' END, CASE WHEN credit_score_out_of_range > 0 THEN 
		'Mild DQ Rule Violated: Field, credit_score, has value(s) outside the range of 300 - 850.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Log Date', CASE WHEN 
		invalid_log_date > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_log_date, CASE WHEN invalid_log_date > 0 THEN 'Failed' 
		ELSE 'Passed' END, CASE WHEN invalid_log_date > 0 THEN 
		'Mild DQ Rule Violated: Year of field, onboard_date, does not correspond with field, customer_since.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Year', CASE WHEN 
		invalid_customer_since > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_customer_since, CASE WHEN invalid_customer_since > 0 THEN 'Failed' 
		ELSE 'Passed' END, CASE WHEN invalid_customer_since > 0 THEN 
		'Mild DQ Rule Violated: Field, customer_since, has year(s) greater than the present year.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Creation Date', CASE WHEN 
		invalid_created_at > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_created_at, CASE WHEN invalid_created_at > 0 
		THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_created_at > 0 THEN 
		'Mild DQ Rule Violated: Field, created_at, has date value(s) greater than present date.' ELSE NULL END FROM dq_checks;


		IF EXISTS (SELECT 1 FROM etl.dq_log WHERE ((check_name = 'PK Null' AND records_failed > 0) OR (check_name = 'Invalid PK' AND records_failed > 0)) 
		AND (batch_id = @batch_id AND step_id = @step_id)) THROW 50004, 'Critical DQ Rule(s) Violated: Check etl.dq_log to see more information.', 4;

		-- Update outdated records in silver table
		UPDATE tgt
			SET
				tgt.first_name = src.first_name,
				tgt.last_name = src.last_name,
				tgt.company_name = src.company_name,
				tgt.segment = src.segment,
				tgt.risk_band = src.risk_band,
				tgt.date_of_birth = src.date_of_birth, 
				tgt.gender = src.gender,
				tgt.national_id = src.national_id,
				tgt.email = src.email,
				tgt.phone_number = src.phone_number,
				tgt.address_line_1 = src.address_line_1,
				tgt.city = src.city,
				tgt.[state] = src.[state],
				tgt.zip_code = src.zip_code,
				tgt.country = src.country,
				tgt.onboard_date = src.onboard_date,
				tgt.onboarding_branch_id = src.onboarding_branch_id,
				tgt.customer_since = src.customer_since,
				tgt.is_active = src.is_active,
				tgt.marketing_opt_in = src.marketing_opt_in,
				tgt.preferred_language = src.preferred_language,
				tgt.annual_income = src.annual_income,
				tgt.credit_score = src.credit_score,
				tgt.created_at = src.created_at,
				tgt.updated_at = src.updated_at,
				
				tgt._source_system = @source_system,
				tgt._batch_id = @batch_id,
				tgt._updated_at = @start_time
				FROM silver.crm_customers tgt
				INNER JOIN #stg_customers src
				ON tgt.customer_id = src.customer_id
			WHERE COALESCE(tgt.updated_at, '1900-01-01') < COALESCE(src.updated_at, '1900-01-01');

		-- Retrieve number of records updated
		SET @rows_updated = @@ROWCOUNT;

		-- Load new records into silver table
		INSERT INTO silver.crm_customers
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
			_batch_id,
			_created_at
		)
		SELECT
			src.customer_id,
			src.first_name,
			src.last_name,
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
			src.onboarding_branch_id,
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
			@start_time
		FROM #stg_customers src
		LEFT JOIN silver.crm_customers tgt
		ON src.customer_id = tgt.customer_id
		WHERE tgt.customer_id IS NULL;


		-- Map values to variables on success
		SET @rows_inserted = @@ROWCOUNT;
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
		IF @start_time IS NULL SET @start_time = SYSDATETIME();
		SET @end_time = SYSDATETIME();
		SET @step_duration_seconds = DATEDIFF(second, @start_time, @end_time);
		SET @batch_duration_seconds = DATEDIFF(second, @batch_start_time, @end_time);
		SET @step_status = 'Failed';
		SET @batch_status = 'Failed';

		IF @rows_extracted IS NULL SET @rows_extracted = 0;
		IF @rows_updated IS NULL SET @rows_updated = 0;
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
