/*
===================================================================================
Script    : 03_etl_load_silver_hrms
Location  : scripts/02_silver/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-25
Version   : 1.0
===================================================================================
Script Purpose:
 	Loads all HRMS records from the bronze layer into corresponding silver layer.
	It performs series of data transformations, and data quality checks on the
	transformed data before loading. Additionally, it has an in-buit logging 
	system designed to track and monitor every ETL step, and thus enabling 
	easy debugging.

Tables Loaded:
	silver.hrms_employees

Usage: EXEC etl.load_silver_hrms
	
Parameter: None

Note: 
	It is imperative that every critical data quality rule is passed else
	transactions are aborted.
===================================================================================
Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-03-25 |  Initial creation                                |
===================================================================================
*/
USE BankingDW;
GO

CREATE OR ALTER PROCEDURE etl.load_silver_hrms AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_silver_hrms',
	@source_system NVARCHAR(50) = 'HRMS',
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
	@wm_employees INT,

	-- Source Name
	@source_employees NVARCHAR(50) = 'bronze.hrms_employees',

	-- target tables
	@target_employees NVARCHAR(50) = 'silver.hrms_employees';


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
	SELECT @wm_employees = COALESCE(last_batch_id, 0) FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_employees;


	-- =======================================================================================
	-- SECTION 4: LOAD ALL SILVER TABLES
	-- =======================================================================================
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD silver.hrms_employees
	-- ===============================================

		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load silver.hrms_employees';
		SET @load_type = 'Incremental: Update & Insert';
		SET @source_object = @source_employees;
		SET @target_object = @target_employees;
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
			_batch_id,
			ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY employee_id) AS record_number
		FROM bronze.hrms_employees
		)
		-- Filter outdated records & perform series of data transformations
		, data_transformations AS
		(
		SELECT
			NULLIF(TRIM(employee_id), '') AS employee_id,
			first_name,
			last_name,
			CASE
				WHEN LEN(email) < 6 THEN NULL
				WHEN email LIKE ('%@%') AND email NOT LIKE ('%firstnationalbank.%') THEN email + '.com'
				WHEN email NOT LIKE ('%@%') THEN REPLACE(REPLACE(email, 'firstnationalbank.com', ' firstnationalbank.com'), ' ', '@')
				ELSE email
			END AS email,
			CASE 
				WHEN phone_number IS NULL THEN 'Unknown'
				WHEN UPPER(phone_number) = 'N/A' THEN 'Unknown'
				WHEN UPPER(phone_number) = 'UNKNOWN' THEN 'Unknown'
				ELSE phone_number
			END AS phone_number,
			department,
			job_title,
			NULLIF(TRIM(branch_id), '') AS branch_id,
			hire_date,
			termination_date,
			salary,
			CASE
				WHEN is_active = 0 AND termination_date IS NULL THEN 1
				WHEN is_active = 0 AND termination_date > GETDATE() THEN 1
				WHEN is_active = 1 AND termination_date < GETDATE() THEN 0
				ELSE is_active
			END AS is_active,
			NULLIF(TRIM(manager_id), '') AS manager_id,
			_batch_id
		FROM base_query bq
		WHERE record_number = 1
		)
		-- Load transformed bronze table into a temporary staging table
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
			manager_id
			INTO #stg_employees
		FROM data_transformations
		WHERE _batch_id > @wm_employees;

		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;

		
		-- Perform data quality checks on staging table
		WITH dq_checks AS
		(
		SELECT
			COUNT(*) AS total_records,
			COUNT(CASE WHEN employee_id IS NULL THEN 1 ELSE NULL END) AS pk_null,
			COUNT(CASE WHEN branch_id IS NULL THEN 1 ELSE NULL END) AS branch_id_null,
			COUNT(CASE WHEN hire_date > termination_date OR hire_date > GETDATE() THEN 1 ELSE NULL END) AS invalid_hire_date
		FROM #stg_employees
		)
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
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'PK Null', CASE WHEN pk_null > 0 THEN
		'Critical' ELSE NULL END, total_records, pk_null, CASE WHEN pk_null > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN pk_null > 0 THEN
		'Critical DQ Rule Violated: NULL(s) detected in primary key, employee_id. Transactions aborted.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'FK Null', CASE WHEN branch_id_null > 0 THEN
		'Warning' ELSE NULL END, total_records, branch_id_null, CASE WHEN branch_id_null > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN 
		branch_id_null > 0 THEN 'Mild DQ Rule Violated: NULL(s) detected in foreign key, branch_id.' ELSE NULL END 
		FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Hire Date', CASE WHEN 
		invalid_hire_date > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_hire_date, CASE WHEN invalid_hire_date > 0 THEN 'Failed' 
		ELSE 'Passed' END, CASE WHEN invalid_hire_date > 0 THEN 
		'Mild DQ Rule Violated: Field, hire_date, has date value(s) greater than termination date or present date.' ELSE NULL END 
		FROM dq_checks;

		IF EXISTS (SELECT 1 FROM etl.dq_log WHERE (check_name = 'PK Null' AND records_failed > 0) AND (batch_id = @batch_id AND step_id = @step_id))
		THROW 50005, 'Critical DQ Rule(s) Violated: Check etl.dq_log to see more information.', 5;


		-- Update outdated records in silver table
		UPDATE tgt
			SET
				tgt.first_name = src.first_name,
				tgt.last_name = src.last_name,
				tgt.email = src.email,
				tgt.phone_number = src.phone_number,
				tgt.department = src.department,
				tgt.job_title = src.job_title,
				tgt.branch_id = src.branch_id,
				tgt.hire_date = src.hire_date,
				tgt.termination_date = src.termination_date,
				tgt.salary = src.salary,
				tgt.is_active = src.is_active,
				tgt.manager_id = src.manager_id,

				tgt._source_system = @source_system,
				tgt._batch_id = @batch_id,
				tgt._updated_at = @start_time
				FROM silver.hrms_employees tgt
				INNER JOIN #stg_employees src
				ON tgt.employee_id = src.employee_id
			WHERE 
				COALESCE(tgt.first_name, '') <> COALESCE(src.first_name, '') OR
				COALESCE(tgt.last_name, '') <> COALESCE(src.last_name, '') OR
				COALESCE(tgt.email, '') <> COALESCE(src.email, '') OR
				COALESCE(tgt.phone_number, '') <> COALESCE(src.phone_number, '') OR
				COALESCE(tgt.department, '') <> COALESCE(src.department, '') OR
				COALESCE(tgt.job_title, '') <> COALESCE(src.job_title, '') OR
				COALESCE(tgt.branch_id, '') <> COALESCE(src.branch_id, '') OR
				COALESCE(tgt.hire_date, '1900-01-01') <> COALESCE(src.hire_date, '1900-01-01') OR
				COALESCE(tgt.termination_date, '1900-01-01') <> COALESCE(src.termination_date, '1900-01-01') OR
				COALESCE(tgt.salary, 1) <> COALESCE(src.salary, 1) OR
				COALESCE(tgt.is_active, 0) <> COALESCE(src.is_active, 0) OR
				COALESCE(tgt.manager_id, '') <> COALESCE(src.manager_id, '')

		-- Retrieve number of records updated
		SET @rows_updated = @@ROWCOUNT;

		INSERT INTO silver.hrms_employees
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
			_batch_id,
			_created_at
		)
		SELECT
			src.employee_id,
			src.first_name,
			src.last_name,
			src.email,
			src.phone_number,
			src.department,
			src.job_title,
			src.branch_id,
			src.hire_date,
			src.termination_date,
			src.salary,
			src.is_active,
			src.manager_id,

			-- Map variables to metadata columns
			@source_system,
			@batch_id,
			@start_time
		FROM #stg_employees src
		LEFT JOIN silver.hrms_employees tgt
		ON src.employee_id = tgt.employee_id
		WHERE tgt.employee_id IS NULL;


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
					COALESCE(@step_name, 'Unknown'),
					COALESCE(@load_type, 'Unknown'),
					COALESCE(@source_object, 'Unknown'),
					COALESCE(@target_object, 'Unknown'),
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
			COALESCE(@source_object, 'Unknown'),
			COALESCE(@target_object, 'Unknown'),
			ERROR_MESSAGE(),
			@end_time
		);

		THROW;
	END CATCH;
END;
