/*
===================================================================================
Script    : 02_etl_load_gold_hrms
Location  : scripts/03_gold/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-30
Version   : 1.0
===================================================================================
Script Purpose:
    Loads all HRMS records from the silver layer into corresponding gold layer.
	It performs data integrations where necessary. Additionally, it has an in-buit 
	logging system designed to track and monitor every ETL step, and thus enabling 
	easy debugging.

Tables Loaded:
	gold.dim_employees

Usage: EXEC etl.load_gold_hrms
	
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
CREATE OR ALTER PROCEDURE etl.load_gold_hrms AS
BEGIN
	-- Suppress number of rows affected	
	SET NOCOUNT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_gold_hrms',
	@source_system NVARCHAR(50) = 'HRMS',
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
	@rows_rejected INT,

	-- Last Batch ID (Retrieved from etl.watermark)
	@wm_employees INT,

	-- Source Name
	@source_employees NVARCHAR(50) = 'silver.hrms_employees',

	-- target tables
	@target_employees NVARCHAR(50) = 'gold.dim_employees';


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
	-- SECTION 4: LOAD ALL GOLD TABLES
	-- =======================================================================================
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD gold.dim_employees
	-- ===============================================

		-- Map values to variables before transactions		
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load gold.dim_employees';
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


		-- Load silver table into a temporary staging table
		SELECT
			m.employee_id,
			CONCAT(m.first_name, ' ', m.last_name) AS employee_name,
			m.email,
			m.phone_number,
			m.department,
			m.job_title,
			m.branch_id,
			m.hire_date,
			m.termination_date,
			m.salary,
			m.is_active,
			m.manager_id,
			e.manager_name
			INTO #stg_employees
		FROM silver.hrms_employees m
		LEFT JOIN
		(SELECT employee_id, CONCAT(first_name, ' ', last_name) AS manager_name 
		FROM silver.hrms_employees) e
		ON m.manager_id = e.employee_id
		WHERE m._batch_id > @wm_employees;

		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;

		-- Overwrite outdated records
		UPDATE tgt
			SET
				tgt.employee_name = src.employee_name,
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
				tgt.manager_name = src.manager_name,

				tgt._source_system = @source_system,
				tgt._batch_id = @batch_id,
				tgt._updated_at = @start_time
				FROM gold.dim_employees tgt
				INNER JOIN #stg_employees src
				ON tgt.employee_id = src.employee_id
			WHERE 
					COALESCE(tgt.employee_name, '') <> COALESCE(src.employee_name, '') OR
					COALESCE(tgt.email, '') <> COALESCE(src.email, '') OR
					COALESCE(tgt.phone_number, '') <> COALESCE(src.phone_number, '')OR
					COALESCE(tgt.department, '') <> COALESCE(src.department, '') OR
					COALESCE(tgt.job_title, '') <> COALESCE(src.job_title, '') OR
					COALESCE(tgt.branch_id, '') <> COALESCE(src.branch_id, '')  OR
					COALESCE(tgt.hire_date, '1900-01-01') <> COALESCE(src.hire_date, '1900-01-01')OR
					COALESCE(tgt.termination_date, '1900-01-01') <> COALESCE(src.termination_date, '1900-01-01') OR
					COALESCE(tgt.salary, 0.00) <> COALESCE(src.salary, 0.00) OR
					COALESCE(tgt.is_active, 0) <> COALESCE(src.is_active, 0) OR 
					COALESCE(tgt.manager_id, '') <> COALESCE(src.manager_id, '') OR
					COALESCE(tgt.manager_name, '') <> COALESCE(src.manager_name, '');
		
		-- Retrieve number of updated records
		SET @rows_updated = @@ROWCOUNT;


		-- Load new records into gold table
		INSERT INTO gold.dim_employees
		(
			employee_id,
			employee_name,
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
			manager_name,

			-- Metadata columns
			_source_system,
			_batch_id,
			_created_at
		)
		SELECT
			src.employee_id,
			src.employee_name,
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
			src.manager_name,

			-- Map variables to metadata columns
			@source_system,
			@batch_id,
			@start_time
		FROM #stg_employees src
		LEFT JOIN gold.dim_employees tgt
		ON src.employee_id = tgt.employee_id
		WHERE tgt.employee_id IS NULL;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;


		-- Retrieve manager key where available
		UPDATE tgt
			SET
				tgt.manager_key = src.employee_key
				FROM gold.dim_employees tgt
				INNER JOIN
					(SELECT employee_id, employee_key FROM gold.dim_employees) src
				ON tgt.manager_id = src.employee_id
			WHERE _batch_id = @batch_id AND manager_key IS NULL AND manager_id IS NOT NULL;

		-- Map values to variables on success
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
			COALESCE(@source_system, 'N/A'),
			COALESCE(@layer, 'N/A'),
			COALESCE(@source_object, 'N/A'),
			COALESCE(@target_object, 'N/A'),
			ERROR_MESSAGE(),
			@end_time
		);

		THROW;
	END CATCH;
END;
