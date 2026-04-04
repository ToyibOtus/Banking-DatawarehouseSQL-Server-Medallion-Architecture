/*
===================================================================================
Script    : 04_etl_load_gold_los
Location  : scripts/03_gold/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-04-01
Version   : 1.0
===================================================================================
Script Purpose:
    Loads all LOS records from the silver layer into corresponding gold layer.
	It performs data integrations where necessary. Additionally, it has an in-buit 
	logging system designed to track and monitor every ETL step, and thus enabling 
	easy debugging.

Tables Loaded:
	gold.fact_loan_applications

Usage: EXEC etl.load_gold_los
	
Parameter: None

===================================================================================
Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-04-01 |  Initial creation                                |
===================================================================================
*/
USE BankingDW;
GO

CREATE OR ALTER PROCEDURE etl.load_gold_los AS
BEGIN
	-- Suppress number of rows affected	
	SET NOCOUNT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_gold_los',
	@source_system NVARCHAR(50) = 'LOS',
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
	@wm_loan_applications INT,

	-- Source Name
	@source_loan_applications NVARCHAR(50) = 'silver.los_loan_applications',

	-- target tables
	@target_loan_applications NVARCHAR(50) = 'gold.fact_loan_applications';


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
	SELECT @wm_loan_applications = COALESCE(last_batch_id, 0) FROM etl.watermark WHERE source_system = @source_system 
	AND target_object = @target_loan_applications;


	-- =======================================================================================
	-- SECTION 4: LOAD ALL GOLD TABLES
	-- =======================================================================================
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD gold.fact_loan_applications
	-- ===============================================

		-- Map values to variables before transactions		
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load gold.fact_loan_applications';
		SET @load_type = 'Incremental: Update & Insert';
		SET @source_object = @source_loan_applications;
		SET @target_object = @target_loan_applications;
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
			la.loan_id,
			dc.customer_key,
			db.branch_key,
			de.employee_key,
			la.loan_type,
			la.loan_status,
			la.application_date,
			la.decision_date,
			la.disbursement_date,
			la.requested_amount,
			la.approved_amount,
			la.disbursed_amount,
			la.amount_paid,
			la.outstanding_balance,
			la.interest_rate,
			la.term_months,
			la.monthly_payment,
			la.days_delinquent,
			la.collateral_type,
			la.collateral_value,
			la.purpose_description,
			la.rejection_reason,
			la.created_at,
			la.updated_at
			INTO #stg_loan_applications
		FROM silver.los_loan_applications la
		LEFT JOIN (SELECT customer_key, customer_id FROM gold.dim_customers WHERE _is_active = 1) dc
		ON la.customer_id = dc.customer_id
		LEFT JOIN gold.dim_branches db
		ON la.branch_id = db.branch_id
		LEFT JOIN gold.dim_employees de
		ON la.loan_officer_employee_id = de.employee_id
		WHERE la._batch_id > @wm_loan_applications;

		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;

		-- Overwrite outdated records
		UPDATE tgt
			SET
				tgt.customer_key = src.customer_key,
				tgt.branch_key = src.branch_key,
				tgt.employee_key = src.employee_key,
				tgt.loan_type = src.loan_type,
				tgt.loan_status = src.loan_status,
				tgt.application_date = src.application_date,
				tgt.decision_date = src.decision_date,
				tgt.disbursement_date = src.disbursement_date,
				tgt.requested_amount = src.requested_amount,
				tgt.approved_amount = src.approved_amount,
				tgt.disbursed_amount = src.disbursed_amount,
				tgt.amount_paid = src.amount_paid,
				tgt.outstanding_balance = src.outstanding_balance,
				tgt.interest_rate = src.interest_rate,
				tgt.term_months = src.term_months,
				tgt.monthly_payment = src.monthly_payment,
				tgt.days_delinquent = src.days_delinquent,
				tgt.collateral_type = src.collateral_type,
				tgt.collateral_value = src.collateral_value,
				tgt.purpose_description = src.purpose_description,
				tgt.rejection_reason = src.rejection_reason,
				tgt.updated_at = src.updated_at,

				tgt._source_system = @source_system,
				tgt._batch_id = @batch_id,
				tgt._updated_at = @start_time
				FROM gold.fact_loan_applications tgt
				INNER JOIN #stg_loan_applications src
				ON tgt.loan_id = src.loan_id
			WHERE
				COALESCE(tgt.customer_key, 0) <> COALESCE(src.customer_key, 0) OR
				COALESCE(tgt.branch_key, 0) <> COALESCE(src.branch_key, 0) OR
				COALESCE(tgt.employee_key, 0) <> COALESCE(src.employee_key, 0) OR
				COALESCE(tgt.loan_type, '') <> COALESCE(src.loan_type, '') OR
				COALESCE(tgt.loan_status, '') <> COALESCE(src.loan_status, '') OR
				COALESCE(tgt.application_date, '1900-01-01') <> COALESCE(src.application_date, '1900-01-01') OR
				COALESCE(tgt.decision_date, '1900-01-01') <> COALESCE(src.decision_date, '1900-01-01') OR
				COALESCE(tgt.disbursement_date, '1900-01-01') <> COALESCE(src.disbursement_date, '1900-01-01') OR
				COALESCE(tgt.requested_amount, 0) <> COALESCE(src.requested_amount, 0) OR
				COALESCE(tgt.approved_amount, 0) <> COALESCE(src.approved_amount, 0) OR
				COALESCE(tgt.disbursed_amount, 0) <> COALESCE(src.disbursed_amount, 0) OR
				COALESCE(tgt.amount_paid, 0) <> COALESCE(src.amount_paid, 0) OR
				COALESCE(tgt.outstanding_balance, 0) <> COALESCE(src.outstanding_balance, 0) OR
				COALESCE(tgt.interest_rate, 0) <> COALESCE(src.interest_rate, 0) OR
				COALESCE(tgt.term_months, 0) <> COALESCE(src.term_months, 0) OR
				COALESCE(tgt.monthly_payment, 0) <> COALESCE(src.monthly_payment, 0) OR
				COALESCE(tgt.days_delinquent, 0) <> COALESCE(src.days_delinquent, 0) OR
				COALESCE(tgt.collateral_type, '') <> COALESCE(src.collateral_type, '') OR
				COALESCE(tgt.collateral_value, 0) <> COALESCE(src.collateral_value, 0) OR
				COALESCE(tgt.purpose_description, '') <> COALESCE(src.purpose_description, '') OR
				COALESCE(tgt.rejection_reason, '') <> COALESCE(src.rejection_reason, '') OR
				COALESCE(tgt.updated_at, '1900-01-01') <> COALESCE(src.updated_at, '1900-01-01');

		-- Retrieve number of updated records
		SET @rows_updated = @@ROWCOUNT;


		-- Load new records into gold table
		INSERT INTO gold.fact_loan_applications
		(
			loan_id,
			customer_key,
			branch_key,
			employee_key,
			loan_type,
			loan_status,
			application_date,
			decision_date,
			disbursement_date,
			requested_amount,
			approved_amount,
			disbursed_amount,
			amount_paid,
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
			_batch_id,
			_created_at
		)
		SELECT
			src.loan_id,
			src.customer_key,
			src.branch_key,
			src.employee_key,
			src.loan_type,
			src.loan_status,
			src.application_date,
			src.decision_date,
			src.disbursement_date,
			src.requested_amount,
			src.approved_amount,
			src.disbursed_amount,
			src.amount_paid,
			src.outstanding_balance,
			src.interest_rate,
			src.term_months,
			src.monthly_payment,
			src.days_delinquent,
			src.collateral_type,
			src.collateral_value,
			src.purpose_description,
			src.rejection_reason,
			src.created_at,
			src.updated_at,

			-- Map variables to metadata columns
			@source_system,
			@batch_id,
			@start_time
		FROM #stg_loan_applications src
		LEFT JOIN gold.fact_loan_applications tgt
		ON src.loan_id = tgt.loan_id
		WHERE tgt.loan_id IS NULL;


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
