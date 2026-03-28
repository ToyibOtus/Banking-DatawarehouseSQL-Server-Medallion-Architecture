/*
===================================================================================
Script    : 04_etl_load_silver_los
Location  : scripts/02_silver/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-28
Version   : 1.0
===================================================================================
Script Purpose:
    Loads all LOS records from the bronze layer into corresponding silver layer.
	It performs series of data transformations, and data quality checks on the
	transformed data before loading. Additionally, it has an in-buit logging 
	system designed to track and monitor every ETL step, and thus enabling 
	easy debugging.

Tables Loaded:
	silver.los_loan_applications

Usage: EXEC etl.load_silver_los
	
Parameter: None

Note: 
	It is imperative that every critical data quality rule is passed else
	transactions are aborted.
===================================================================================
Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-03-28 |  Initial creation                                |
===================================================================================
*/
USE BankingDW;
GO

CREATE OR ALTER PROCEDURE etl.load_silver_los AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_silver_los',
	@source_system NVARCHAR(50) = 'LOS',
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
	@wm_loan_applications INT,

	-- Source Name
	@source_loan_applications NVARCHAR(50) = 'bronze.los_loan_applications',

	-- target tables
	@target_loan_applications NVARCHAR(50) = 'silver.los_loan_applications';


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
	SELECT @wm_loan_applications = COALESCE(last_batch_id, 0) FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_loan_applications;


	-- =======================================================================================
	-- SECTION 4: LOAD ALL SILVER TABLES
	-- =======================================================================================
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD silver.los_loan_applications
	-- ===============================================

		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load silver.los_loan_applications';
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

		
		-- Retrieve records from bronze table
		WITH base_query AS
		(
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
			_batch_id,
			ROW_NUMBER() OVER(PARTITION BY loan_id ORDER BY updated_at DESC) AS record_recency
		FROM bronze.los_loan_applications
		)
		-- Filter outdated records & perform series of data transformations
		, data_transformations AS
		(
		SELECT
			NULLIF(TRIM(loan_id), '') AS loan_id,
			NULLIF(TRIM(customer_id), '') AS customer_id,
			NULLIF(TRIM(branch_id), '') AS branch_id,
			NULLIF(TRIM(loan_officer_employee_id), '') AS loan_officer_employee_id,
			loan_type,
			loan_status,
			application_date,
			decision_date,
			disbursement_date,
			requested_amount,
			CASE
				WHEN loan_status IN ('Closed', 'Disbursed', 'In Arrears', 'Defaulted', 'Approved') AND approved_amount IS NULL AND monthly_payment IS NOT NULL 
				THEN CAST((monthly_payment * (1 - (POWER((1 + (interest_rate/100/12)), -term_months))))/ (interest_rate/100/12) AS DECIMAL(18, 2))
				ELSE approved_amount
			END AS approved_amount,
			disbursed_amount,
			CASE 
				WHEN loan_status = 'Closed' THEN 0
				ELSE outstanding_balance 
			END AS outstanding_balance,
			CASE
				WHEN loan_status IN ('Pending', 'Rejected') THEN NULL
				ELSE interest_rate
			END AS interest_rate,
			CASE
				WHEN loan_status IN ('Pending', 'Rejected') THEN NULL
				ELSE term_months
			END AS term_months,
			CASE
				WHEN loan_status IN ('Pending', 'Rejected') THEN NULL
				ELSE monthly_payment
			END AS monthly_payment,
			days_delinquent,
			collateral_type,
			collateral_value, 
			CASE
				WHEN purpose_description IS NULL THEN 'Unknown'
				ELSE purpose_description
			END AS purpose_description,
			CASE
				WHEN loan_status = 'Rejected' AND rejection_reason IS NULL THEN 'Unknown'
				ELSE rejection_reason
			END AS rejection_reason,
			CAST(created_at AS DATE) AS created_at,
			CASE 
				WHEN updated_at > GETDATE() THEN NULL
				ELSE CAST(updated_at AS DATE)
			END AS updated_at,
			_batch_id
		FROM base_query
		WHERE record_recency = 1
		)
		-- Derive a new column, and perform additional transformations
		, metric_transformations AS
		(
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
			disbursed_amount - outstanding_balance AS amount_paid,
			outstanding_balance,
			interest_rate,
			term_months,
			CASE
				WHEN loan_status IN ('Closed', 'Approved', 'Disbursed', 'In Arrears', 'Defaulted') AND monthly_payment IS NULL 
				THEN CAST((approved_amount * (interest_rate/100/12))/ (1 - POWER((1 + (interest_rate/100/12)), -term_months)) AS DECIMAL(18, 2))
				ELSE monthly_payment
			END AS monthly_payment,
			days_delinquent,
			collateral_type,
			collateral_value,
			purpose_description,
			rejection_reason,
			created_at,
			updated_at,
			_batch_id
		FROM data_transformations
		)
		-- Load transformed bronze table into a temporary staging table
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
			updated_at
			INTO #stg_loan_applications
		FROM metric_transformations
		WHERE _batch_id > @wm_loan_applications;

		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;

		
		-- Perform data quality checks on staging table
		WITH dq_checks AS
		(
		SELECT
			COUNT(*) AS total_records,
			COUNT(CASE WHEN loan_id IS NULL THEN 1 ELSE NULL END) AS pk_null,
			COUNT(CASE WHEN loan_id NOT LIKE ('LN%') THEN 1 ELSE NULL END) AS invalid_pk,
			COUNT(CASE WHEN loan_status IN ('Closed', 'Disbursed', 'In Arrears', 'Defaulted', 'Approved') AND approved_amount IS NULL THEN 1 
			ELSE NULL END) AS approved_amount_null,
			COUNT(CASE WHEN loan_status IN ('Closed', 'Approved', 'Disbursed', 'In Arrears', 'Defaulted') AND monthly_payment IS NULL THEN 1
			ELSE NULL END) AS monthly_payment_null,
			COUNT(CASE WHEN loan_status IN ('Closed', 'Approved', 'Disbursed', 'In Arrears', 'Defaulted') AND monthly_payment - 
			((approved_amount * (interest_rate/100/12))/(1 - (POWER((1 + (interest_rate/100/12)), -term_months)))) > 1 THEN 1 ELSE NULL END) AS invalid_monthly_payment,
			COUNT(CASE WHEN application_date > decision_date OR application_date > disbursement_date OR application_date > GETDATE() THEN 1 
			ELSE NULL END) AS invalid_application_date,
			COUNT(CASE WHEN decision_date > disbursement_date OR decision_date > GETDATE() THEN 1 ELSE NULL END) AS invalid_decision_date,
			COUNT(CASE WHEN disbursement_date > GETDATE() THEN 1 ELSE NULL END) AS invalid_disbursed_date,
			COUNT(CASE WHEN created_at > updated_at OR created_at > GETDATE() THEN 1 ELSE NULL END) AS invalid_created_at
		FROM #stg_loan_applications
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
		'Critical DQ Rule Violated: NULL(s) detected in primary key, loan_id. Transactions aborted.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid PK', CASE WHEN invalid_pk > 0 THEN 
		'Critical' ELSE NULL END, total_records, invalid_pk, CASE WHEN invalid_pk > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_pk > 0 THEN
		'Critical DQ Rule Violated: Invalid value(s) detected in primary key, loan_id. Transactions aborted.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Approved Amount Null', CASE WHEN 
		approved_amount_null > 0 THEN 'Warning' ELSE NULL END, total_records, approved_amount_null, CASE WHEN approved_amount_null > 0 THEN 'Failed' 
		ELSE 'Passed' END, CASE WHEN approved_amount_null > 0 THEN
		'Mild DQ Rule Violated: Records with loan status neither "Rejected" nor "Pending" should have NULL(s) present in field, approved_amount.' 
		ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Monthly Payment Null', CASE WHEN 
		monthly_payment_null > 0 THEN 'Warning' ELSE NULL END, total_records, monthly_payment_null, CASE WHEN monthly_payment_null > 0 THEN 'Failed' 
		ELSE 'Passed' END, CASE WHEN monthly_payment_null > 0 THEN
		'Mild DQ Rule Violated: Records with loan status neither "Rejected" nor "Pending" should have NULL(s) present in field, monthly_payment.' 
		ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Monthly Payment', CASE WHEN 
		invalid_monthly_payment > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_monthly_payment, CASE WHEN invalid_monthly_payment > 0 
		THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_monthly_payment > 0 THEN
		'Mild DQ Rule Violated: Value(s) greater than set $1 margin of error detected in field, monthly_payment.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Application Date', CASE WHEN 
		invalid_application_date > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_application_date, CASE WHEN invalid_application_date > 0 
		THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_application_date > 0 THEN
		'Mild DQ Rule Violated: Field, application_date, has date value(s) greater than decision date, or disbursement date, or present date.' 
		ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Decision Date', CASE WHEN 
		invalid_decision_date > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_decision_date, CASE WHEN invalid_decision_date > 0 
		THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_decision_date > 0 THEN
		'Mild DQ Rule Violated: Field, decision_date, has date value(s) greater than disbursement date, or present date.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Disbursement Date', CASE WHEN 
		invalid_disbursed_date > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_disbursed_date, CASE WHEN invalid_disbursed_date > 0 
		THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_disbursed_date > 0 THEN
		'Mild DQ Rule Violated: Field, disbursement_date, has date value(s) greater than present date.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Creation Date', CASE WHEN 
		invalid_created_at > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_created_at, CASE WHEN invalid_created_at > 0 
		THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_created_at > 0 THEN
		'Mild DQ Rule Violated: Field, created_at, has date value(s) greater than present date.' ELSE NULL END FROM dq_checks;

		IF EXISTS (SELECT 1 FROM etl.dq_log WHERE ((check_name = 'PK Null' AND records_failed > 0) OR (check_name = 'Invalid PK' AND records_failed > 0)) 
		AND (batch_id = @batch_id AND step_id = @step_id)) THROW 50006, 'Critical DQ Rule(s) Violated: Check etl.dq_log to see more information.', 6;


		-- Update outdated records in silver table
		UPDATE tgt
			SET
				tgt.loan_id = src.loan_id,
				tgt.customer_id = src.customer_id,
				tgt.branch_id = src.branch_id,
				tgt.loan_officer_employee_id = src.loan_officer_employee_id,
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
				tgt.created_at = src.created_at,
				tgt.updated_at = src.updated_at,

				tgt._source_system = @source_system,
				tgt._batch_id = @batch_id,
				tgt._updated_at = @start_time
				FROM silver.los_loan_applications tgt
				INNER JOIN #stg_loan_applications src
				ON tgt.loan_id = src.loan_id
				WHERE
					COALESCE(tgt.updated_at, '1900-01-01') < COALESCE(src.updated_at, '1900-01-01');

		-- Retrieve number of records updated
		SET @rows_updated = @@ROWCOUNT;


		-- Load data into silver table
		INSERT INTO silver.los_loan_applications
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
			src.customer_id,
			src.branch_id,
			src.loan_officer_employee_id,
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
		LEFT JOIN silver.los_loan_applications tgt
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
