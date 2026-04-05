/*
===================================================================================
Script    : 01_etl_load_silver_cbs
Location  : scripts/02_silver/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-23
Version   : 1.1
===================================================================================
Script Purpose:
    Loads all CBS records from the bronze layer into corresponding silver layer.
	It performs series of data transformations, and data quality checks on the
	transformed data before loading. Additionally, it has an in-buit logging 
	system designed to track and monitor every ETL step, and thus enabling 
	easy debugging.

Tables Loaded:
	silver.cbs_accounts
	silver.cbs_branches
	silver.cbs_transactions

Usage: EXEC etl.load_silver_cbs
	
Parameter: None

Note: 
	It is imperative that every critical data quality rule is passed else
	transactions are aborted.
===================================================================================
Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-03-23 |  Initial creation                                |
	|   1.1   |  2026-03-28 |  Checked for invalid values in primary key in    |
	|         |             |  DQ monitor                                      |
===================================================================================
*/
USE BankingDW;
GO

CREATE OR ALTER PROCEDURE etl.load_silver_cbs AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_silver_cbs',
	@source_system NVARCHAR(50) = 'CBS',
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
	@wm_accounts INT,
	@wm_branches INT,
	@wm_transactions INT,

	-- Source Name
	@source_accounts NVARCHAR(50) = 'bronze.cbs_accounts',
	@source_branches NVARCHAR(50) = 'bronze.cbs_branches',
	@source_transactions NVARCHAR(50) = 'bronze.cbs_transactions',

	-- target tables
	@target_accounts NVARCHAR(50) = 'silver.cbs_accounts',
	@target_branches NVARCHAR(50) = 'silver.cbs_branches',
	@target_transactions NVARCHAR(50) = 'silver.cbs_transactions';


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
	SELECT @wm_accounts = COALESCE(last_batch_id, 0) FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_accounts;

	SELECT @wm_branches = COALESCE(last_batch_id, 0) FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_branches;

	SELECT @wm_transactions = COALESCE(last_batch_id, 0) FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_transactions;


	-- =======================================================================================
	-- SECTION 4: LOAD ALL SILVER TABLES
	-- =======================================================================================
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD silver.cbs_accounts
	-- ===============================================

		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load silver.cbs_accounts';
		SET @load_type = 'Incremental: Update & Insert';
		SET @source_object = @source_accounts;
		SET @target_object = @target_accounts;
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
			_batch_id,
			ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY updated_at DESC) AS record_recency
		FROM bronze.cbs_accounts
		)
		-- Retrieve only records with valid customers & perform relevant transformations
		, data_transformations AS
		(
		SELECT
			NULLIF(TRIM(account_id), '') AS account_id,
			NULLIF(TRIM(customer_id), '') AS customer_id,
			account_type,
			CASE
				WHEN account_status IS NULL AND (close_date IS NOT NULL AND close_date <= GETDATE()) THEN 'Closed'
				WHEN account_status IS NULL THEN 'N/A'
				ELSE account_status
			END AS account_status,
			open_date,
			CASE
				WHEN close_date > GETDATE() AND account_status = 'Closed' THEN NULL
				ELSE close_date
			END AS close_date,
			currency_code,
			current_balance,
			CASE 
				WHEN available_balance IS NULL THEN current_balance + overdraft_limit
				ELSE available_balance
			END AS available_balance,
			overdraft_limit,
			interest_rate,
			NULLIF(TRIM(branch_id), '') AS branch_id,
			NULLIF(TRIM(assigned_employee_id), '') AS assigned_employee_id,
			is_primary,
			CAST(created_at AS DATE) AS created_at,
			CASE
				WHEN updated_at > SYSDATETIME() THEN NULL
				ELSE CAST(updated_at AS DATE)
			END AS updated_at,
			_batch_id
		FROM base_query
		WHERE record_recency = 1 AND customer_id IN (SELECT customer_id FROM silver.crm_customers)
		)
		-- Load transformed bronze table into a temporary staging table
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
			updated_at
			INTO #stg_accounts
		FROM data_transformations
		WHERE _batch_id > @wm_accounts;
		
		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table
		WITH dq_checks AS
		(
		SELECT
			COUNT(*) AS total_records,
			COUNT(CASE WHEN account_id IS NULL THEN 1 ELSE NULL END) AS pk_null,
			COUNT(CASE WHEN account_id NOT LIKE ('ACC%') THEN 1 ELSE NULL END) AS invalid_pk,
			COUNT(CASE WHEN open_date > close_date OR open_date > GETDATE() THEN 1 ELSE NULL END) AS invalid_open_date,
			COUNT(CASE WHEN created_at > updated_at OR created_at > GETDATE() THEN 1 ELSE NULL END) AS invalid_created_at,
			COUNT(CASE WHEN currency_code IS NULL OR TRIM(currency_code) = '' THEN 1 ELSE NULL END) AS invalid_currency_code,
			COUNT(CASE WHEN currency_code NOT IN ('CAD', 'USD', 'GBP', 'EUR') THEN 1 ELSE NULL END) AS new_currency_code,
			COUNT(CASE WHEN available_balance IS NULL OR available_balance < 0 THEN 1 ELSE NULL END) AS invalid_available_balance,
			COUNT(CASE WHEN current_balance IS NULL OR current_balance <> available_balance - overdraft_limit THEN 1 ELSE NULL END) AS invalid_current_balance,
			COUNT(CASE WHEN overdraft_limit IS NULL OR overdraft_limit < 0 OR overdraft_limit <> available_balance - current_balance THEN 1 ELSE NULL END) AS invalid_overdraft_limit,
			COUNT(CASE WHEN interest_rate IS NULL OR interest_rate < 0 OR (account_type IN ('Business Checking', 'Checking') AND interest_rate > 0) OR interest_rate > 25 THEN 1 ELSE NULL END) AS invalid_interest_rate
		FROM #stg_accounts
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
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'PK Null', 
		CASE WHEN pk_null > 0 THEN 'Critical' ELSE NULL END, total_records, pk_null, CASE WHEN pk_null > 0 THEN 'Failed' 
		ELSE 'Passed' END, CASE WHEN pk_null > 0 THEN 
		'Critical DQ Rule Violated: NULL(s) detected in Primary Key, account_id. Transactions aborted.' ELSE NULL END  FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid PK', CASE WHEN invalid_pk > 0 THEN 
		'Critical' ELSE NULL END, total_records, invalid_pk, CASE WHEN invalid_pk > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_pk > 0 THEN
		'Critical DQ Rule Violated: Invalid value(s) detected in primary key, account_id. Transactions aborted.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Open Date', 
		CASE WHEN invalid_open_date > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_open_date, CASE WHEN invalid_open_date > 0 THEN 'Failed' 
		ELSE 'Passed' END, CASE WHEN invalid_open_date > 0 THEN 
		'Mild DQ Rule Violated: Field, open_date, has date value(s) greater than closed date or present date.' ELSE NULL END  FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Creation Date', 
		CASE WHEN invalid_created_at > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_created_at, CASE WHEN invalid_created_at > 0 THEN 'Failed' 
		ELSE 'Passed' END, CASE WHEN invalid_created_at > 0 THEN 
		'Mild DQ Rule Violated: Field, created_at, has date value(s) greater than present date.' ELSE NULL END  FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Currency Code', 
		CASE WHEN invalid_currency_code > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_currency_code, CASE WHEN invalid_currency_code > 0 THEN 'Failed' 
		ELSE 'Passed' END, CASE WHEN invalid_currency_code > 0 THEN 
		'Mild DQ Rule Violated: NULL(s), empty spaces, or an empty string detected in field, currency_code.' ELSE NULL END  FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'New Currency Code', 
		CASE WHEN new_currency_code > 0 THEN 'Info' ELSE NULL END, total_records, new_currency_code, CASE WHEN new_currency_code > 0 THEN 'Warning' ELSE 
		'Passed' END, CASE WHEN new_currency_code > 0 THEN 'Minor DQ Rule Violated: New currency code(s) detected in currency_code' ELSE NULL END  
		FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Available Balance', 
		CASE WHEN invalid_available_balance > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_available_balance, CASE WHEN invalid_available_balance > 0 
		THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_available_balance > 0 THEN 
		'Mild DQ Rule Violated: Invalid value(s) detected in available_balance.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Current Balance', 
		CASE WHEN invalid_current_balance > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_current_balance, 
		CASE WHEN invalid_current_balance > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_current_balance > 0 THEN 
		'Mild DQ Rule Violated: Invalid value(s) detected in current_balance.' ELSE NULL END 
		FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Overdraft Limit', 
		CASE WHEN invalid_overdraft_limit > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_overdraft_limit, 
		CASE WHEN invalid_overdraft_limit > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_overdraft_limit > 0 THEN 
		'Mild DQ Rule Violated: Invalid value(s) detected in overdraft_limit.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Interest Rate', 
		CASE WHEN invalid_interest_rate > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_interest_rate, CASE WHEN invalid_interest_rate > 0 THEN 
		'Failed' ELSE 'Passed' END, CASE WHEN invalid_interest_rate > 0 THEN 'Mild DQ Rule Violated: Invalid value(s) detected in interest_rate' ELSE NULL END 
		FROM dq_checks;

		-- Throw error when critical DQ rule is violated
		IF EXISTS (SELECT 1 FROM etl.dq_log WHERE ((check_name = 'PK Null' AND records_failed > 0) OR (check_name = 'Invalid PK' AND records_failed > 0)) 
		AND (batch_id = @batch_id AND step_id = @step_id)) THROW 50001, 'Critical DQ Rule(s) Violated: Check etl.dq_log to see more information.', 1;

		-- Update outdated records in silver table
		UPDATE tgt
			SET
				tgt.account_id = src.account_id,
				tgt.customer_id = src.customer_id,
				tgt.account_type = src.account_type,
				tgt.account_status = src.account_status,
				tgt.open_date = src.open_date,
				tgt.close_date = src.close_date,
				tgt.currency_code = src.currency_code,
				tgt.current_balance = src.current_balance,
				tgt.available_balance = src.available_balance,
				tgt.overdraft_limit = src.overdraft_limit,
				tgt.interest_rate = src.interest_rate,
				tgt.branch_id = src.branch_id,
				tgt.assigned_employee_id = src.assigned_employee_id,
				tgt.is_primary = src.is_primary,
				tgt.created_at = src.created_at,
				tgt.updated_at = src.updated_at,

				tgt._source_system = @source_system,
				tgt._batch_id = @batch_id,
				tgt._updated_at = @start_time
				FROM silver.cbs_accounts tgt
				INNER JOIN #stg_accounts src
				ON tgt.account_id = src.account_id
			WHERE COALESCE(tgt.updated_at, '1900-01-01') < COALESCE(src.updated_at, '1900-01-01');
		
		-- Retrieve number of records updated
		SET @rows_updated = @@ROWCOUNT;

		-- Load new records into silver table
		INSERT INTO silver.cbs_accounts
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
			_batch_id,
			_created_at
		)
		SELECT
			src.account_id,
			src.customer_id,
			src.account_type,
			src.account_status,
			src.open_date,
			src.close_date,
			src.currency_code,
			src.current_balance,
			src.available_balance,
			src.overdraft_limit,
			src.interest_rate,
			src.branch_id,
			src.assigned_employee_id,
			src.is_primary,
			src.created_at,
			src.updated_at,

			-- Map values to metadata columns
			@source_system,
			@batch_id,
			@start_time
		FROM #stg_accounts src
		LEFT JOIN silver.cbs_accounts tgt
		ON src.account_id = tgt.account_id
		WHERE tgt.account_id IS NULL;

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
		DROP TABLE IF EXISTS #stg_accounts;


	-- ===============================================
	-- STEP 2: LOAD silver.cbs_branches
	-- ===============================================

		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load silver.cbs_branches';
		SET @load_type = 'Incremental: Update & Insert';
		SET @source_object = @source_branches;
		SET @target_object = @target_branches;
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

		-- Perform relevant data transformations on bronze table
		WITH data_transformations AS
		(
		SELECT
			NULLIF(TRIM(branch_id), '') AS branch_id,
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
			NULLIF(TRIM(manager_employee_id), '') AS manager_employee_id,
			_batch_id
		FROM bronze.cbs_branches
		)
		-- Load transformed bronze table into a temporary staging table
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
			manager_employee_id
			INTO #stg_branches
		FROM data_transformations
		WHERE _batch_id > @wm_branches;

		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;


		-- Perform data quality checks on staging table
		WITH dq_checks AS
		(
		SELECT
			COUNT(*) AS total_records,
			COUNT(*) - COUNT (DISTINCT branch_id) AS pk_duplicates,
			COUNT(CASE WHEN branch_id IS NULL THEN 1 ELSE NULL END) AS pk_null,
			COUNT(CASE WHEN branch_id NOT LIKE ('BRN%') THEN 1 ELSE NULL END) AS invalid_pk,
			COUNT(CASE WHEN manager_employee_id IS NULL THEN 1 ELSE NULL END) AS employee_id_null,
			COUNT(CASE WHEN opened_date > GETDATE() THEN 1 ELSE NULL END) AS invalid_opened_date,
			COUNT(CASE WHEN country IS NULL OR TRIM(country) = '' THEN 1 ELSE NULL END) AS invalid_country,
			COUNT(CASE WHEN country NOT IN ('USA') THEN 1 ELSE NULL END) AS country_new
		FROM #stg_branches
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
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'PK Duplicate', CASE WHEN 
		pk_duplicates > 0 THEN 'Critical' ELSE NULL END, total_records, pk_duplicates, CASE WHEN pk_duplicates > 0 THEN 'Failed' ELSE 'Passed' END, 
		CASE WHEN pk_duplicates > 0 THEN 'Critical DQ Rule Violated: Primary key, branch_id, contains duplicates. Transactions aborted.' ELSE NULL END 
		FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'PK Null', CASE WHEN 
		pk_null > 0 THEN 'Critical' ELSE NULL END, total_records, pk_null, CASE WHEN pk_null > 0 THEN 'Failed' ELSE 'Passed' END, 
		CASE WHEN pk_null > 0 THEN 'Critical DQ Rule Violated: NULL(s) detected in primary key, branch_id. Transactions aborted.' 
		ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid PK', CASE WHEN invalid_pk > 0 THEN 
		'Critical' ELSE NULL END, total_records, invalid_pk, CASE WHEN invalid_pk > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_pk > 0 THEN
		'Critical DQ Rule Violated: Invalid value(s) detected in primary key, branch_id. Transactions aborted.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'FK Null', CASE WHEN 
		employee_id_null > 0 THEN 'Warning' ELSE NULL END, total_records, employee_id_null, CASE WHEN employee_id_null > 0 THEN 'Failed' ELSE 'Passed' END, 
		CASE WHEN employee_id_null > 0 THEN 'Mild DQ Rule Violated: NULL(s) detected in Foreign key, manager_employee_id. Transactions aborted.' 
		ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Opened Date', CASE WHEN 
		invalid_opened_date > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_opened_date, CASE WHEN invalid_opened_date > 0 THEN 'Failed' 
		ELSE 'Passed' END, CASE WHEN invalid_opened_date > 0 THEN 
		'Mild DQ Rule Violated: Field, opened_date, has date value(s) greater than present date' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Country ', CASE WHEN 
		invalid_country > 0 THEN 'Warning' ELSE NULL END, total_records,invalid_country, CASE WHEN invalid_country > 0 THEN 'Failed' ELSE 'Passed' END, 
		CASE WHEN invalid_country > 0 THEN 
		'Mild DQ Rule Violated: NULL(s), empty spaces, or an empty string detected in field, country.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'New Country', CASE WHEN 
		country_new > 0 THEN 'Info' ELSE NULL END, total_records, country_new, CASE WHEN country_new > 0 THEN 'Warning' ELSE 'Passed' END, 
		CASE WHEN country_new > 0 THEN 
		'Minor DQ Rule Violated: New value(s) detected in field, country.' ELSE NULL END FROM dq_checks;

		-- Throw error when critical DQ rule is violated
		IF EXISTS (SELECT 1 FROM etl.dq_log WHERE ((check_name = 'PK Duplicate' AND records_failed > 0) OR (check_name = 'PK Null' AND records_failed > 0) 
		OR (check_name = 'Invalid PK' AND records_failed > 0)) AND (batch_id = @batch_id AND step_id = @step_id)) 
		THROW 50002, 'Critical DQ Rule(s) Violated: Check etl.dq_log to see more information.', 2;

		-- Update outdated records in silver table
		UPDATE tgt
			SET
					tgt.branch_name = src.branch_name,
					tgt.branch_type = src.branch_type,
					tgt.address_line_1 = src.address_line_1,
					tgt.city = src.city,
					tgt.[state] = src.[state],
					tgt.zip_code = src.zip_code,
					tgt.country = src.country,
					tgt.phone_number = src.phone_number,
					tgt.email = src.email,
					tgt.opened_date = src.opened_date,
					tgt.is_active = src.is_active,
					tgt.region = src.region,
					tgt.manager_employee_id = src.manager_employee_id,

					_source_system = @source_system,
					_batch_id = @batch_id,
					_updated_at = @start_time
				FROM silver.cbs_branches tgt
				INNER JOIN #stg_branches src
				ON tgt.branch_id = src.branch_id
				WHERE 
					COALESCE(tgt.branch_name, '') <> COALESCE(src.branch_name, '') OR
					COALESCE(tgt.branch_type, '') <> COALESCE(src.branch_type, '') OR
					COALESCE(tgt.address_line_1, '') <> COALESCE(src.address_line_1, '') OR
					COALESCE(tgt.city, '') <> COALESCE(src.city, '') OR
					COALESCE(tgt.[state], '') <> COALESCE(src.[state], '') OR
					COALESCE(tgt.zip_code, '') <> COALESCE(src.zip_code, '') OR
					COALESCE(tgt.country, '') <> COALESCE(src.country, '') OR
					COALESCE(tgt.phone_number, '') <> COALESCE(src.phone_number, '') OR
					COALESCE(tgt.email, '') <> COALESCE(src.email, '') OR
					COALESCE(tgt.opened_date, '1900-01-01') <> COALESCE(src.opened_date, '1900-01-01') OR
					COALESCE(tgt.is_active, 0) <> COALESCE(src.is_active, 0) OR
					COALESCE(tgt.region, '') <> COALESCE(src.region, '') OR
					COALESCE(tgt.manager_employee_id, '') <> COALESCE(src.manager_employee_id, '');
		
		-- Retrieve number of records updated
		SET @rows_updated = @@ROWCOUNT;

		-- Load new records into silver table
		INSERT INTO silver.cbs_branches
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
			_batch_id,
			_created_at
		)
		SELECT
			src.branch_id,
			src.branch_name,
			src.branch_type,
			src.address_line_1,
			src.city,
			src.[state],
			src.zip_code,
			src.country,
			src.phone_number,
			src.email,
			src.opened_date,
			src.is_active,
			src.region,
			src.manager_employee_id,

			-- Map values to metadata columns
			@source_system,
			@batch_id,
			@start_time
		FROM #stg_branches src
		LEFT JOIN silver.cbs_branches tgt
		ON src.branch_id = tgt.branch_id
		WHERE tgt.branch_id IS NULL;

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
		DROP TABLE IF EXISTS #stg_branches;


	-- ===============================================
	-- STEP 3: LOAD silver.cbs_transactions
	-- ===============================================
		
		-- Map values to variables before transactions
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load silver.cbs_transactions';
		SET @load_type = 'Incremental: Insert';
		SET @source_object = @source_transactions;
		SET @target_object = @target_transactions;
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
			_batch_id,
			ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY created_at DESC) AS record_recency
		FROM bronze.cbs_transactions
		)
		-- Filter outdated records & perform relevant transformations
		, data_transformations AS
		(
		SELECT
			NULLIF(TRIM(transaction_id), '') AS transaction_id,
			NULLIF(TRIM(account_id), '') AS account_id,
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
			NULLIF(TRIM(counterpart_account_id), '') AS counterpart_account_id,
			CASE
				WHEN merchant_name IS NULL THEN 'N/A'
				ELSE merchant_name
			END AS merchant_name,
			CASE
				WHEN merchant_category IS NULL THEN 'N/A'
				ELSE merchant_category
			END AS merchant_category,
			CASE
				WHEN reference_number IS NULL THEN 'N/A'
				ELSE reference_number
			END AS reference_number,
			CASE
				WHEN [description] IS NULL THEN 'N/A'
				ELSE [description]
			END AS [description],
			NULLIF(TRIM(branch_id), '') AS branch_id,
			is_flagged,
			created_at,
			_batch_id
		FROM base_query
		WHERE record_recency = 1 
		AND account_id IN (SELECT account_id FROM silver.cbs_accounts)
		)
		-- Load transformed records from bronze into a temporary staging table
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
			_batch_id
			INTO #stg_transactions
		FROM data_transformations
		WHERE _batch_id > @wm_transactions;

		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;


		-- Perform data quality checks on staging table
		WITH dq_checks AS
		(
		SELECT
			COUNT(*) AS total_records,
			COUNT(CASE WHEN transaction_id IS NULL THEN 1 ELSE NULL END) AS pk_null,
			COUNT(CASE WHEN transaction_id NOT LIKE ('TXN%') THEN 1 ELSE NULL END) AS invalid_pk,
			COUNT(CASE WHEN transaction_type NOT IN ('Transfer', 'Wire Transfer') AND counterpart_account_id IS NOT NULL THEN 1 ELSE NULL END) AS unexpected_counterpart,
			COUNT(CASE WHEN currency IS NULL OR TRIM(currency) = '' THEN 1 ELSE NULL END) AS invalid_currency,
			COUNT(CASE WHEN currency NOT IN ('CAD', 'USD', 'GBP', 'EUR') THEN 1 ELSE NULL END) AS new_currency,
			COUNT(CASE WHEN transaction_date > GETDATE() THEN 1 ELSE NULL END) AS invalid_transaction_date,
			COUNT(CASE WHEN CONVERT(NVARCHAR, transaction_date, 120) + ' ' + CONVERT(NVARCHAR, transaction_time, 108) <> CONVERT(NVARCHAR, transaction_date_time, 120)
			THEN 1 ELSE NULL END) AS invalid_transaction_date_time,
			COUNT(CASE WHEN created_at > GETDATE() THEN 1 ELSE NULL END) AS invalid_created_at
		FROM #stg_transactions
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
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'PK Null', CASE WHEN 
		pk_null > 0 THEN 'Critical' ELSE NULL END, total_records, pk_null, CASE WHEN pk_null > 0 THEN 'Failed' ELSE 'Passed' END, 
		CASE WHEN pk_null > 0 THEN
		'Critical DQ Rule Violated: NULL(s) detected in Primary Key, transaction_id. Transactions aborted.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid PK', CASE WHEN invalid_pk > 0 THEN 
		'Critical' ELSE NULL END, total_records, invalid_pk, CASE WHEN invalid_pk > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_pk > 0 THEN
		'Critical DQ Rule Violated: Invalid value(s) detected in primary key, transaction_id. Transactions aborted.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Unexpected Counterpart Account ID', CASE WHEN 
		unexpected_counterpart > 0 THEN 'Warning' ELSE NULL END, total_records, unexpected_counterpart, CASE WHEN unexpected_counterpart > 0 
		THEN 'Failed' ELSE 'Passed' END, CASE WHEN unexpected_counterpart > 0 THEN
		'Mild DQ Rule Violated: Field, counterpart_account_id should be NULL in non-transfer transactions.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Currency', CASE WHEN 
		invalid_currency > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_currency, CASE WHEN invalid_currency > 0 THEN 'Failed' ELSE 'Passed' END, 
		CASE WHEN invalid_currency > 0 THEN
		'Mild DQ Rule Violated: Null(s), empty spaces, or an empty string detected in field, currency.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'New Currency', CASE WHEN 
		new_currency > 0 THEN 'Info' ELSE NULL END, total_records, new_currency, CASE WHEN new_currency > 0 THEN 'Warning' ELSE 'Passed' END, 
		CASE WHEN new_currency > 0 THEN
		'Minor DQ Rule Violated: New value(s) detected in field, currency.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Transaction Date', CASE WHEN 
		invalid_transaction_date > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_transaction_date, 
		CASE WHEN invalid_transaction_date > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_transaction_date > 0 THEN
		'Mild DQ Rule Violated: Field, transaction_date, has date value(s) greater than present date.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Transaction DateTime', CASE WHEN 
		invalid_transaction_date_time > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_transaction_date_time, 
		CASE WHEN invalid_transaction_date_time > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_transaction_date_time > 0 THEN
		'Mild DQ Rule Violated: Field, transaction_date_time not compatible with transaction_date & transaction_time.' ELSE NULL END FROM dq_checks
		UNION
		SELECT @batch_id, @step_id, @start_time, @source_system, @layer, @source_object, @target_object, 'Invalid Creation Date', CASE WHEN 
		invalid_created_at > 0 THEN 'Warning' ELSE NULL END, total_records, invalid_created_at, 
		CASE WHEN invalid_created_at > 0 THEN 'Failed' ELSE 'Passed' END, CASE WHEN invalid_created_at > 0 THEN
		'Mild DQ Rule Violated: Field, created_at, has date value(s) greater than present date.' ELSE NULL END FROM dq_checks;

		-- Throw error when critical DQ rule is violated
		IF EXISTS (SELECT 1 FROM etl.dq_log WHERE ((check_name = 'PK Null' AND records_failed > 0) OR (check_name = 'Invalid PK' AND records_failed > 0)) 
		AND (batch_id = @batch_id AND step_id = @step_id)) THROW 50003, 'Critical DQ Rule(s) Violated: Check etl.dq_log to see more information.', 3; 

		-- Load new records into silver table
		INSERT INTO silver.cbs_transactions
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
			_batch_id,
			_created_at
		)
		SELECT
			src.transaction_id,
			src.account_id,
			src.transaction_type,
			src.amount,
			src.debit_credit,
			src.currency,
			src.transaction_date,
			src.transaction_time,
			src.transaction_date_time,
			src.channel,
			src.[status],
			src.balance_after,
			src.counterpart_account_id,
			src.merchant_name,
			src.merchant_category,
			src.reference_number,
			src.[description],
			src.branch_id,
			src.is_flagged,
			src.created_at,

			-- Map values to metadata columns
			@source_system,
			@batch_id,
			@start_time
		FROM #stg_transactions src
		LEFT JOIN silver.cbs_transactions tgt
		ON src.transaction_id = tgt.transaction_id
		WHERE tgt.transaction_id IS NULL;

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
