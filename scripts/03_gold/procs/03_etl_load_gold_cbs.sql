/*
===================================================================================
Script    : 03_etl_load_gold_cbs
Location  : scripts/03_gold/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-30
Version   : 1.0
===================================================================================
Script Purpose:
    Loads all CBS records from the silver layer into corresponding gold layer.
	It performs data integrations where necessary. Additionally, it has an in-buit 
	logging system designed to track and monitor every ETL step, and thus enabling 
	easy debugging.

Tables Loaded:
	gold.dim_branches
	gold.dim_accounts
	gold.fact_transactions

Usage: EXEC etl.load_gold_cbs
	
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
CREATE OR ALTER PROCEDURE etl.load_gold_cbs AS
BEGIN
	-- Suppress number of rows affected	
	SET NOCOUNT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.load_gold_cbs',
	@source_system NVARCHAR(50) = 'CBS',
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
	@rows_expired INT,

	-- Last Batch ID (Retrieved from etl.watermark)
	@wm_branches INT,
	@wm_accounts INT,
	@wm_transactions INT,

	-- Source Names
	@source_branches NVARCHAR(50) = 'silver.cbs_branches',
	@source_accounts NVARCHAR(50) = 'silver.cbs_accounts',
	@source_transactions NVARCHAR(50) = 'silver.cbs_transactions',

	-- target tables
	@target_branches NVARCHAR(50) = 'gold.dim_branches',
	@target_accounts NVARCHAR(50) = 'gold.dim_accounts',
	@target_transactions NVARCHAR(50) = 'gold.fact_transactions';


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
	SELECT @wm_branches = COALESCE(last_batch_id, 0) FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_branches;

	SELECT @wm_accounts = COALESCE(last_batch_id, 0) FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_accounts;

	SELECT @wm_transactions = COALESCE(last_batch_id, 0) FROM etl.watermark WHERE source_system = @source_system AND target_object = @target_transactions;


	-- =======================================================================================
	-- SECTION 4: LOAD ALL GOLD TABLES
	-- =======================================================================================
	BEGIN TRY
	-- ===============================================
	-- STEP 1: LOAD gold.dim_branches
	-- ===============================================

		-- Map values to variables before transactions		
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load gold.dim_branches';
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


		-- Load silver table into a temporary staging table
		SELECT
			cb.branch_id,
			cb.branch_name,
			cb.branch_type,
			cb.address_line_1,
			cb.city,
			cb.[state],
			cb.zip_code,
			cb.country,
			cb.phone_number,
			cb.email,
			cb.opened_date,
			cb.is_active,
			cb.region,
			de.employee_key AS manager_key,
			cb.manager_employee_id AS manager_id
			INTO #stg_branches
		FROM silver.cbs_branches cb
		LEFT JOIN gold.dim_employees de
		ON cb.manager_employee_id = de.employee_id
		WHERE cb._batch_id > @wm_branches;

		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;

		-- Overwrite outdated records
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
				tgt.email = src. email,
				tgt.is_active = src.is_active,
				tgt.region = src.region,
				tgt.manager_key = src.manager_key,
				tgt.manager_id = src.manager_id,

				tgt._source_system = @source_system,
				tgt._batch_id = @batch_id,
				tgt._updated_at = @start_time
				FROM gold.dim_branches tgt
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
					COALESCE(tgt.email, '') <> COALESCE(src. email, '') OR
					COALESCE(tgt.is_active, 0) <> COALESCE(src.is_active, 0) OR
					COALESCE(tgt.region, '') <> COALESCE(src.region, '') OR
					COALESCE(tgt.manager_key, 0) <> COALESCE(src.manager_key, 0) OR
					COALESCE(tgt.manager_id, '') <> COALESCE(src.manager_id, '');

		
		-- Retrieve number of updated records
		SET @rows_updated = @@ROWCOUNT;


		-- Load new records into gold table
		INSERT INTO gold.dim_branches
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
			manager_key,
			manager_id,

			-- metadata columns
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
			src.manager_key,
			src.manager_id,

			-- Map variables to metadata columns
			@source_system,
			@batch_id,
			@start_time
		FROM #stg_branches src
		LEFT JOIN gold.dim_branches tgt
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
	-- STEP 2: LOAD gold.dim_accounts
	-- ===============================================

		-- Map values to variables before transactions		
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load gold.dim_accounts';
		SET @load_type = 'SCD2: Expire & Insert';
		SET @source_object = @source_accounts;
		SET @target_object = @target_accounts;
		SET @step_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
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
			@rows_expired,
			@rows_rejected
		);
		-- Retrieve recently generated step_id
		SET @step_id = SCOPE_IDENTITY();


		-- Load silver table into a temporary staging table
		SELECT
			ca.account_id,
			dc.customer_key,
			ca.customer_id,
			ca.account_type,
			ca.account_status,
			ca.open_date,
			ca.close_date,
			ca.currency_code,
			ca.overdraft_limit,
			ca.interest_rate,
			db.branch_key,
			ca.branch_id,
			de.employee_key,
			ca.assigned_employee_id AS employee_id,
			ca.is_primary,
			ca.created_at,
			ca.updated_at
			INTO #stg_accounts
		FROM silver.cbs_accounts ca
		LEFT JOIN 
		(SELECT customer_id, customer_key FROM gold.dim_customers WHERE _is_active = 1) dc
		ON ca.customer_id = dc.customer_id
		LEFT JOIN gold.dim_branches db
		ON ca.branch_id = db.branch_id
		LEFT JOIN gold.dim_employees de
		ON ca.assigned_employee_id = de.employee_id
		WHERE ca._batch_id > @wm_accounts;

		-- Retrieve number of loaded records
		SET @rows_extracted = @@ROWCOUNT;

		-- Overwrite metadata of outdated records
		UPDATE tgt
			SET
				tgt._source_system = @source_system,
				tgt._batch_id = @batch_id,
				tgt._updated_at = @start_time,
				tgt._valid_to = CAST(@start_time AS DATE),
				tgt._is_active = 0
				FROM gold.dim_accounts tgt
				INNER JOIN #stg_accounts src
				ON tgt.account_id = src.account_id
			WHERE
				(COALESCE(tgt.account_status, '') <> COALESCE(src.account_status, '') OR
				COALESCE(tgt.close_date, '1900-01-01') <> COALESCE(src.close_date, '1900-01-01') OR
				COALESCE(tgt.overdraft_limit, 0) <> COALESCE(src.overdraft_limit, 0) OR
				COALESCE(tgt.interest_rate, 0) <> COALESCE(src.interest_rate, 0) OR
				COALESCE(tgt.is_primary, 0) <> COALESCE(src.is_primary, 0))
				AND tgt._is_active = 1;

		SET @rows_expired = @@ROWCOUNT;


		-- Load updated records into gold table
		INSERT INTO gold.dim_accounts
		(
			account_id,
			customer_key,
			customer_id,
			account_type,
			account_status,
			open_date,
			close_date,
			currency_code,
			overdraft_limit,
			interest_rate,
			branch_key,
			branch_id,
			employee_key,
			employee_id,
			is_primary,
			created_at,
			updated_at,

			-- metadata columns
			_source_system,
			_batch_id,
			_created_at,
			_valid_from,
			_is_active
		)
		SELECT
			src.account_id,
			src.customer_key,
			src.customer_id,
			src.account_type,
			src.account_status,
			src.open_date,
			src.close_date,
			src.currency_code,
			src.overdraft_limit,
			src.interest_rate,
			src.branch_key,
			src.branch_id,
			src.employee_key,
			src.employee_id,
			src.is_primary,
			src.created_at,
			src.updated_at,

			-- map variables to metadata columns
			@source_system,
			@batch_id,
			@start_time,
			CAST(@start_time AS DATE),
			1
		FROM #stg_accounts src
		WHERE EXISTS (SELECT 1 FROM gold.dim_accounts tgt
		WHERE src.account_id = tgt.account_id AND _batch_id = @batch_id AND _is_active = 0);

		-- Retrieve number of updated records
		SET @rows_inserted = @@ROWCOUNT;

		-- Load new records into gold table
		INSERT INTO gold.dim_accounts
		(
			account_id,
			customer_key,
			customer_id,
			account_type,
			account_status,
			open_date,
			close_date,
			currency_code,
			overdraft_limit,
			interest_rate,
			branch_key,
			branch_id,
			employee_key,
			employee_id,
			is_primary,
			created_at,
			updated_at,

			-- metadata columns
			_source_system,
			_batch_id,
			_created_at,
			_valid_from,
			_is_active
		)
		SELECT
			src.account_id,
			src.customer_key,
			src.customer_id,
			src.account_type,
			src.account_status,
			src.open_date,
			src.close_date,
			src.currency_code,
			src.overdraft_limit,
			src.interest_rate,
			src.branch_key,
			src.branch_id,
			src.employee_key,
			src.employee_id,
			src.is_primary,
			src.created_at,
			src.updated_at,

			-- map variables to metadata columns
			@source_system,
			@batch_id,
			@start_time,
			CAST(@start_time AS DATE),
			1
		FROM #stg_accounts src
		LEFT JOIN gold.dim_accounts tgt
		ON src.account_id = tgt.account_id
		WHERE tgt.account_id IS NULL;

		-- Map values to variables on success
		SET @rows_inserted = @rows_inserted + @@ROWCOUNT;
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
				rows_expired = @rows_expired,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #stg_accounts;


	-- ===============================================
	-- STEP 3: LOAD gold.fact_transactions
	-- ===============================================

		-- Map values to variables before transactions		
		SET @start_time = SYSDATETIME();
		SET @step_id = NULL;
		SET @step_name = 'Load gold.fact_transactions';
		SET @load_type = 'Incremental: Insert';
		SET @source_object = @source_transactions;
		SET @target_object = @target_transactions;
		SET @step_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
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
			@rows_rejected
		);
		-- Retrieve recently generated step_id
		SET @step_id = SCOPE_IDENTITY();

		
		-- Load silver table into a temporary staging table
		SELECT
			ct.transaction_id,
			da.account_key,
			ct.transaction_type,
			ct.amount,
			ct.debit_credit,
			ct.currency,
			ct.transaction_date,
			ct.transaction_time,
			ct.transaction_date_time,
			ct.channel,
			ct.[status],
			ct.balance_after,
			ct.counterpart_account_id,
			ct.merchant_name,
			ct.merchant_category,
			ct.reference_number,
			ct.[description],
			db.branch_key,
			ct.is_flagged,
			ct.created_at
			INTO #stg_transactions
		FROM silver.cbs_transactions ct
		LEFT JOIN (SELECT account_id, account_key FROM gold.dim_accounts WHERE _is_active = 1) da
		ON ct.account_id = da.account_id
		LEFT JOIN gold.dim_branches db
		ON ct.branch_id = db.branch_id
		WHERE ct._batch_id > @wm_transactions;


		-- Retrieve rows loaded
		SET @rows_extracted = @@ROWCOUNT;


		-- Load new records into gold table
		INSERT INTO gold.fact_transactions
		(
			transaction_id,
			account_key,
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
			branch_key,
			is_flagged,
			created_at,

			-- Metadata Columns
			_source_system,
			_batch_id,
			_created_at
		)
		SELECT
			src.transaction_id,
			src.account_key,
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
			src.branch_key,
			src.is_flagged,
			src.created_at,

			-- Map variables to metadata columns
			@source_system,
			@batch_id,
			@start_time
		FROM #stg_transactions src
		LEFT JOIN gold.fact_transactions tgt
		ON src.transaction_id = tgt.transaction_id
		WHERE tgt.transaction_id IS NULL;


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
