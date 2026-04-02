/*
===================================================================================
Script    : 05_etl_reconcile_gold_keys
Location  : scripts/03_gold/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-04-01
Version   : 1.0
===================================================================================
Script Purpose:
    Resolves surrogate foreign keys across Gold dimension tables that could not
    be resolved during initial load due to cross-system circular dependencies.
    This procedure runs after all Gold load procedures have completed, ensuring
    all referenced dimension tables are fully populated before key resolution
    is attempted.

Keys Resolved:
    gold.dim_customers.branch_key
        → Resolved via branch_id → gold.dim_branches.branch_key
        → Applied to current active versions only (_is_active = 1)

    gold.dim_employees.branch_key
        → Resolved via branch_id → gold.dim_branches.branch_key

Usage: EXEC etl.reconcile_gold_keys
	
Parameter: None

===================================================================================
Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-04-01 |  Initial creation                                |
===================================================================================
*/
CREATE OR ALTER PROCEDURE etl.reconcile_gold_keys AS
BEGIN
	-- Suppress number of rows affected	
	SET NOCOUNT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT = NULL,
	@batch_name NVARCHAR(50) = 'etl.reconcile_gold_keys',
	@source_system NVARCHAR(50) = 'Gold',
	@layer NVARCHAR(50) = 'Gold',
	@batch_start_time DATETIME2,
	@batch_end_time DATETIME2,
	@batch_duration_seconds INT,
	@batch_status NVARCHAR(50) = 'Running',
	@rows_resolved INT = 0,
	@total_rows_resolved INT = 0,
	@executed_by NVARCHAR(100) = SUSER_NAME();

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
		@total_rows_resolved,
		@executed_by
	);
	-- Retrieve recently generated batch_id
	SET @batch_id = SCOPE_IDENTITY();


	-- =======================================================================================
	-- SECTION 3: RECONCILE RELEVANT GOLD KEYS
	-- =======================================================================================
	BEGIN TRY
		-- Reconcile gold.dim_customers
		UPDATE tgt
			SET 
				tgt.branch_key = src.branch_key
				FROM gold.dim_customers tgt
				INNER JOIN gold.dim_branches src
				ON tgt.branch_id = src.branch_id
			WHERE COALESCE(tgt.branch_key, 0) <> COALESCE(src.branch_key, 0)
			AND tgt._is_active = 1;

		-- Retrieve number of rows resolved
		SET @rows_resolved = @@ROWCOUNT;
		SET @total_rows_resolved = @total_rows_resolved + @rows_resolved;

		-- Reconcile gold.dim_employees
		UPDATE tgt
			SET 
				tgt.branch_key = src.branch_key
				FROM gold.dim_employees tgt
				INNER JOIN gold.dim_branches src
				ON tgt.branch_id = src.branch_id
			WHERE COALESCE(tgt.branch_key, 0) <> COALESCE(src.branch_key, 0);

		-- Retrieve number of rows resolved
		SET @rows_resolved = @@ROWCOUNT;
		SET @total_rows_resolved = @total_rows_resolved + @rows_resolved;

		-- Map values to variables on success
		SET @batch_end_time = SYSDATETIME();
		SET @batch_duration_seconds = DATEDIFF(second, @batch_start_time, @batch_end_time);
		SET @batch_status = 'Success';

		-- Update log details at batch-level on success
		UPDATE etl.batch_log
			SET
				end_time = @batch_end_time,
				load_duration_seconds = @batch_duration_seconds,
				load_status = @batch_status,
				total_rows_processed = @total_rows_resolved
			WHERE batch_id = @batch_id;
	END TRY


	-- =======================================================================================
	-- SECTION 4: ERROR HANDLING
	-- =======================================================================================
	BEGIN CATCH
		-- Map values to variables on failure
		SET @batch_end_time = SYSDATETIME();
		SET @batch_duration_seconds = DATEDIFF(second, @batch_start_time, @batch_end_time);
		SET @batch_status = 'Failed';

		IF @total_rows_resolved IS NULL SET @total_rows_resolved = 0;

		-- Update log details at batch-level on failure
		UPDATE etl.batch_log
			SET
				end_time = @batch_end_time,
				load_duration_seconds = @batch_duration_seconds,
				load_status = @batch_status,
				total_rows_processed = @total_rows_resolved,
				err_message = ERROR_MESSAGE()
			WHERE batch_id = @batch_id;

		THROW;
	END CATCH;
END;
