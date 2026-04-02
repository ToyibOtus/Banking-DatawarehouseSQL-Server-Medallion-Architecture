/*
====================================================================================
Script    : 02_create_log_tables.sql
Location  : scripts/00_init/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-13
Version   : 1.6
====================================================================================
Script Purpose:
    Creates the ETL logging framework tables used to track every
    pipeline run, step execution, row-level error, and incremental
    load watermark across all layers.

  Tables Created:
      etl.batch_log    One row per pipeline run
      etl.step_log     One row per step within a run
      etl.error_log    One row per rejected record
	  etl.dq_log       One row per data quality check
      etl.watermark    Last successful load point per source table

  Warning:
      Running this script will drop and recreate all ETL log tables.
      All existing log history will be permanently lost.
====================================================================================
  Change Log:
	 
	 | Version |     Date    |    Description                                   |
	 |---------|-------------|--------------------------------------------------|
	 | 	 1.0   |  2026-03-13 |  Initial creation                                |
	 |	 1.1   |  2026-03-17 |  Added load duration to batch and step logs,     |
	 |	       |             |  loaded etl.watermark                            |
	 |   1.2   |  2026-03-18 |  Seeded watermark log table with bronze records  | 
	 |   1.3   |  2026-03-18 |  Seeded watermark log table with silver records  | 
	 |   1.4   |  2026-03-21 |  Removed error_code, raw_record & record_key     |
	 |         |             |  from error_log                                  |
	 |   1.5   |  2026-03-21 |  Added a new log table, dq_log                   |
	 |   1.6   |  2026-03-29 |  Added new records to dq_log table               |
	 |   1.6   |  2026-03-30 |  Added a new field, rows expired to step table   |
====================================================================================
*/
USE BankingDW;
GO

DROP TABLE IF EXISTS etl.error_log;
DROP TABLE IF EXISTS etl.dq_log;
DROP TABLE IF EXISTS etl.step_log;
DROP TABLE IF EXISTS etl.watermark;
DROP TABLE IF EXISTS etl.batch_log;
GO

-- Create etl.batch_log table
CREATE TABLE etl.batch_log
(
	batch_id INT IDENTITY(1, 1) PRIMARY KEY,
	batch_name NVARCHAR(50) NOT NULL,
	source_system NVARCHAR(50) NOT NULL,
	layer NVARCHAR(50) NOT NULL,
	start_time DATETIME2 NOT NULL,
	end_time DATETIME2,
	load_duration_seconds INT,
	load_status NVARCHAR(50) NOT NULL,
	total_rows_processed INT,
	executed_by NVARCHAR(100),
	err_message NVARCHAR(MAX),
	CONSTRAINT chk_layer_etl_batch_log CHECK(layer IN ('Bronze', 'Silver', 'Gold')),
	CONSTRAINT chk_load_status CHECK(load_status IN('Running', 'Success', 'Failed'))
);
GO

-- Create etl.step_log table
CREATE TABLE etl.step_log
(
	step_id INT IDENTITY(1, 1) PRIMARY KEY,
	batch_id INT NOT NULL,
	step_name NVARCHAR(50) NOT NULL,
	load_type NVARCHAR(50) NOT NULL,
	source_object NVARCHAR(200) NOT NULL,
	target_object NVARCHAR(50) NOT NULL,
	start_time DATETIME2 NOT NULL,
	end_time DATETIME2,
	load_duration_seconds INT,
	step_status NVARCHAR(50) NOT NULL,
	rows_extracted INT,
	rows_inserted INT,
	rows_updated INT,
	rows_expired INT,
	rows_rejected INT,
	err_message NVARCHAR(MAX),
	CONSTRAINT fk_batch_id_etl_step_log FOREIGN KEY(batch_id) REFERENCES etl.batch_log (batch_id),
	CONSTRAINT chk_step_status CHECK(step_status IN('Running', 'Success', 'Failed'))
);
GO

-- Create etl.error_log table
CREATE TABLE etl.error_log
(
	error_id INT IDENTITY(1, 1) PRIMARY KEY,
	batch_id INT NOT NULL,
	step_id INT NOT NULL,
	source_system NVARCHAR(50) NOT NULL,
	layer NVARCHAR(50) NOT NULL,
	source_object NVARCHAR(200) NOT NULL,
	target_object NVARCHAR(50) NOT NULL,
	error_description NVARCHAR(MAX) NOT NULL,
	rejected_at DATETIME2 NOT NULL,
	CONSTRAINT fk_batch_id_etl_error_log FOREIGN KEY(batch_id) REFERENCES etl.batch_log (batch_id),
	CONSTRAINT fk_step_id_etl_error_log FOREIGN KEY(step_id) REFERENCES etl.step_log (step_id),
	CONSTRAINT chk_layer_etl_error_log CHECK(layer IN('Bronze', 'Silver', 'Gold'))
);
GO

CREATE TABLE etl.dq_log
(
	dq_id INT IDENTITY(1, 1) PRIMARY KEY,
	batch_id INT NOT NULL,
	step_id INT NOT NULL,
	checked_at DATETIME2 NOT NULL,
	source_system NVARCHAR(50) NOT NULL,
	layer NVARCHAR(50) NOT NULL,
	source_object NVARCHAR(50) NOT NULL,
	target_object NVARCHAR(50) NOT NULL,
	check_name NVARCHAR(50) NOT NULL,
	severity NVARCHAR(50),
	records_checked INT NOT NULL,
	records_failed INT NOT NULL,
	dq_status NVARCHAR(50) NOT NULL,
	dq_description NVARCHAR(MAX),
	CONSTRAINT fk_batch_id_etl_dq_log FOREIGN KEY(batch_id) REFERENCES etl.batch_log (batch_id),
	CONSTRAINT fk_step_id_etl_dq_log FOREIGN KEY(step_id) REFERENCES etl.step_log (step_id),
	CONSTRAINT chk_severity_etl_dq_log CHECK(severity IN('Critical', 'Warning', 'Info')),
	CONSTRAINT chk_dq_status_etl_dq_log CHECK(dq_status IN('Failed', 'Warning', 'Passed'))
);

-- Create etl.watermark table
CREATE TABLE etl.watermark
(
	watermark_id INT IDENTITY(1, 1) PRIMARY KEY,
	source_system NVARCHAR(50) NOT NULL,
	source_object NVARCHAR(200) NULL,
	target_object NVARCHAR(50) NOT NULL,
	last_batch_id INT NULL,
	last_loaded DATETIME2 NOT NULL,
	CONSTRAINT fk_last_batch_id_etl_watermark FOREIGN KEY(last_batch_id) REFERENCES etl.batch_log (batch_id)
);
GO

-- Load etl.watermark table
INSERT INTO etl.watermark
(
	source_system,
	target_object,
	last_loaded
)
VALUES 
	('CRM', 'bronze.crm_customers', '1900-01-01'),
	('HRMS', 'bronze.hrms_employees', '1900-01-01'),
	('CBS', 'bronze.cbs_accounts', '1900-01-01'),
	('CBS', 'bronze.cbs_branches', '1900-01-01'),
	('CBS', 'bronze.cbs_transactions', '1900-01-01'),
	('LOS', 'bronze.los_loan_applications', '1900-01-01'),
	('CRM', 'silver.crm_customers', '1900-01-01'),
	('HRMS', 'silver.hrms_employees', '1900-01-01'),
	('CBS', 'silver.cbs_accounts', '1900-01-01'),
	('CBS', 'silver.cbs_branches', '1900-01-01'),
	('CBS', 'silver.cbs_transactions', '1900-01-01'),
	('LOS', 'silver.los_loan_applications', '1900-01-01'),
	('CRM', 'gold.dim_customers', '1900-01-01'),
	('HRMS', 'gold.dim_employees', '1900-01-01'),
	('CBS', 'gold.dim_branches', '1900-01-01'),
	('CBS', 'gold.dim_accounts', '1900-01-01'),
	('CBS', 'gold.fact_transactions', '1900-01-01'),
	('LOS', 'gold.fact_loan_applications', '1900-01-01');
