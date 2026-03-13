/*
===========================================================================
Script    : 02_create_log_tables.sql
Location  : scripts/00_init/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-13
Version   : 1.0
===========================================================================
Script Purpose:
    Creates the ETL logging framework tables used to track every
    pipeline run, step execution, row-level error, and incremental
    load watermark across all layers.

  Tables Created:
      etl.batch_log    One row per pipeline run
      etl.step_log     One row per step within a run
      etl.error_log    One row per rejected record
      etl.watermark    Last successful load point per source table

  Warning:
      Running this script will drop and recreate all ETL log tables.
      All existing log history will be permanently lost.
=============================================================================
  Change Log:
      1.0   2026-03-13   Initial creation
=============================================================================
*/
USE BankingDW;
GO

DROP TABLE IF EXISTS etl.error_log;
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
	step_name NVARCHAR(50) NOT NULL,
	batch_id INT NOT NULL,
	source_object NVARCHAR(200) NOT NULL,
	target_object NVARCHAR(50) NOT NULL,
	start_time DATETIME2 NOT NULL,
	end_time DATETIME2,
	step_status NVARCHAR(50) NOT NULL,
	rows_extracted INT,
	rows_inserted INT,
	rows_updated INT,
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
	record_key NVARCHAR(200),
	error_code NVARCHAR(50),
	error_description NVARCHAR(MAX) NOT NULL,
	rejected_at DATETIME2 NOT NULL,
	raw_record NVARCHAR(MAX),
	CONSTRAINT fk_batch_id_etl_error_log FOREIGN KEY(batch_id) REFERENCES etl.batch_log (batch_id),
	CONSTRAINT fk_step_id_etl_error_log FOREIGN KEY(step_id) REFERENCES etl.step_log (step_id),
	CONSTRAINT chk_layer_etl_error_log CHECK(layer IN ('Bronze', 'Silver', 'Gold'))
);
GO

-- Create etl.watermark table
CREATE TABLE etl.watermark
(
	watermark_id INT IDENTITY(1, 1) PRIMARY KEY,
	source_system NVARCHAR(50) NOT NULL,
	source_object NVARCHAR(200) NOT NULL,
	target_object NVARCHAR(50) NOT NULL,
	last_batch_id INT NOT NULL,
	last_loaded DATETIME2 NOT NULL,
	updated_at DATETIME2 NOT NULL,
	CONSTRAINT fk_last_batch_id_etl_watermark FOREIGN KEY(last_batch_id) REFERENCES etl.batch_log (batch_id)
);
GO
