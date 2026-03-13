/*
===========================================================================
Script    : 01_create_database.sql
Location  : scripts/00_init/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-12
===========================================================================
Script Purpose:
	Creates the BankingDW database and all required schemas across
    the Medallion Architecture layers (Bronze, Silver, Gold) and
    the ETL control layer

Warning:
	Running this script permanently deletes the database [BankingDW],
	and all data inside it.
	Ensure to have proper backup before running.
===========================================================================
  Change Log:
      1.0   2026-03-13   Initial creation
===========================================================================
*/
USE master;
GO

-- Drop database if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'BankingDW')
BEGIN
	ALTER DATABASE BankingDW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE BankingDW;
END;
GO

CREATE DATABASE BankingDW;
GO

USE BankingDW;
GO

-- Create bronze layer
CREATE SCHEMA bronze;
GO

-- Create silver layer
CREATE SCHEMA silver;
GO

-- Create gold layer
CREATE SCHEMA gold;
GO

-- Create etl schema
CREATE SCHEMA etl;
GO
