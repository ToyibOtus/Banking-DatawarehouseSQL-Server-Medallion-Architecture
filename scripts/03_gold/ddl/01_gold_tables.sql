/*
===================================================================================
Script    : 01_gold_tables
Location  : scripts/03_gold/ddl/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-19
Version   : 1.0
===================================================================================
Script Purpose:
    Creates the gold tables into which integrated records from the silver layer 
	are loaded.
 
  Tables Created:
      gold.dim_customers
	  gold.dim_employees
	  gold.dim_branches
	  gold.dim_accounts
	  gold.fact_transactions
	  gold.fact_loan_applications

 
  Warning:
      Running this script will drop and recreate all gold tables.
      All existing data will be permanently lost.
===================================================================================
  Change Log:
 
  | Version |     Date    |  Description                                        |
  |---------|-------------|-----------------------------------------------------|
  |   1.0   |  2026-03-19 |  Initial creation                                   |
===================================================================================
*/
USE BankingDW;
GO

-- Drop table [gold.dim_customers] if it exists
DROP TABLE IF EXISTS gold.dim_customers;

-- Create table [gold.dim_customers]
CREATE TABLE gold.dim_customers
(
	customer_key INT IDENTITY(1, 1) PRIMARY KEY,
	customer_id NVARCHAR(50) NOT NULL,
	customer_name NVARCHAR(50),
	company_name NVARCHAR(50),
	segment NVARCHAR(50),
	risk_band NVARCHAR(50),
	date_of_birth DATE,
	gender NVARCHAR(50),
	national_id NVARCHAR(50),
	email NVARCHAR(250),
	phone_number NVARCHAR(50),
	address_line_1 NVARCHAR(200),
	city NVARCHAR(50),
	[state] NVARCHAR(50),
	zip_code NVARCHAR(50),
	country NVARCHAR(50),
	onboard_date DATE,
	branch_key INT,
	branch_id NVARCHAR(50),
	customer_since INT,
	is_active BIT,
	marketing_opt_in BIT,
	preferred_language NVARCHAR(50),
	annual_income DECIMAL(18, 2),
	credit_score INT,
	created_at DATE,
	updated_at DATE,

	_source_system NVARCHAR(50) NOT NULL,
	_batch_id INT NOT NULL,
	_created_at DATETIME2 NOT NULL,
	_updated_at DATETIME2,
	_valid_from DATE NOT NULL,
	_valid_to DATE,
	_is_active BIT NOT NULL
);

-- Drop table [gold.dim_employees] if it exists
DROP TABLE IF EXISTS gold.dim_employees;

-- Create table [gold.dim_employees]
CREATE TABLE gold.dim_employees
(
	employee_key INT IDENTITY(1, 1) PRIMARY KEY,
	employee_id NVARCHAR(50) NOT NULL,
	employee_name NVARCHAR(50),
	email NVARCHAR(250),
	phone_number NVARCHAR(50),
	department NVARCHAR(50),
	job_title NVARCHAR(50),
	branch_key INT,
	branch_id NVARCHAR(50),
	hire_date DATE,
	termination_date DATE,
	salary DECIMAL(18, 2),
	is_active BIT,
	manager_key INT,
	manager_id NVARCHAR(50),
	manager_name NVARCHAR(50),

	_source_system NVARCHAR(50) NOT NULL,
	_batch_id INT NOT NULL,
	_created_at DATETIME2 NOT NULL,
	_updated_at DATETIME2
);

-- Drop table [gold.dim_branches] if it exists
DROP TABLE IF EXISTS gold.dim_branches;

-- Create table [gold.dim_branches]
CREATE TABLE gold.dim_branches
(
	branch_key INT IDENTITY(1, 1) PRIMARY KEY,
	branch_id NVARCHAR(50),
	branch_name NVARCHAR(200),
	branch_type NVARCHAR(50),
	address_line_1 NVARCHAR(200),
	city NVARCHAR(50),
	[state] NVARCHAR(50),
	zip_code NVARCHAR(50),
	country NVARCHAR(50),
	phone_number NVARCHAR(50),
	email NVARCHAR(250),
	opened_date DATE,
	is_active BIT,
	region NVARCHAR(50),
	manager_key INT,
	manager_id NVARCHAR(50),

	_source_system NVARCHAR(50) NOT NULL,
	_batch_id INT NOT NULL,
	_created_at DATETIME2 NOT NULL,
	_updated_at DATETIME2
);

-- Drop table [gold.dim_accounts] if it exists
DROP TABLE IF EXISTS gold.dim_accounts;

-- Create table [gold.dim_accounts]
CREATE TABLE gold.dim_accounts
(
	account_key INT IDENTITY(1, 1) PRIMARY KEY,
	account_id NVARCHAR(50),
	customer_key INT,
	customer_id NVARCHAR(50),
	account_type NVARCHAR(50),
	account_status NVARCHAR(50),
	open_date DATE,
	close_date DATE,
	currency_code NVARCHAR(50),
	overdraft_limit DECIMAL(18, 2),
	interest_rate DECIMAL(12, 4),
	branch_key INT,
	branch_id NVARCHAR(50),
	employee_key INT,
	employee_id NVARCHAR(50),
	is_primary BIT,
	created_at DATE,
	updated_at DATE,

	_source_system NVARCHAR(50) NOT NULL,
	_batch_id INT NOT NULL,
	_created_at DATETIME2 NOT NULL,
	_updated_at DATETIME2,
	_valid_from DATE NOT NULL,
	_valid_to DATE,
	_is_active BIT NOT NULL
);

-- Drop table [gold.fact_transactions] if it exists
DROP TABLE IF EXISTS gold.fact_transactions;

-- Create table [gold.fact_transactions]
CREATE TABLE gold.fact_transactions
(
	transaction_key INT IDENTITY(1, 1) PRIMARY KEY,
	transaction_id NVARCHAR(50),
	account_key INT,
	transaction_type NVARCHAR(50),
	amount DECIMAL(18, 2),
	debit_credit NVARCHAR(50),
	currency NVARCHAR(50),
	transaction_date DATE,
	transaction_time TIME(0),
	transaction_date_time DATETIME2(0),
	channel NVARCHAR(50),
	[status] NVARCHAR(50),
	balance_after DECIMAL(18, 2),
	counterpart_account_id NVARCHAR(50),
	merchant_name NVARCHAR(50),
	merchant_category NVARCHAR(50),
	reference_number NVARCHAR(50),
	[description] NVARCHAR(500),
	branch_key INT,
	is_flagged BIT,
	created_at DATETIME2(0),

	_source_system NVARCHAR(50) NOT NULL,
	_batch_id INT NOT NULL,
	_created_at DATETIME2 NOT NULL
);

-- Drop table [gold.fact_loan_applications] if it exists
DROP TABLE IF EXISTS gold.fact_loan_applications;

-- Create table [gold.fact_loan_applications]
CREATE TABLE gold.fact_loan_applications
(
	loan_key INT IDENTITY(1, 1) PRIMARY KEY,
	loan_id NVARCHAR(50) NOT NULL,
	customer_key INT,
	branch_key INT,
	employee_key INT,
	loan_type NVARCHAR(50),
	loan_status NVARCHAR(50),
	application_date DATE,
	decision_date DATE,
	disbursement_date DATE,
	requested_amount DECIMAL(18, 2),
	approved_amount DECIMAL(18, 2),
	disbursed_amount DECIMAL(18, 2),
	amount_paid DECIMAL(18, 2),
	outstanding_balance DECIMAL(18, 2),
	interest_rate DECIMAL(18, 4),
	term_months INT,
	monthly_payment DECIMAL(18, 2),
	days_delinquent INT,
	collateral_type NVARCHAR(50),
	collateral_value DECIMAL(18, 2),
	purpose_description NVARCHAR(500),
	rejection_reason NVARCHAR(500),
	created_at DATE,
	updated_at DATE,

	_source_system NVARCHAR(50) NOT NULL,
	_batch_id INT NOT NULL,
	_created_at DATETIME2 NOT NULL,
	_updated_at DATETIME2
);
