/*
===================================================================================
Script    : 02_gold_views
Location  : scripts/03_gold/ddl/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-04-04
Version   : 1.0
===================================================================================
Script Purpose:
	Creates analyst-facing views over all Gold layer tables. Each view exposes
	only business-relevant columns — all ETL metadata columns are excluded. Views 
	over SCD2 tables (dim_customers, dim_accounts) apply a WHERE _is_active = 1 
	filter, ensuring analysts always query current records without needing 
	knowledge of the underlying SCD2 implementation.

Views Created:
    gold.vw_customers          
    gold.vw_accounts             
    gold.vw_branches             
    gold.vw_employees            
    gold.vw_transactions        
    gold.vw_loan_applications  

NOTE: 
	This script must be executed after all Gold layer stored procedures
	have been created and successfully run at least once. The views reference
	Gold tables that must exist and be populated before use
===================================================================================
  Change Log:
 
  | Version |     Date    |  Description                                        |
  |---------|-------------|-----------------------------------------------------|
  |   1.0   |  2026-04-04 |  Initial creation                                   |
===================================================================================
*/
USE BankingDW;
GO

-- Drop view [gold.vw_accounts] if exists
IF OBJECT_ID ('gold.vw_accounts', 'V') IS NOT NULL
DROP VIEW gold.vw_accounts;
GO

-- Create view [gold.vw_accounts]
CREATE VIEW gold.vw_accounts AS
SELECT
	account_key,
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
	updated_at
FROM gold.dim_accounts
WHERE _is_active = 1;
GO

-- Drop view [gold.vw_branches] if exists
IF OBJECT_ID ('gold.vw_branches', 'V') IS NOT NULL
DROP VIEW gold.vw_branches;
GO

-- Create view [gold.vw_branches]
CREATE VIEW gold.vw_branches AS
SELECT
	branch_key,
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
	manager_id
FROM gold.dim_branches;
GO

-- Drop view [gold.vw_customers] if exists
IF OBJECT_ID ('gold.vw_customers', 'V') IS NOT NULL
DROP VIEW gold.vw_customers;
GO

-- Create view [gold.vw_customers]
CREATE VIEW gold.vw_customers AS
SELECT
	customer_key,
	customer_id,
	customer_name,
	company_name,
	segment,
	risk_band,
	date_of_birth,
	gender,
	national_id,
	email,
	phone_number,
	address_line_1,
	city,
	[state],
	zip_code,
	country,
	onboard_date,
	branch_key,
	branch_id,
	customer_since,
	is_active,
	marketing_opt_in,
	preferred_language,
	annual_income,
	credit_score,
	created_at,
	updated_at
FROM gold.dim_customers
WHERE _is_active = 1;
GO

-- Drop view [gold.vw_employees] if exists
IF OBJECT_ID ('gold.vw_employees', 'V') IS NOT NULL
DROP VIEW gold.vw_employees;
GO

-- Create view [gold.vw_employees]
CREATE VIEW gold.vw_employees AS
SELECT
	employee_key,
	employee_id,
	employee_name,
	email,
	phone_number,
	department,
	job_title,
	branch_key,
	branch_id,
	hire_date,
	termination_date,
	salary,
	is_active,
	manager_key,
	manager_id,
	manager_name
FROM gold.dim_employees;
GO

-- Drop view [gold.vw_loan_applications] if exists
IF OBJECT_ID ('gold.vw_loan_applications', 'V') IS NOT NULL
DROP VIEW gold.vw_loan_applications;
GO

-- Create view [gold.vw_loan_applications]
CREATE VIEW gold.vw_loan_applications AS
SELECT
	loan_key,
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
	updated_at
FROM gold.fact_loan_applications;
GO

-- Drop view [gold.vw_transactions] if exists
IF OBJECT_ID ('gold.vw_transactions', 'V') IS NOT NULL
DROP VIEW gold.vw_transactions;
GO

-- Create view [gold.vw_transactions]
CREATE VIEW gold.vw_transactions AS
SELECT 
	transaction_key,
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
	created_at
FROM gold.fact_transactions;
GO
