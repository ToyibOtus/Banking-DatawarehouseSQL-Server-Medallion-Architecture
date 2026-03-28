/*
===================================================================================
Script    : 01_silver_quality_checks.sql
Location  : tests/00_data_quality_checks/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-28
Version   : 1.0
===================================================================================
Script Purpose:
    Performs comprehensive data quality validation checks across all Silver
    layer tables to verify the integrity, completeness, consistency, and
    accuracy of transformed data loaded from the Bronze layer.

Tables Checked:
    silver.cbs_accounts
    silver.cbs_branches
    silver.cbs_transactions
    silver.crm_customers
    silver.hrms_employees
    silver.los_loan_applications

Check Categories:
    - Primary key integrity    (NULLs, duplicates, invalid formats)
    - Foreign key integrity    (referential validity, invalid formats)
    - String field quality     (unwanted spaces, empty strings, NULLs)
    - Data standardisation     (low cardinality field consistency)
    - Date validation          (chronological order, future dates)
    - Metric validation        (business rule compliance)
    - Loan lifecycle rules     (status-amount-delinquency consistency)

Usage:
    Run after executing all Silver layer stored procedures.
    Each query is labelled with its expectation.
    Any result returned indicates a data quality issue.

Note:
    Queries marked "Expectation: No Result" should return zero rows
    on a clean Silver load. Any rows returned warrant investigation.
    Queries marked "Expectation: User-Friendly Values" are for manual
    inspection of distinct values — results are expected.
===================================================================================
Change Log:

	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-03-28 |  Initial creation                                |
===================================================================================
*/
--===========================================================================
-- Data Quality Checks on silver.crm_customers
--===========================================================================
-- Check for NULLs & duplicates in primary key
-- Expectation: No Result
SELECT
	customer_id,
	COUNT(*) AS duplicate_chk
FROM silver.crm_customers
GROUP BY customer_id
HAVING customer_id IS NULL OR COUNT(*) > 1;


-- Check for invalid values in primary key
-- Expectation: No Result
SELECT
	customer_id
FROM silver.crm_customers
WHERE TRIM(customer_id) = '' OR customer_id NOT LIKE ('CUST%');


-- Check for unwanted spaces in primary key
-- Expectation: No Result
SELECT
	customer_id
FROM silver.crm_customers
WHERE customer_id <> TRIM(customer_id);


-- Check for invalid foreign keys
-- Expectation: No Result
SELECT onboarding_branch_id FROM silver.crm_customers WHERE onboarding_branch_id NOT IN
(SELECT branch_id FROM silver.cbs_branches);


-- Check for invalid values in foreign key
-- Expectation: No Result
SELECT
	onboarding_branch_id
FROM silver.crm_customers
WHERE TRIM(onboarding_branch_id) = '' OR onboarding_branch_id NOT LIKE ('BRN%');


-- Check for unwanted spaces in foreign key
-- Expectation: No Result
SELECT
	onboarding_branch_id
FROM silver.crm_customers
WHERE onboarding_branch_id <> TRIM(onboarding_branch_id);



-- Data Standardization & Consistency on low cardinality fields
-- Expectation: User friendly values & No NULLs 
SELECT DISTINCT segment FROM silver.crm_customers;

SELECT DISTINCT risk_band FROM silver.crm_customers;

SELECT DISTINCT gender FROM silver.crm_customers;

SELECT DISTINCT city FROM silver.crm_customers;

SELECT DISTINCT [state] FROM silver.crm_customers;

SELECT DISTINCT country FROM silver.crm_customers;

SELECT DISTINCT customer_since FROM silver.crm_customers;

SELECT DISTINCT is_active FROM silver.crm_customers;

SELECT DISTINCT marketing_opt_in FROM silver.crm_customers;

SELECT DISTINCT preferred_language FROM silver.crm_customers;

SELECT DISTINCT credit_score FROM silver.crm_customers ORDER BY credit_score DESC;


-- Check for unwanted spaces in high cardinality string fields
-- Expectations: No Result
SELECT
	first_name
FROM silver.crm_customers
WHERE first_name <> TRIM(first_name);

SELECT
	last_name
FROM silver.crm_customers
WHERE last_name <> TRIM(last_name);

SELECT
	company_name
FROM silver.crm_customers
WHERE company_name <> TRIM(company_name);

SELECT
	email
FROM silver.crm_customers
WHERE email <> TRIM(email);

SELECT
	phone_number
FROM silver.crm_customers
WHERE phone_number <> TRIM(phone_number);

SELECT
	address_line_1
FROM silver.crm_customers
WHERE address_line_1 <> TRIM(address_line_1);

SELECT
	zip_code
FROM silver.crm_customers
WHERE zip_code <> TRIM(zip_code);

SELECT
	national_id
FROM silver.crm_customers
WHERE national_id <> TRIM(national_id);



-- Check for NULLs & empty strings in high cardinality string fields
-- Expectation: No Result
SELECT
	first_name
FROM silver.crm_customers
WHERE first_name IS NULL OR TRIM(first_name) = ''; 

SELECT
	last_name
FROM silver.crm_customers
WHERE last_name IS NULL OR TRIM(last_name) = ''; 

SELECT 
	company_name
FROM silver.crm_customers
WHERE TRIM(company_name) = ''
ORDER BY company_name; 

SELECT
	national_id
FROM silver.crm_customers
WHERE TRIM(national_id) = ''
ORDER BY national_id;

SELECT
	email
FROM silver.crm_customers
WHERE email IS NULL OR TRIM(email) = ''
ORDER BY email;

SELECT
	phone_number
FROM silver.crm_customers
WHERE phone_number IS NULL OR phone_number = 'N/A' OR TRIM(phone_number) = ''
ORDER BY phone_number;

SELECT
	address_line_1
FROM silver.crm_customers
WHERE address_line_1 IS NULL OR TRIM(address_line_1) = ''
ORDER BY address_line_1;

SELECT
	zip_code
FROM silver.crm_customers
WHERE TRIM(zip_code) = '';  


-- Check for invalid dates
-- Expectation: No Result
SELECT
	date_of_birth
FROM silver.crm_customers
WHERE date_of_birth > GETDATE();

SELECT
	created_at
FROM silver.crm_customers
WHERE created_at > updated_at OR created_at > GETDATE();

SELECT
	updated_at
FROM silver.crm_customers
WHERE updated_at > GETDATE();

SELECT
	onboard_date
FROM silver.crm_customers
WHERE onboard_date > GETDATE();

SELECT
	customer_since
FROM silver.crm_customers
WHERE customer_since > YEAR(GETDATE());

SELECT
	onboard_date,
	customer_since
FROM silver.crm_customers
WHERE YEAR(onboard_date) <> customer_since;

SELECT
	onboard_date,
	customer_since
FROM silver.crm_customers
WHERE customer_since IS NULL AND onboard_date IS NOT NULL;


-- Check for invalid credit score
-- Expectation: No Result
SELECT
	credit_score
FROM silver.crm_customers
WHERE NOT credit_score BETWEEN 300 AND 850;


-- Check for invalid emails
-- Expectation: No Result
SELECT
	email
FROM silver.crm_customers
WHERE LEN(email) < 6 OR email NOT LIKE ('%@%.%');



--===========================================================================
-- Data Quality Checks on silver.cbs_accounts
--===========================================================================
USE BankingDW;
GO

-- Check for NULLs & duplicates in primary key
-- Expectation: No Result
SELECT
	account_id,
	COUNT(*) AS duplicate_chk
FROM silver.cbs_accounts
GROUP BY account_id
HAVING account_id IS NULL OR COUNT(*) > 1;

-- Check for invalid values in primary key
-- Expectation: No Result
SELECT
	account_id
FROM silver.cbs_accounts
WHERE TRIM(account_id) = '' OR account_id NOT LIKE ('ACC%');


-- Check for unwanted spaces in primary key
-- Expectation: No Result
SELECT
	account_id
FROM silver.cbs_accounts
WHERE account_id <> TRIM(account_id);


-- Check for invalid foreign keys
-- Expectation: No Result
SELECT
	customer_id
FROM silver.cbs_accounts
WHERE customer_id NOT IN
(SELECT customer_id FROM silver.crm_customers);


SELECT
	branch_id
FROM silver.cbs_accounts
WHERE branch_id NOT IN
(SELECT branch_id FROM silver.cbs_branches);



SELECT
	assigned_employee_id
FROM silver.cbs_accounts
WHERE assigned_employee_id NOT IN
(SELECT employee_id FROM silver.hrms_employees);


-- Check for unwanted spaces in foreign keys
-- Expectation: No Result
SELECT
	customer_id
FROM silver.cbs_accounts
WHERE customer_id <> TRIM(customer_id);

SELECT
	branch_id
FROM silver.cbs_accounts
WHERE branch_id <> TRIM(branch_id);

SELECT
	assigned_employee_id
FROM silver.cbs_accounts
WHERE assigned_employee_id <> TRIM(assigned_employee_id);


-- Check for invalid values in foreign keys
-- Expectations: No Result
SELECT
	customer_id
FROM silver.cbs_accounts
WHERE TRIM(customer_id) = '' OR customer_id NOT LIKE ('CUST%');

SELECT
	branch_id
FROM silver.cbs_accounts
WHERE TRIM(branch_id) = '' OR branch_id NOT LIKE ('BRN%');

SELECT
	assigned_employee_id
FROM silver.cbs_accounts
WHERE TRIM(assigned_employee_id) = '' OR assigned_employee_id NOT LIKE ('EMP%');


-- Check for unwanted spaces or empty strings in string fields
-- Expectations: No Result
SELECT
	account_type
FROM silver.cbs_accounts
WHERE account_type <> TRIM(account_type) OR TRIM(account_type) = '';

SELECT
	account_status
FROM silver.cbs_accounts
WHERE account_status <> TRIM(account_status) OR TRIM(account_status) = '';

SELECT
	currency_code
FROM silver.cbs_accounts
WHERE currency_code <> TRIM(currency_code) OR TRIM(currency_code) = '';



-- Data Standardization & Consistency in Low Cardinality Fields
-- Expectations: User-friendly values & No NULLs
SELECT DISTINCT
	account_type
FROM silver.cbs_accounts;

SELECT DISTINCT
	account_status
FROM silver.cbs_accounts;

SELECT DISTINCT
	currency_code
FROM silver.cbs_accounts;

SELECT DISTINCT
	is_primary
FROM silver.cbs_accounts;


-- Check for invalid dates
-- Expectation: No Result
SELECT
	open_date,
	close_date
FROM silver.cbs_accounts
WHERE open_date > close_date OR open_date > GETDATE();

SELECT
	created_at,
	updated_at
FROM silver.cbs_accounts
WHERE created_at > updated_at OR created_at > GETDATE();


SELECT
	updated_at
FROM silver.cbs_accounts
WHERE updated_at > SYSDATETIME();



-- Check for invalid metrics
-- Expectations: Metrics should align with business rules
-- Business Rule: Available Balance = Current Balance + Overdraft Limit
SELECT
	current_balance,
	available_balance,
	overdraft_limit
FROM silver.cbs_accounts
WHERE current_balance IS NULL OR current_balance <> available_balance - overdraft_limit OR
available_balance IS NULL OR available_balance < 0 OR available_balance <> current_balance + overdraft_limit OR
overdraft_limit IS NULL OR overdraft_limit < 0 OR overdraft_limit <> available_balance - current_balance;


-- Check for invalid interest rates
-- Expectation: No Result; Non-saving accounts should have interest rates equal to zero
SELECT
	interest_rate
FROM silver.cbs_accounts
WHERE interest_rate IS NULL OR (NOT(interest_rate >= 0 AND interest_rate <= 25));


SELECT
	interest_rate
FROM silver.cbs_accounts
WHERE account_type IN ('Business Checking', 'Checking') 
AND interest_rate > 0;



-- Check customers with multiple primary account
-- Expectation: No Result
SELECT
	customer_id,
	COUNT(account_id) AS total_accounts,
	COUNT(is_primary)AS primary_account_count
FROM silver.cbs_accounts
WHERE is_primary = 1
GROUP BY customer_id
HAVING COUNT(is_primary) > 1;


-- Check for customers with no primary account
-- Expectation: No Result
SELECT
	customer_id,
	COUNT(account_id) AS total_accounts,
	COUNT(CASE WHEN is_primary = 1 THEN 1 ELSE NULL END) AS primary_account_count
FROM silver.cbs_accounts
GROUP BY customer_id
HAVING COUNT(CASE WHEN is_primary = 1 THEN 1 ELSE NULL END) = 0;



--===========================================================================
-- Data Quality Checks on silver.cbs_branches
--===========================================================================

-- Check for NULLS & duplicates in branch_id
-- Expectation: No Result
SELECT
	branch_id,
	COUNT(*) AS duplicate_chk
FROM silver.cbs_branches
GROUP BY branch_id 
HAVING branch_id IS NULL OR COUNT(*) > 1;


-- Check for invalid values in primary key
-- Expectation: No Result
SELECT
	branch_id
FROM silver.cbs_branches
WHERE TRIM(branch_id) = '' OR branch_id NOT LIKE ('BRN%');


-- Check for unwanted spaces in primary key
-- Expectation: No Result
SELECT
	branch_id
FROM silver.cbs_branches
WHERE branch_id <> TRIM(branch_id);


-- Check for invalid employees
-- Expectations: No Result
SELECT manager_employee_id FROM silver.cbs_branches WHERE manager_employee_id NOT IN
(SELECT employee_id FROM silver.hrms_employees);


-- Check for invalid values in foreign keys
-- Expectations: No Result
SELECT
	manager_employee_id
FROM silver.cbs_branches
WHERE TRIM(manager_employee_id) = '' OR manager_employee_id NOT LIKE ('EMP%');


-- Check for unwanted spaces in foreign keys
-- Expectations: No Result
SELECT
	manager_employee_id
FROM silver.cbs_branches
WHERE manager_employee_id <> TRIM(manager_employee_id);


-- Check for unwanted spaces in string fields
-- Expectation: No Result
SELECT
	branch_name
FROM silver.cbs_branches
WHERE branch_name <> TRIM(branch_name);

SELECT
	branch_type
FROM silver.cbs_branches
WHERE branch_type <> TRIM(branch_type);

SELECT
	address_line_1
FROM silver.cbs_branches
WHERE address_line_1 <> TRIM(address_line_1);

SELECT
	city
FROM silver.cbs_branches
WHERE city <> TRIM(city);

SELECT
	[state]
FROM silver.cbs_branches
WHERE [state] <> TRIM([state]);

SELECT
	zip_code
FROM silver.cbs_branches
WHERE zip_code <> TRIM(zip_code);

SELECT
	country
FROM silver.cbs_branches
WHERE country <> TRIM(country);

SELECT
	phone_number
FROM silver.cbs_branches
WHERE phone_number <> TRIM(phone_number);

SELECT
	email
FROM silver.cbs_branches
WHERE email <> TRIM(email);

SELECT
	region
FROM silver.cbs_branches
WHERE region <> TRIM(region);



-- Check for NULLs & empty strings in high cardinality fields
-- Expectations: No Result
SELECT DISTINCT
	branch_name
FROM silver.cbs_branches
WHERE branch_name IS NULL OR branch_name = '';

SELECT DISTINCT
	address_line_1
FROM silver.cbs_branches
WHERE address_line_1 IS NULL OR address_line_1 = '';

SELECT DISTINCT
	city
FROM silver.cbs_branches
WHERE city IS NULL OR city = '';

SELECT DISTINCT
	zip_code
FROM silver.cbs_branches
WHERE zip_code IS NULL OR zip_code = '';

SELECT DISTINCT
	phone_number
FROM silver.cbs_branches
WHERE phone_number IS NULL OR phone_number = '';

SELECT DISTINCT
	email
FROM silver.cbs_branches
WHERE email IS NULL OR email = '';


-- Data Standardization & Consistency in Low Cardinality Fields
-- Expectation: No NULLS & User-Friendly Values
SELECT DISTINCT
	branch_type
FROM silver.cbs_branches;

SELECT DISTINCT
	[state]
FROM silver.cbs_branches;

SELECT DISTINCT
	zip_code
FROM silver.cbs_branches;

SELECT DISTINCT
	country
FROM silver.cbs_branches;

SELECT DISTINCT
	region
FROM silver.cbs_branches;


-- Check for invalid emails
-- Expectation: No Result
SELECT
	email
FROM silver.cbs_branches
WHERE email NOT LIKE ('%@%.%') OR LEN(email) < 6;


-- Check for invalid dates
-- Expectation: No Result
SELECT
	opened_date
FROM silver.cbs_branches
WHERE opened_date > GETDATE();


--===========================================================================
-- Data Quality Checks on silver.cbs_transactions
--===========================================================================

-- Check for NULLs & duplicates in primary key
-- Expectation: No Result
SELECT
	transaction_id,
	COUNT(*) AS duplicate_chk
FROM silver.cbs_transactions
GROUP BY transaction_id
HAVING transaction_id IS NULL OR COUNT(*) > 1;


-- Check for invalid values in primary key
-- Expectations: No Result
SELECT
	transaction_id
FROM silver.cbs_transactions
WHERE TRIM(transaction_id) = '' OR transaction_id NOT LIKE ('TXN%');


-- Check for unwanted spaces in primary key
-- Expectations: No Result
SELECT
	transaction_id
FROM silver.cbs_transactions
WHERE transaction_id <> TRIM(transaction_id);


-- Check for invalid foreign keys
-- Expectation: No Result
SELECT account_id FROM silver.cbs_transactions WHERE account_id NOT IN
(SELECT account_id FROM silver.cbs_accounts);


SELECT branch_id FROM silver.cbs_transactions WHERE branch_id NOT IN
(SELECT branch_id FROM silver.cbs_branches);

-- Check for invalid values in foreign key
-- Expectation: No Result
SELECT
	account_id
FROM silver.cbs_transactions
WHERE TRIM(account_id) = '' OR account_id NOT LIKE ('ACC%');


SELECT
	branch_id
FROM silver.cbs_transactions
WHERE TRIM(branch_id) = '' OR branch_id NOT LIKE ('BRN%');


-- Data Standardization & Consistency on Low Cardinality Fields
-- Expectation: User-Friendly Values, & No NULLs AND  Empty Strings
SELECT DISTINCT
	transaction_type
FROM silver.cbs_transactions;

SELECT DISTINCT
	debit_credit
FROM silver.cbs_transactions;

SELECT DISTINCT
	currency
FROM silver.cbs_transactions;

SELECT DISTINCT
	channel
FROM silver.cbs_transactions;

SELECT DISTINCT
	[status]
FROM silver.cbs_transactions;

SELECT DISTINCT
	merchant_name
FROM silver.cbs_transactions;

SELECT DISTINCT
	merchant_category
FROM silver.cbs_transactions;

SELECT DISTINCT
	is_flagged
FROM silver.cbs_transactions;


-- Check for unwanted spaces, empty strings & NULLs in high cardinality string fields
-- Expectation: No Result
SELECT
	reference_number
FROM silver.cbs_transactions
WHERE reference_number = '' OR reference_number IS NULL 
OR reference_number <> TRIM(reference_number);

SELECT
	[description]
FROM silver.cbs_transactions
WHERE [description] = '' OR [description] IS NULL 
OR [description] <> TRIM([description]);


-- Check for invalid dates
-- Expectations: No Result
SELECT
	transaction_date
FROM silver.cbs_transactions
WHERE transaction_date > GETDATE();


SELECT
	transaction_date_time
FROM silver.cbs_transactions
WHERE transaction_date_time > SYSDATETIME();

SELECT
	transaction_date,
	transaction_time,
	transaction_date_time
FROM silver.cbs_transactions
WHERE CONVERT(NVARCHAR, transaction_date, 120) + ' ' + CONVERT(NVARCHAR, transaction_time, 108) <> CONVERT(NVARCHAR, transaction_date_time, 120);

SELECT
	created_at
FROM silver.cbs_transactions
WHERE created_at > SYSDATETIME();



--===========================================================================
-- Data Quality Checks on silver.hrms_employees
--===========================================================================

-- Check for duplicates & NULLs in primary key
-- Expectation: No Result
SELECT
	employee_id,
	COUNT(*) AS duplicate_chk
FROM silver.hrms_employees
GROUP BY employee_id
HAVING employee_id IS NULL OR COUNT(*) > 1;


-- Check for invalid values in primary key
-- Expectation: No Result
SELECT
	employee_id
FROM silver.hrms_employees
WHERE TRIM(employee_id) = '' OR employee_id NOT LIKE ('EMP%'); 


-- Check for unwanted sapces in primary key
-- Expectation: No Result
SELECT
	employee_id
FROM silver.hrms_employees
WHERE employee_id <> TRIM(employee_id);


-- Check for unwanted spaces, empty strings, & invalid values in foreign key
-- Expectation: No Result
SELECT
	branch_id
FROM silver.hrms_employees
WHERE branch_id <> TRIM(branch_id) OR TRIM(branch_id) = '' OR branch_id NOT LIKE ('BRN%');

SELECT
	manager_id
FROM silver.hrms_employees
WHERE manager_id <> TRIM(manager_id) OR TRIM(manager_id) = '' OR manager_id NOT LIKE ('EMP%');


-- Check for invalid foreign keys
-- Expectation: No Result
SELECT branch_id FROM silver.hrms_employees WHERE branch_id NOT IN
(SELECT branch_id FROM silver.cbs_branches);

SELECT manager_id FROM silver.hrms_employees WHERE manager_id NOT IN
(SELECT employee_id FROM silver.hrms_employees);



-- Check for unwanted spaces in string fields
-- Expectation: No Result
SELECT
	first_name
FROM silver.hrms_employees
WHERE first_name <> TRIM(first_name);

SELECT
	last_name
FROM silver.hrms_employees
WHERE last_name <> TRIM(last_name);

SELECT
	email
FROM silver.hrms_employees
WHERE email <> TRIM(email);

SELECT
	phone_number
FROM silver.hrms_employees
WHERE phone_number <> TRIM(phone_number);

SELECT
	department
FROM silver.hrms_employees
WHERE department <> TRIM(department);

SELECT
	job_title
FROM silver.hrms_employees
WHERE job_title <> TRIM(job_title);


-- Check for Nulls and empty spaces in high cardinality fields
-- Expectation: No Result
SELECT
	first_name
FROM silver.hrms_employees
WHERE first_name IS NULL OR TRIM(first_name) = '';

SELECT
	last_name
FROM silver.hrms_employees
WHERE last_name IS NULL OR TRIM(last_name) = '';

SELECT
	email
FROM silver.hrms_employees
WHERE email IS NULL OR TRIM(email) = '';

SELECT
	phone_number
FROM silver.hrms_employees
WHERE phone_number IS NULL OR TRIM(phone_number) = '';

SELECT
	phone_number
FROM silver.hrms_employees
WHERE phone_number IS NULL OR TRIM(phone_number) = '';


-- Check for invalid emails
-- Expectation: No Result
SELECT
	email
FROM silver.hrms_employees
WHERE LEN(email) < 6 OR email NOT LIKE ('%@%.%');


-- Data Standardization & Consistency on Low Cardinality Fields
-- Expectation: User Friendly Values and No Nulls

SELECT DISTINCT department FROM silver.hrms_employees;

SELECT DISTINCT job_title FROM silver.hrms_employees;


-- Check for invalid dates
-- Expectation: No Result
SELECT
	hire_date,
	termination_date
FROM silver.hrms_employees
WHERE hire_date > termination_date OR hire_date > GETDATE();


-- Check for invalid active employees
-- Expectation: No Result

SELECT
	employee_id,
	is_active
FROM silver.hrms_employees
WHERE is_active = 1 AND termination_date < GETDATE();



-- Check for invalid inactive employees
-- Expectation: No Result
SELECT
	employee_id,
	termination_date,
	is_active
FROM silver.hrms_employees
WHERE is_active = 0 AND termination_date > GETDATE();

SELECT
	employee_id,
	termination_date,
	is_active
FROM silver.hrms_employees
WHERE is_active = 0 AND termination_date IS NULL;



--===========================================================================
-- Data Quality Checks on silver.los_loan_applications
--===========================================================================

-- Check for duplicates, NULLs, unwanted spaces and strings in primary key
-- Expectation: No Result
SELECT
	loan_id,
	COUNT(*) AS duplicate_chk
FROM silver.los_loan_applications
GROUP BY loan_id
HAVING loan_id IS NULL OR TRIM(loan_id) = '' OR loan_id <> TRIM(loan_id) OR COUNT(*) > 1
ORDER BY loan_id;


-- Check for invalid foreign keys
-- Expectation: No Result
SELECT customer_id FROM silver.los_loan_applications WHERE customer_id NOT IN
(SELECT customer_id FROM silver.crm_customers);

SELECT branch_id FROM silver.los_loan_applications WHERE branch_id NOT IN
(SELECT branch_id FROM silver.cbs_branches);

SELECT loan_officer_employee_id FROM silver.los_loan_applications WHERE loan_officer_employee_id NOT IN
(SELECT employee_id FROM silver.hrms_employees);


-- Check for unwanted strings & spaces in foreign keys
-- Expectation: No Result
SELECT
	customer_id
FROM silver.los_loan_applications
WHERE customer_id <> TRIM(customer_id) OR TRIM(customer_id) = '';

SELECT
	branch_id
FROM silver.los_loan_applications
WHERE branch_id <> TRIM(branch_id) OR TRIM(branch_id) = '';

SELECT
	loan_officer_employee_id
FROM silver.los_loan_applications
WHERE loan_officer_employee_id <> TRIM(loan_officer_employee_id) OR TRIM(loan_officer_employee_id) = '';


-- Check for unwanted values in primary key & foreign keys
-- Expectation: NULLs or/and IDs
SELECT DISTINCT loan_id FROM silver.los_loan_applications ORDER BY loan_id;
SELECT DISTINCT customer_id FROM silver.los_loan_applications ORDER BY customer_id;
SELECT DISTINCT branch_id FROM silver.los_loan_applications ORDER BY branch_id;
SELECT DISTINCT loan_officer_employee_id FROM silver.los_loan_applications ORDER BY loan_officer_employee_id;


-- Check for unwanted values in string fields
-- Expectations: No Result
SELECT
	loan_type
FROM silver.los_loan_applications
WHERE loan_type <> TRIM(loan_type);

SELECT
	loan_status
FROM silver.los_loan_applications
WHERE loan_status <> TRIM(loan_status);

SELECT
	collateral_type
FROM silver.los_loan_applications
WHERE collateral_type <> TRIM(collateral_type);

SELECT
	purpose_description
FROM silver.los_loan_applications
WHERE purpose_description <> TRIM(purpose_description);

SELECT
	rejection_reason
FROM silver.los_loan_applications
WHERE rejection_reason <> TRIM(rejection_reason);


-- Data Standardization & Consistency in String Fields
-- Expectation: User-Friendly Values & No NULLs
SELECT DISTINCT loan_type FROM silver.los_loan_applications ORDER BY loan_type;
SELECT DISTINCT loan_status FROM silver.los_loan_applications ORDER BY loan_status;
SELECT DISTINCT collateral_type FROM silver.los_loan_applications ORDER BY collateral_type;
SELECT DISTINCT purpose_description FROM silver.los_loan_applications ORDER BY purpose_description;
SELECT DISTINCT rejection_reason FROM silver.los_loan_applications ORDER BY rejection_reason;

-- Check for invalid dates
-- Expectation: No Result
SELECT
	application_date,
	decision_date,
	disbursement_date
FROM silver.los_loan_applications
WHERE application_date > decision_date OR application_date > disbursement_date 
OR application_date > GETDATE();


SELECT
	decision_date,
	disbursement_date
FROM silver.los_loan_applications
WHERE decision_date > disbursement_date OR decision_date > GETDATE();


SELECT
	disbursement_date
FROM silver.los_loan_applications
WHERE disbursement_date  > GETDATE();


SELECT
	created_at,
	updated_at
FROM silver.los_loan_applications
WHERE created_at > updated_at OR created_at > GETDATE();


SELECT
	created_at,
	updated_at
FROM silver.los_loan_applications
WHERE updated_at > GETDATE();
	

-- Check for invalid amounts
-- Expectation: No Result
SELECT
	requested_amount,
	approved_amount,
	disbursed_amount,
	(disbursed_amount - outstanding_balance) AS payment_made,
	outstanding_balance
FROM silver.los_loan_applications
WHERE disbursed_amount > requested_amount OR disbursed_amount > approved_amount OR disbursed_amount < 0 OR
approved_amount > requested_amount OR approved_amount < 0;


-- Check for invalid loan status
-- Expectation: No Result
SELECT
	requested_amount,
	approved_amount,
	disbursed_amount,
	outstanding_balance,
	days_delinquent,
	loan_status
FROM silver.los_loan_applications
WHERE (loan_status = 'Pending' OR loan_status = 'Rejected') AND (approved_amount IS NOT NULL OR disbursed_amount IS NOT NULL);


SELECT
	requested_amount,
	approved_amount,
	disbursed_amount,
	outstanding_balance,
	days_delinquent,
	loan_status
FROM silver.los_loan_applications
WHERE (loan_status = 'Closed') AND (approved_amount IS NULL OR disbursed_amount IS NULL OR outstanding_balance <> 0 OR days_delinquent <> 0);



SELECT
	requested_amount,
	approved_amount,
	disbursed_amount,
	outstanding_balance,
	days_delinquent,
	loan_status,
	interest_rate,
	term_months,
	monthly_payment
FROM silver.los_loan_applications
WHERE loan_status = 'Approved' AND approved_amount IS NULL;


SELECT
	requested_amount,
	approved_amount,
	disbursed_amount,
	outstanding_balance,
	days_delinquent,
	loan_status
FROM silver.los_loan_applications
WHERE loan_status = 'Disbursed' AND (approved_amount IS NULL OR disbursed_amount IS NULL OR outstanding_balance IS NULL);



SELECT
	requested_amount,
	approved_amount,
	disbursed_amount,
	outstanding_balance,
	days_delinquent,
	loan_status
FROM silver.los_loan_applications
WHERE loan_status = 'In Arrears' AND (approved_amount IS NULL OR disbursed_amount IS NULL OR outstanding_balance IS NULL OR days_delinquent = 0);



SELECT
	requested_amount,
	approved_amount,
	disbursed_amount,
	outstanding_balance,
	days_delinquent,
	loan_status
FROM silver.los_loan_applications
WHERE loan_status = 'Defaulted' AND (approved_amount IS NULL OR disbursed_amount IS NULL OR outstanding_balance IS NULL OR days_delinquent < 90);


-- Check for invalid Days Delinquent
-- Expectation: No Result
SELECT
	days_delinquent
FROM silver.los_loan_applications
WHERE loan_status IN('Pending', 'Closed', 'Approved', 'Rejected') AND days_delinquent <> 0;

SELECT
	days_delinquent
FROM silver.los_loan_applications
WHERE loan_status IN('In Arrears') AND days_delinquent = 0;

SELECT
	days_delinquent
FROM silver.los_loan_applications
WHERE loan_status IN('Defaulted') AND days_delinquent  < 90;


-- Check for invalid interest rate & term_months
-- Expectation: No Result
SELECT
	interest_rate,
	term_months
FROM silver.los_loan_applications
WHERE (NOT interest_rate BETWEEN 0 AND 40) OR
term_months < 0;



SELECT
	loan_status,
	interest_rate,
	term_months,
	monthly_payment
FROM silver.los_loan_applications
WHERE loan_status IN ('Pending', 'Rejected') AND (interest_rate IS NOT NULL OR term_months IS NOT NULL OR monthly_payment IS NOT NULL);


-- Check for invalid monthly payments
-- Expectation: No Result
SELECT
	loan_status,
	approved_amount,
	interest_rate,
	term_months,
	monthly_payment
FROM silver.los_loan_applications
WHERE loan_status IN ('Closed', 'Approved', 'Disbursed', 'In Arrears', 'Defaulted') AND ((monthly_payment IS NULL OR monthly_payment < 0 OR
monthly_payment - (approved_amount * (interest_rate/100/12))/ (1 - POWER((1 + (interest_rate/100/12)), -term_months)) > 1));
