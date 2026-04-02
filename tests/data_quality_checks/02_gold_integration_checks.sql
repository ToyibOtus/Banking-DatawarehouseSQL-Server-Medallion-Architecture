/*
===================================================================================
Script    : 02_gold_integration_checks.sql
Location  : scripts/data_quality_checks/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-04-01
===================================================================================
Script Purpose:
    Validates referential integrity across all Gold layer dimension and fact tables.
    Each query checks for orphan foreign keys — rows where a surrogate key exists
    in one table but has no matching row in the referenced dimension.

    Expectation: Every query should return zero rows on a clean Gold load.
    Any rows returned indicate a broken join requiring investigation.

===================================================================================
Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-04-01 |  Initial creation                                |
===================================================================================
*/
-- Check for broken joins
-- Expectation: No Result
SELECT
	da.customer_key,
	dc.customer_key
FROM gold.dim_accounts da
LEFT JOIN gold.dim_customers dc
ON da.customer_key = dc.customer_key
WHERE (da.customer_key IS NOT NULL AND dc.customer_key IS NULL);


-- Check for broken joins
-- Expectation: No Result
SELECT
	da.branch_key,
	db.branch_key
FROM gold.dim_accounts da
LEFT JOIN gold.dim_branches db
ON da.branch_key = db.branch_key
WHERE (da.branch_key IS NOT NULL AND db.branch_key IS NULL);


-- Check for broken joins
-- Expectation: No Result
SELECT
	da.employee_key,
	de.employee_key
FROM gold.dim_accounts da
LEFT JOIN gold.dim_employees de
ON da.employee_key = de.employee_key
WHERE (da.employee_key IS NOT NULL AND de.employee_key IS NULL);


-- Check for broken joins
-- Expectation: No Result
SELECT
	db.manager_key,
	de.employee_key
FROM gold.dim_branches db
LEFT JOIN gold.dim_employees de
ON db.manager_key = de.employee_key
WHERE (db.manager_key IS NOT NULL AND de.employee_key IS NULL);


-- Check for broken joins
-- Expectation: No Result
SELECT
	dc.branch_key,
	db.branch_key
FROM gold.dim_customers dc
LEFT JOIN gold.dim_branches db
ON dc.branch_key = db.branch_key
WHERE (dc.branch_key IS NOT NULL AND db.branch_key IS NULL);


-- Check for broken joins
-- Expectation: No Result
SELECT 
	de1.employee_key,
	de1.manager_key,
	de2.employee_key,
	de2.manager_key
FROM gold.dim_employees de1
LEFT JOIN gold.dim_employees de2
ON de1.manager_key = de2.manager_key
WHERE de1.manager_key IS NOT NULL AND de2.manager_key IS NULL;



-- Check for broken joins
-- Expectation: No Result
SELECT
	da.account_key,
	ft.account_key
FROM gold.fact_transactions ft
LEFT JOIN gold.dim_accounts da
ON da.account_key = ft.account_key
WHERE (ft.account_key IS NOT NULL AND da.account_key IS NULL);


-- Check for broken joins
-- Expectation: No Result
SELECT
	db.branch_key,
	ft.branch_key
FROM gold.fact_transactions ft 
LEFT JOIN gold.dim_branches db
ON db.branch_key = ft.branch_key
WHERE (ft.branch_key IS NOT NULL AND db.branch_key IS NULL);


-- Check for broken joins
-- Expectation: No Result
SELECT
	la.customer_key,
	dc.customer_key
FROM gold.fact_loan_applications la
LEFT JOIN gold.dim_customers dc
ON la.customer_key = dc.customer_key
WHERE (la.customer_key IS NOT NULL AND dc.customer_key IS NULL);


-- Check for broken joins
-- Expectation: No Result
SELECT
	la.customer_key,
	la.branch_key,
	la.employee_key,
	dc.customer_key,
	db.branch_key,
	de.employee_key
FROM gold.fact_loan_applications la
LEFT JOIN gold.dim_customers dc
ON la.customer_key = dc.customer_key
LEFT JOIN gold.dim_branches db
ON la.branch_key = db.branch_key
LEFT JOIN gold.dim_employees de
ON la.employee_key = de.employee_key
WHERE
	(la.customer_key IS NOT NULL AND dc.customer_key IS NULL) OR
	(la.branch_key IS NOT NULL AND db.branch_key IS NULL) OR
	(la.employee_key IS NOT NULL AND de.employee_key IS NULL);
