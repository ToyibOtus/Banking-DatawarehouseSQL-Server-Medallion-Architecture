# Data Dictionary — Gold Layer

---

## Overview
The Gold layer contains business-ready data structured to support BI analytics
and reporting. It is composed of two table types:

- **Dimension Tables** — descriptive context for analytical queries
- **Fact Tables** — measurable business events and metrics

All Gold tables carry ETL audit columns prefixed with `_` for data lineage
traceability. SCD2 tables carry additional versioning columns (`_valid_from`,
`_valid_to`, `_is_active`) to preserve historical attribute changes.

---

## 01. gold.dim_customers
**Purpose:** Houses current and historical customer profile information sourced
from the CRM system. SCD2 is applied to analytically significant attributes —
a new row is inserted when these change, preserving full customer history.

| Column | Data Type | Description |
|--------|-----------|-------------|
| customer_key | INT | Surrogate primary key. Uniquely identifies each version of a customer record. |
| customer_id | NVARCHAR(50) | Natural key from the CRM source system (e.g. CUST000001). |
| customer_name | NVARCHAR(100) | Full name of the customer (e.g. Adeyemi John). |
| company_name | NVARCHAR(50) | Company name for business customers. NULL for individual customers. |
| segment | NVARCHAR(50) | Customer classification by relationship tier (e.g. Retail, SME, Corporate). SCD2 tracked. |
| risk_band | NVARCHAR(50) | Credit risk classification assigned by the bank (e.g. Low, Medium, High). SCD2 tracked. |
| date_of_birth | DATE | Customer's date of birth (e.g. 1990-04-15). SCD2 tracked. |
| gender | NVARCHAR(50) | Customer's gender (e.g. Male, Female). SCD2 tracked. |
| national_id | NVARCHAR(50) | Government-issued national identification number. |
| email | NVARCHAR(250) | Customer's primary email address. SCD1 — overwritten on change. |
| phone_number | NVARCHAR(50) | Customer's primary contact number. SCD1 — overwritten on change. |
| address_line_1 | NVARCHAR(200) | First line of customer's residential or business address. SCD2 tracked. |
| city | NVARCHAR(50) | City of customer's address (e.g. Lagos). SCD2 tracked. |
| state | NVARCHAR(50) | State or province of customer's address (e.g. Lagos State). SCD2 tracked. |
| zip_code | NVARCHAR(50) | Postal code of customer's address (e.g. 100001). SCD2 tracked. |
| country | NVARCHAR(50) | Country of customer's address (e.g. Nigeria). SCD2 tracked. |
| onboard_date | DATE | Date the customer was first onboarded with the bank. |
| branch_key | INT | Surrogate FK to dim_branches. References the branch where the customer was onboarded. |
| branch_id | NVARCHAR(50) | Natural key of the onboarding branch (e.g. BRN0001). |
| customer_since | INT | Year the customer relationship with the bank began (e.g. 2018). |
| is_active | BIT | Indicates whether the customer is currently active with the bank (1 = Active, 0 = Inactive). SCD2 tracked. |
| marketing_opt_in | BIT | Indicates whether the customer has opted into marketing communications (1 = Opted In, 0 = Opted Out). SCD2 tracked. |
| preferred_language | NVARCHAR(50) | Customer's preferred communication language (e.g. English, French). SCD1 — overwritten on change. |
| annual_income | DECIMAL(18,2) | Customer's declared annual income in local currency (e.g. 5000000.00). SCD2 tracked. |
| credit_score | INT | Numerical credit score assigned to the customer (e.g. 720). SCD2 tracked. |
| created_at | DATE | Date the customer record was originally created in the CRM source system. |
| updated_at | DATE | Date the customer record was last modified in the CRM source system. |
| _source_system | NVARCHAR(50) | ETL audit — identifies the source system that produced this record (CRM). |
| _batch_id | INT | ETL audit — references the batch run that created or last updated this row. |
| _created_at | DATETIME2 | ETL audit — timestamp when this row was first inserted into Gold. |
| _updated_at | DATETIME2 | ETL audit — timestamp when this row was last modified in Gold. |
| _valid_from | DATE | SCD2 — date from which this version of the customer record became effective. |
| _valid_to | DATE | SCD2 — date on which this version was superseded. NULL if current. |
| _is_active | BIT | SCD2 — flags the current version of the customer record (1 = Current, 0 = Historical). |

---

## 02. gold.dim_employees
**Purpose:** Houses employee profile information sourced from the HRMS system.
SCD1 only — all changes overwrite existing records in place. One row per
employee represents their current state.

| Column | Data Type | Description |
|--------|-----------|-------------|
| employee_key | INT | Surrogate primary key. Uniquely identifies each employee record. |
| employee_id | NVARCHAR(50) | Natural key from the HRMS source system (e.g. EMP000001). |
| employee_name | NVARCHAR(100) | Full name of the employee (e.g. Fatima Bello). |
| email | NVARCHAR(250) | Employee's work email address. SCD1 — overwritten on change. |
| phone_number | NVARCHAR(50) | Employee's work contact number. SCD1 — overwritten on change. |
| department | NVARCHAR(50) | Department the employee belongs to (e.g. Retail Banking, Risk). |
| job_title | NVARCHAR(50) | Employee's current job title (e.g. Branch Manager, Loan Officer). |
| branch_key | INT | Surrogate FK to dim_branches. References the branch the employee is assigned to. |
| branch_id | NVARCHAR(50) | Natural key of the employee's assigned branch (e.g. BRN0001). |
| hire_date | DATE | Date the employee was hired by the bank. |
| termination_date | DATE | Date the employee's contract was terminated. NULL if currently employed. |
| salary | DECIMAL(18,2) | Employee's current gross salary in local currency (e.g. 1200000.00). |
| is_active | BIT | Indicates whether the employee is currently employed (1 = Active, 0 = Terminated). |
| manager_key | INT | Surrogate FK self-referencing employee_key. References the employee's direct line manager. |
| manager_id | NVARCHAR(50) | Natural key of the employee's direct line manager (e.g. EMP000010). |
| manager_name | NVARCHAR(100) | Full name of the employee's direct line manager (e.g. Chukwuemeka Obi). |
| _source_system | NVARCHAR(50) | ETL audit — identifies the source system that produced this record (HRMS). |
| _batch_id | INT | ETL audit — references the batch run that created or last updated this row. |
| _created_at | DATETIME2 | ETL audit — timestamp when this row was first inserted into Gold. |
| _updated_at | DATETIME2 | ETL audit — timestamp when this row was last modified in Gold. |

---

## 03. gold.dim_branches
**Purpose:** Houses branch profile and location information sourced from the
CBS system. SCD1 only — all changes overwrite existing records in place.
One row per branch represents its current state.

| Column | Data Type | Description |
|--------|-----------|-------------|
| branch_key | INT | Surrogate primary key. Uniquely identifies each branch record. |
| branch_id | NVARCHAR(50) | Natural key from the CBS source system (e.g. BRN0001). |
| branch_name | NVARCHAR(200) | Full name of the branch (e.g. Victoria Island Branch). |
| branch_type | NVARCHAR(50) | Classification of the branch by operational type (e.g. Full Service, Digital, Kiosk). |
| address_line_1 | NVARCHAR(200) | First line of the branch's physical address. |
| city | NVARCHAR(50) | City where the branch is located (e.g. Lagos). |
| state | NVARCHAR(50) | State where the branch is located (e.g. Lagos State). |
| zip_code | NVARCHAR(50) | Postal code of the branch location (e.g. 101001). |
| country | NVARCHAR(50) | Country where the branch is located (e.g. Nigeria). |
| phone_number | NVARCHAR(50) | Branch's primary contact number. |
| email | NVARCHAR(250) | Branch's primary email address. |
| opened_date | DATE | Date the branch was officially opened. |
| is_active | BIT | Indicates whether the branch is currently operational (1 = Active, 0 = Closed). |
| region | NVARCHAR(50) | Geographic or operational region the branch belongs to (e.g. South West, North Central). |
| manager_key | INT | Surrogate FK to dim_employees. References the employee who manages this branch. |
| manager_id | NVARCHAR(50) | Natural key of the branch manager from the HRMS source system (e.g. EMP000042). |
| _source_system | NVARCHAR(50) | ETL audit — identifies the source system that produced this record (CBS). |
| _batch_id | INT | ETL audit — references the batch run that created or last updated this row. |
| _created_at | DATETIME2 | ETL audit — timestamp when this row was first inserted into Gold. |
| _updated_at | DATETIME2 | ETL audit — timestamp when this row was last modified in Gold. |

---

## 04. gold.dim_accounts
**Purpose:** Houses current and historical account information sourced from
the CBS system. SCD2 is applied to analytically significant attributes —
a new row is inserted when these change, preserving the full history of
account state changes.

| Column | Data Type | Description |
|--------|-----------|-------------|
| account_key | INT | Surrogate primary key. Uniquely identifies each version of an account record. |
| account_id | NVARCHAR(50) | Natural key from the CBS source system (e.g. ACC0000001). |
| customer_key | INT | Surrogate FK to dim_customers. References the customer version active at account load time. |
| customer_id | NVARCHAR(50) | Natural key of the account holder from the CRM source system (e.g. CUST000001). |
| account_type | NVARCHAR(50) | Classification of the account by product type (e.g. Savings, Current, Fixed Deposit). |
| account_status | NVARCHAR(50) | Current lifecycle status of the account (e.g. Active, Dormant, Closed). SCD2 tracked. |
| open_date | DATE | Date the account was originally opened. Immutable — never changes. |
| close_date | DATE | Date the account was closed. NULL if the account is still open. SCD2 tracked. |
| currency_code | NVARCHAR(50) | ISO currency code of the account (e.g. NGN, USD, GBP). |
| overdraft_limit | DECIMAL(18,2) | Maximum overdraft credit facility extended to the account (e.g. 500000.00). SCD2 tracked. |
| interest_rate | DECIMAL(12,4) | Annual interest rate applied to the account (e.g. 0.0350 = 3.50%). SCD2 tracked. |
| branch_key | INT | Surrogate FK to dim_branches. References the branch where the account is domiciled. |
| branch_id | NVARCHAR(50) | Natural key of the account's domicile branch (e.g. BRN0001). |
| employee_key | INT | Surrogate FK to dim_employees. References the relationship manager assigned to the account. |
| employee_id | NVARCHAR(50) | Natural key of the assigned relationship manager (e.g. EMP000087). |
| is_primary | BIT | Indicates whether this is the customer's primary account (1 = Primary, 0 = Secondary). SCD2 tracked. |
| created_at | DATE | Date the account record was originally created in the CBS source system. |
| updated_at | DATE | Date the account record was last modified in the CBS source system. |
| _source_system | NVARCHAR(50) | ETL audit — identifies the source system that produced this record (CBS). |
| _batch_id | INT | ETL audit — references the batch run that created or last updated this row. |
| _created_at | DATETIME2 | ETL audit — timestamp when this row was first inserted into Gold. |
| _updated_at | DATETIME2 | ETL audit — timestamp when this row was last modified in Gold. |
| _valid_from | DATE | SCD2 — date from which this version of the account record became effective. |
| _valid_to | DATE | SCD2 — date on which this version was superseded. NULL if current. |
| _is_active | BIT | SCD2 — flags the current version of the account record (1 = Current, 0 = Historical). |

---

## 05. gold.fact_transactions
**Purpose:** Records all credit and debit transactional events at account level
sourced from the CBS system. Append-only — rows are never updated after
insertion. One row per transaction event.

| Column | Data Type | Description |
|--------|-----------|-------------|
| transaction_key | INT | Surrogate primary key. Uniquely identifies each transaction row. |
| transaction_id | NVARCHAR(50) | Natural key from the CBS source system (e.g. TXN000000001). |
| account_key | INT | Surrogate FK to dim_accounts. References the account version active at transaction time. |
| transaction_type | NVARCHAR(50) | Classification of the transaction by type (e.g. Deposit, Withdrawal, Transfer, Payment). |
| amount | DECIMAL(18,2) | Monetary value of the transaction in account currency (e.g. 150000.00). |
| debit_credit | NVARCHAR(50) | Indicates the direction of the transaction (D = Debit, C = Credit). |
| currency | NVARCHAR(50) | ISO currency code of the transaction (e.g. NGN, USD). |
| transaction_date | DATE | Calendar date on which the transaction occurred. |
| transaction_time | TIME(0) | Time at which the transaction occurred (e.g. 14:32:00). |
| transaction_date_time | DATETIME2(0) | Combined date and time of the transaction (e.g. 2024-03-15 14:32:00). |
| channel | NVARCHAR(50) | Channel through which the transaction was initiated (e.g. Mobile, ATM, Branch, Online). |
| status | NVARCHAR(50) | Settlement status of the transaction (e.g. Completed, Pending, Failed, Reversed). |
| balance_after | DECIMAL(18,2) | Account ledger balance immediately after the transaction settled (e.g. 4850000.00). |
| counterpart_account_id | NVARCHAR(50) | Natural key of the receiving or sending account for transfer transactions. NULL for non-transfer types. May reference external accounts not in this system. |
| merchant_name | NVARCHAR(50) | Name of the merchant for point-of-sale or payment transactions (e.g. Shoprite Nigeria). NULL for non-merchant transactions. |
| merchant_category | NVARCHAR(50) | Category of the merchant (e.g. Groceries, Utilities, Entertainment). NULL for non-merchant transactions. |
| reference_number | NVARCHAR(50) | Unique reference number assigned to the transaction by the CBS (e.g. REF20240315001). |
| description | NVARCHAR(500) | Free-text description or narration of the transaction (e.g. Salary Payment March 2024). |
| branch_key | INT | Surrogate FK to dim_branches. References the branch where the transaction was initiated. NULL for digital channel transactions. |
| is_flagged | BIT | Indicates whether the transaction has been flagged for fraud or compliance review (1 = Flagged, 0 = Not Flagged). |
| created_at | DATETIME2(0) | Timestamp when the transaction record was created in the CBS source system. |
| _source_system | NVARCHAR(50) | ETL audit — identifies the source system that produced this record (CBS). |
| _batch_id | INT | ETL audit — references the batch run that inserted this row. |
| _created_at | DATETIME2 | ETL audit — timestamp when this row was inserted into Gold. |

---

## 06. gold.fact_loan_applications
**Purpose:** Tracks the full lifecycle of loan applications sourced from the
LOS system. Accumulating snapshot — one row per loan, updated in place as
the loan progresses through each lifecycle stage from application to closure.

| Column | Data Type | Description |
|--------|-----------|-------------|
| loan_key | INT | Surrogate primary key. Uniquely identifies each loan application record. |
| loan_id | NVARCHAR(50) | Natural key from the LOS source system (e.g. LOAN000000001). |
| customer_key | INT | Surrogate FK to dim_customers. References the customer version active at loan load time. |
| branch_key | INT | Surrogate FK to dim_branches. References the branch where the loan was originated. |
| employee_key | INT | Surrogate FK to dim_employees. References the loan officer who processed the application. |
| loan_type | NVARCHAR(50) | Classification of the loan by product type (e.g. Personal, Mortgage, Business, Auto). |
| loan_status | NVARCHAR(50) | Current lifecycle stage of the loan (e.g. Pending, Approved, Disbursed, Closed, Defaulted, Rejected). Updated as loan progresses. |
| application_date | DATE | Date the loan application was submitted by the customer. |
| decision_date | DATE | Date a credit decision was made on the application. NULL if still pending. |
| disbursement_date | DATE | Date the approved loan amount was disbursed to the customer. NULL if not yet disbursed. |
| requested_amount | DECIMAL(18,2) | Loan amount requested by the customer (e.g. 5000000.00). |
| approved_amount | DECIMAL(18,2) | Loan amount approved by the bank. May differ from requested amount. NULL if not yet approved. |
| disbursed_amount | DECIMAL(18,2) | Actual amount disbursed to the customer. May differ from approved amount. NULL if not yet disbursed. |
| amount_paid | DECIMAL(18,2) | Cumulative repayment amount made by the customer to date (e.g. 750000.00). |
| outstanding_balance | DECIMAL(18,2) | Remaining loan balance yet to be repaid (e.g. 4250000.00). |
| interest_rate | DECIMAL(12,4) | Annual interest rate applied to the loan (e.g. 0.1800 = 18.00%). |
| term_months | INT | Agreed repayment duration in months (e.g. 24). |
| monthly_payment | DECIMAL(18,2) | Scheduled monthly repayment amount (e.g. 208333.33). |
| days_delinquent | INT | Number of days the loan repayment is overdue. 0 if current. |
| collateral_type | NVARCHAR(50) | Type of asset pledged as collateral (e.g. Property, Vehicle, Stocks). NULL for unsecured loans. |
| collateral_value | DECIMAL(18,2) | Estimated market value of the pledged collateral (e.g. 12000000.00). NULL for unsecured loans. |
| purpose_description | NVARCHAR(500) | Customer-stated purpose for the loan (e.g. Business expansion, Home renovation). |
| rejection_reason | NVARCHAR(500) | Reason provided for loan rejection. NULL if not rejected (e.g. Insufficient income, Poor credit score). |
| created_at | DATE | Date the loan application record was originally created in the LOS source system. |
| updated_at | DATE | Date the loan application record was last modified in the LOS source system. |
| _source_system | NVARCHAR(50) | ETL audit — identifies the source system that produced this record (LOS). |
| _batch_id | INT | ETL audit — references the batch run that created or last updated this row. |
| _created_at | DATETIME2 | ETL audit — timestamp when this row was first inserted into Gold. |
| _updated_at | DATETIME2 | ETL audit — timestamp when this row was last modified in Gold. |
