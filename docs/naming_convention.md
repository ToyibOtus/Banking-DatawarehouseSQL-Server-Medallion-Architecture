# Naming Conventions

This document defines the naming standards applied consistently across all
database objects, scripts, and files in this project.

---

## General Rules

- All names use **snake_case** — lowercase words separated by underscores
- No spaces, hyphens, or special characters in any object name

---

## Schemas

Schemas group objects by architectural layer or function.

| Schema  | Purpose                                      |
|---------|----------------------------------------------|
| bronze  | Raw ingested data — as-is from source        |
| silver  | Cleansed and standardised data               |
| gold    | Business-ready dimensional model             |
| etl     | ETL procedures, logs, watermarks, and errors |

---

## Tables

### Bronze and Silver Tables

Prefixed with the source system name to indicate data origin: <source_system>_<object_name>

| Example                        | Description                        |
|--------------------------------|------------------------------------|
| bronze.cbs_accounts            | Raw CBS accounts data              |
| bronze.crm_customers           | Raw CRM customers data             |
| silver.hrms_employees          | Cleansed HRMS employees data       |
| silver.los_loan_applications   | Cleansed LOS loan applications     |

### Gold Tables

Prefixed with object type to indicate dimensional role: 
* dim_<object_name>: dimension table
* fact_<object_name>: fact table
* vw_<object_name>: analytical view

| Example                          | Description                        |
|----------------------------------|------------------------------------|
| gold.dim_customers               | Customer dimension                 |
| gold.dim_accounts                | Account dimension                  |
| gold.fact_transactions           | Transactions fact table            |
| gold.fact_loan_applications      | Loan applications fact table       |
| gold.vw_customers                | Analyst-facing view of dim_customers|

### ETL Log Tables

Named by function

| Table              | Purpose                                      |
|--------------------|----------------------------------------------|
| etl.batch_log      | Tracks every procedure execution             |
| etl.step_log       | Tracks every table load within a procedure   |
| etl.error_log      | Captures runtime errors with full context    |
| etl.dq_log         | Logs data quality assertion results          |
| etl.watermark      | Stores last successful load point per table  |

---

## Columns

* Bronze: as-is from source system
* Silver: as-is from source system
* Gold: user-friendly values 


### Surrogate Keys
<object_name>_key

| Example          | Description                        |
|------------------|------------------------------------|
| customer_key     | Surrogate PK for dim_customers     |
| account_key      | Surrogate PK for dim_accounts      |
| transaction_key  | Surrogate PK for fact_transactions |

### Audit Columns

Prefixed with underscore `_` to visually separate them from business columns:

**Examples:**
_source_system   
_source_file      
_batch_id         
_batch_date       
_load_timestamp   

---

## Stored Procedures

### ETL Load Procedures
etl.load_<layer_name>_<source_system>

| Procedure                  | Description                              |
|----------------------------|------------------------------------------|
| etl.load_bronze_cbs        | Loads all CBS Bronze tables              |
| etl.load_silver_crm        | Loads all CRM Silver tables              |
| etl.load_gold_hrms         | Loads all HRMS Gold tables               |
| etl.load_gold_los`         | Loads all LOS Gold tables                |

### Special Procedures
etl.reconcile_gold_keys: Cross-system surrogate key resolution

### Orchestrator Procedures
etl.run_<layer_name>_pipeline


| Procedure                      | Description                          |
|--------------------------------|--------------------------------------|
| etl.run_bronze_pipeline        | Executes all Bronze load procedures  |
| etl.run_silver_pipeline        | Executes all Silver load procedures  |
| etl.run_gold_pipeline          | Executes all Gold load procedures    |
| etl.run_master_orchestrator    | Executes full Bronze → Silver → Gold |

---

## Constraints

* Foreign key: fk_<column_name>_<object_name>
* Unique constraint: uq_<column_name>_<object_name>
* Check constraint: chk_<column_name>_<object_name>

| Example                                        | Description                       |
|------------------------------------------------|-----------------------------------|
| fk_customer_key_gold_dim_accounts              | FK on customer_key in dim_accounts|
| fk_branch_key_gold_fact_transactions           | FK on branch_key in fact_transactions|

---

## Scripts

### Naming Pattern
<sequence_number>_<script_name>.sql

Scripts are numbered sequentially within each folder to indicate execution order.

| Example                                | Description                        |
|----------------------------------------|------------------------------------|
| 01_create_database.sql                 | Database initialisation            |
| 02_create_log_tables.sql               | ETL log table DDL                  |
| 01_bronze_tables.sql                   | Bronze DDL                         |
| 01_etl_load_bronze_cbs.sql             | Bronze CBS load procedure          |
| 01_etl_run_bronze_pipeline.sql         | Bronze orchestrator                |
