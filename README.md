## Data Pipeline Assignment

---

## Summary of Work Completed

### ✅ Part 1: Data Extraction
- Used Python to fetch data and save as `posts_transformed.csv`.
- Performed transformations using pandas.

### ✅ Part 2: Cloud Storage
- Uploaded the transformed file to an AWS S3 bucket 

### ✅ Part 3: Snowflake Integration
- Connected Python script to Snowflake using `snowflake-connector-python[pandas]`.
- Created:
  - `RAW_POSTS` table
  - `TRANSFORMED_POSTS_VIEW`
  - `FINAL_POSTS_VIEW`

---

## Environment & Setup
- Python: 3.11 (venv recommended)
- Recommended libraries:
```bash
pip install requests pandas boto3 "snowflake-connector-python[pandas]"
```

Notes:
- Adjust the Python version and venv name as needed.

## Snowflake Setup (example SQL)
Use these statements as a starting point for creating the warehouse, database, and schema.

```sql
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS MY_DB;
CREATE SCHEMA IF NOT EXISTS MY_DB.PUBLIC;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE MY_DB TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA MY_DB.PUBLIC TO ROLE ACCOUNTADMIN;
```

Adjust roles, privileges, and identifiers to match your account and security model.

```

## Challenges & Fixes
- Snowflake account identifier mismatch — ensure you use the dedicated Snowflake account identifier from your login URL.
- Column name case sensitivity — normalize column names to uppercase before upload.
- Datetime with timezone warnings — set logical-type flags (e.g., `use_logical_type=True`) when writing via pandas helpers.

---