-- Create the stored procedure
CREATE OR REPLACE PROCEDURE validate_data_tables(
    table1_name STRING,
    table2_name STRING,
    key_columns STRING DEFAULT NULL,
    stage_name STRING DEFAULT '@~/'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'validate_tables_sp'
AS
$$
# Paste the entire script here
$$;

-- Run the validation
CALL validate_data_tables('PREM', 'OUTBOUND', 'ID', '@~/');
