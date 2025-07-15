# Simple usage
validator = HealthcareDataValidator()
results = validator.validate_healthcare_data('PREM', 'OUTBOUND')

# With custom options
results = validator.validate_healthcare_data(
    prem_table='PREM',
    outbound_table='OUTBOUND',
    create_tables=True,      # Create Snowflake tables
    write_local_csv=True,    # Create local CSV files
    stage_name='@my_stage/'  # Custom stage location
)



# TO CREATE A STORED_PROCEDURE
CREATE OR REPLACE PROCEDURE validate_healthcare_data_sp(
    prem_table STRING,
    outbound_table STRING,
    create_tables BOOLEAN DEFAULT TRUE,
    stage_name STRING DEFAULT '@~/'
)
RETURNS STRING
LANGUAGE PYTHON
-- [Include the full script here]

-- Run the validation
CALL validate_healthcare_data_sp('PREM', 'OUTBOUND', TRUE, '@my_validation_stage/');
