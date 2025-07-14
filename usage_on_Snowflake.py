# In Snowflake Notebook or Stored Procedure
validator = SnowflakeDataValidator()

results = validator.validate_tables(
    table1_name='PREM',
    table2_name='OUTBOUND', 
    key_columns=['ID', 'CUSTOMER_ID'],  # Your key columns
    stage_name='@my_validation_stage/'  # Optional: custom stage
)

# Check results
print(f"Validation passed: {results['validation_passed']}")
print(f"Mismatched rows: {results['mismatched_rows_count']}")
