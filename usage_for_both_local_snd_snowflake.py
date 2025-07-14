# Writes to BOTH local files AND Snowflake stage
results = validator.validate_tables(
    'PREM', 'OUTBOUND', ['ID'], 
    stage_name='@my_stage/',
    write_local_csv=True
)
