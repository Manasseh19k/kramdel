import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime
import glob
import os

def load_csv_to_sql_server(csv_file, connection_string, table_name, column_mapping, batch_size=1000):
    """
    Load CSV data to SQL Server with column mapping and transformations.
    
    Parameters:
    - csv_file: Path to CSV file
    - connection_string: SQL Server connection string
    - table_name: Target table name in SQL Server
    - column_mapping: Dictionary mapping CSV columns to database columns
    - batch_size: Number of rows to insert at once
    """
    
    # Read CSV file
    print(f"Reading CSV file: {csv_file}")
    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} rows")
    
    # Add load_date column with current date
    df['load_date'] = datetime.now()
    
    # Transform 'institutional' to 'I' in billing_class column
    if 'negotiated_rates__0__negotiated_prices__0__billing_class' in df.columns:
        df['negotiated_rates__0__negotiated_prices__0__billing_class'] = df[
            'negotiated_rates__0__negotiated_prices__0__billing_class'
        ].replace('institutional', 'I')
    
    # Create a new dataframe with mapped columns
    mapped_df = pd.DataFrame()
    
    for csv_col, db_col in column_mapping.items():
        if csv_col in df.columns:
            mapped_df[db_col] = df[csv_col]
        elif csv_col == 'load_date':
            mapped_df[db_col] = df['load_date']
        else:
            print(f"Warning: CSV column '{csv_col}' not found")
    
    # Connect to SQL Server
    print(f"Connecting to SQL Server...")
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    # Get the column names for the INSERT statement
    db_columns = list(mapped_df.columns)
    placeholders = ','.join(['?' for _ in db_columns])
    columns_str = ','.join([f'[{col}]' for col in db_columns])
    
    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    # Insert data in batches
    total_rows = len(mapped_df)
    rows_inserted = 0
    
    print(f"Inserting data into {table_name}...")
    
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = mapped_df.iloc[start_idx:end_idx]
        
        # Convert dataframe to list of tuples
        batch_data = []
        for _, row in batch_df.iterrows():
            # Replace NaN with None for SQL NULL
            row_data = [None if pd.isna(val) else val for val in row]
            batch_data.append(tuple(row_data))
        
        try:
            cursor.executemany(insert_query, batch_data)
            conn.commit()
            rows_inserted += len(batch_data)
            print(f"Inserted {rows_inserted}/{total_rows} rows")
        except Exception as e:
            print(f"Error inserting batch: {e}")
            conn.rollback()
            raise
    
    cursor.close()
    conn.close()
    
    print(f"Successfully loaded {rows_inserted} rows into {table_name}")
    return rows_inserted



def process_multiple_csv_files(csv_pattern, connection_string, table_name, column_mapping, batch_size=1000):
    """
    Process multiple CSV files and load them into SQL Server.
    
    Parameters:
    - csv_pattern: Glob pattern for CSV files
    - connection_string: SQL Server connection string
    - table_name: Target table name
    - column_mapping: Column mapping dictionary
    - batch_size: Batch size for inserts
    """
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        print(f"No files found matching pattern: {csv_pattern}")
        return
    
    print(f"Found {len(csv_files)} files to process")
    
    total_rows_loaded = 0
    
    for csv_file in csv_files:
        print(f"\nProcessing: {csv_file}")
        try:
            rows_loaded = load_csv_to_sql_server(
                csv_file, 
                connection_string, 
                table_name, 
                column_mapping, 
                batch_size
            )
            total_rows_loaded += rows_loaded
        except Exception as e:
            print(f"Error processing {csv_file}: {e}")
    
    print(f"\nTotal rows loaded: {total_rows_loaded}")
    return total_rows_loaded

# Example usage
if __name__ == "__main__":
    # SQL Server connection string with Windows Authentication (no password)
    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=your_server_name;"
        "DATABASE=your_database_name;"
        "Trusted_Connection=yes"
    )
    
    # Define column mapping from CSV to database
    # Format: 'csv_column_name': 'database_column_name'
    column_mapping = {
        'name': 'charge_description',
        'billing_code_type': 'code_type',
        'billing_code_type_version': 'code_type_version',
        'billing_code': 'charge_code',
        'negotiated_rates__0__provider_groups__0__npi': 'provider_npi',
        'negotiated_rates__0__provider_groups__0__tin__type': 'tin_type',
        'negotiated_rates__0__provider_groups__0__tin__value': 'tin_value',
        'negotiated_rates__0__negotiated_prices__0__negotiated_type': 'rate_type',
        'negotiated_rates__0__negotiated_prices__0__negotiated_rate': 'negotiated_amount',
        'negotiated_rates__0__negotiated_prices__0__expiration_date': 'rate_expiration_date',
        'negotiated_rates__0__negotiated_prices__0__service_code': 'service_code',
        'negotiated_rates__0__negotiated_prices__0__billing_class': 'billing_class',
        'negotiated_rates__1__negotiated_prices__0__billing_code_modifier': 'modifier_code',
        'load_date': 'load_date'  # This will be added automatically
    }
    
    # Table name in SQL Server
    table_name = '[dbo].[hospital_rates]'  # Update with your table name
    
    # Load single CSV file
    # load_csv_to_sql_server(
    #     csv_file='your_file_database_ready.csv',
    #     connection_string=connection_string,
    #     table_name=table_name,
    #     column_mapping=column_mapping,
    #     batch_size=1000
    # )
    
    # Load multiple CSV files
    process_multiple_csv_files(
        csv_pattern='database_ready_files/*.csv',
        connection_string=connection_string,
        table_name=table_name,
        column_mapping=column_mapping,
        batch_size=1000
    )
