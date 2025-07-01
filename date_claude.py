import pandas as pd
import numpy as np
import glob
import os
import re
from datetime import datetime

def convert_date_format(date_str):
    """
    Convert date from MM/DD/YYYY format to YYYY-MM-DD format.
    Returns the original value if conversion fails.
    """
    if pd.isna(date_str) or date_str == '':
        return date_str
    
    try:
        # Convert string to datetime object
        date_obj = datetime.strptime(str(date_str), '%m/%d/%Y')
        # Return in YYYY-MM-DD format
        return date_obj.strftime('%Y-%m-%d')
    except:
        # If conversion fails, try other common formats
        try:
            # Try if it's already in YYYY-MM-DD format
            datetime.strptime(str(date_str), '%Y-%m-%d')
            return str(date_str)
        except:
            # Return original value if all conversions fail
            return date_str

def transform_professional_to_database_format(input_file, output_file):
    """
    Transform the professional CSV into a normalized format suitable for RDBMS.
    Each negotiated_rate group becomes a separate row.
    
    Parameters:
    - input_file: Path to input CSV file (like professional.csv)
    - output_file: Path to output CSV file (like organize_data.csv format)
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Key columns that are constant for each billing code
    key_columns = ['billing_code', 'billing_code_type', 'billing_code_type_version', 'name']
    
    # Verify key columns exist
    for col in key_columns:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in the input file")
    
    # Find all negotiated_rates groups
    negotiated_rate_pattern = re.compile(r'negotiated_rates__(\d+)__')
    negotiated_rate_indices = set()
    
    for col in df.columns:
        match = negotiated_rate_pattern.search(col)
        if match:
            negotiated_rate_indices.add(int(match.group(1)))
    
    negotiated_rate_indices = sorted(negotiated_rate_indices)
    print(f"Found {len(negotiated_rate_indices)} negotiated_rate groups")
    
    # Create output rows
    output_rows = []
    
    # Process each row in the input
    for idx, row in df.iterrows():
        # Get the key values for this row
        key_values = {col: row[col] for col in key_columns}
        
        # Process each negotiated_rate group
        for rate_idx in negotiated_rate_indices:
            # Check if this negotiated_rate group has data
            rate_col = f'negotiated_rates__{rate_idx}__negotiated_prices__0__negotiated_rate'
            
            # Skip if no rate data exists
            if rate_col not in df.columns or pd.isna(row.get(rate_col)):
                continue
            
            # Create output row with key values
            output_row = key_values.copy()
            
            # Define the columns we want to extract for this negotiated_rate
            columns_to_extract = [
                'negotiated_prices__0__negotiated_type',
                'negotiated_prices__0__negotiated_rate',
                'negotiated_prices__0__expiration_date',
                'negotiated_prices__0__service_code',
                'negotiated_prices__0__billing_class',
                'negotiated_prices__0__billing_code_modifier'
            ]
            
            # Map the columns with dynamic indices to fixed column names
            for col_suffix in columns_to_extract:
                # Source column with dynamic index
                source_col = f'negotiated_rates__{rate_idx}__{col_suffix}'
                # Target column with fixed index (always 0)
                target_col = f'negotiated_rates__0__{col_suffix}'
                
                if source_col in df.columns:
                    output_row[target_col] = row[source_col]
                else:
                    output_row[target_col] = np.nan
            
            # Also check for provider_references if needed
            provider_ref_col = f'negotiated_rates__{rate_idx}__provider_references'
            if provider_ref_col in df.columns and pd.notna(row[provider_ref_col]):
                output_row['provider_references'] = row[provider_ref_col]
            
            output_rows.append(output_row)
    
    # Create output dataframe
    result_df = pd.DataFrame(output_rows)
    
    # Define the required output columns in the correct order
    required_columns = [
        'billing_code',
        'billing_code_type',
        'billing_code_type_version',
        'name',
        'negotiated_rates__0__negotiated_prices__0__billing_code_modifier',
        'negotiated_rates__0__negotiated_prices__0__expiration_date',
        'negotiated_rates__0__negotiated_prices__0__negotiated_rate',
        'negotiated_rates__0__negotiated_prices__0__negotiated_type',
        'negotiated_rates__0__negotiated_prices__0__service_code',
        'negotiated_rates__0__negotiated_prices__0__billing_class'
    ]
    
    # Ensure all required columns exist (add empty columns if missing)
    for col in required_columns:
        if col not in result_df.columns:
            result_df[col] = np.nan
    
    # Reorder columns to match desired output
    # Note: Changed the last column name to match organize_data.csv
    result_df = result_df[required_columns]
    
    # Convert expiration_date format from MM/DD/YYYY to YYYY-MM-DD
    if 'negotiated_rates__0__negotiated_prices__0__expiration_date' in result_df.columns:
        result_df['negotiated_rates__0__negotiated_prices__0__expiration_date'] = result_df[
            'negotiated_rates__0__negotiated_prices__0__expiration_date'
        ].apply(convert_date_format)
    
    # Rename the last column to match the expected format (if different in organize_data.csv)
    # Based on the file info, the last column in organize_data.csv is negotiated_rates__10__negotiated_prices__0__billing_class
    result_df.rename(columns={
        'negotiated_rates__0__negotiated_prices__0__billing_class': 'negotiated_rates__10__negotiated_prices__0__billing_class'
    }, inplace=True)
    
    # Remove rows where all negotiated data is empty
    negotiated_cols = [col for col in result_df.columns if 'negotiated_rates' in col]
    result_df = result_df.dropna(subset=negotiated_cols, how='all')
    
    # Save to CSV
    result_df.to_csv(output_file, index=False)
    print(f"Transformed data saved to: {output_file}")
    print(f"Created {len(result_df)} rows from {len(df)} original rows")
    
    return result_df

def process_multiple_files(input_pattern, output_dir):
    """
    Process multiple CSV files matching a pattern.
    
    Parameters:
    - input_pattern: Glob pattern for input files (e.g., '*.csv')
    - output_dir: Directory to save processed files
    """
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all matching files
    input_files = glob.glob(input_pattern)
    
    if not input_files:
        print(f"No files found matching pattern: {input_pattern}")
        return
    
    print(f"Found {len(input_files)} files to process")
    
    # Process each file
    for input_file in input_files:
        # Generate output filename
        base_name = os.path.basename(input_file)
        name_without_ext = os.path.splitext(base_name)[0]
        
        print(f"\nProcessing: {input_file}")
        try:
            # Transform to database format
            output_file = os.path.join(output_dir, f"{name_without_ext}_database_ready.csv")
            transform_professional_to_database_format(input_file, output_file)
                
        except Exception as e:
            print(f"Error processing {input_file}: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Single file processing
    # transform_professional_to_database_format(
    #     'professional.csv', 
    #     'professional_database_ready.csv'
    # )
    
    # Multiple files processing
    process_multiple_files(
        input_pattern='*professional*.csv',  # Process all professional CSV files
        output_dir='database_ready_files'
    )
