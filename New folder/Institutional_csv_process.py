import pandas as pd
import numpy as np
import glob
import os
import re

def transform_to_database_format(input_file, output_file):
    """
    Transform the JSON-extracted CSV into a normalized format suitable for RDBMS.
    Each negotiated_rate group with each provider_group becomes a separate row.
    
    Parameters:
    - input_file: Path to input CSV file (like Book1.xlsx converted to CSV)
    - output_file: Path to output CSV file (like organize_data.xlsx format)
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Key columns that are constant for each billing code
    key_columns = ['name', 'billing_code_type', 'billing_code_type_version', 'billing_code']
    
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
            # Find all provider_groups within this negotiated_rate
            provider_pattern = re.compile(f'negotiated_rates__{rate_idx}__provider_groups__(\d+)__')
            provider_indices = set()
            
            for col in df.columns:
                match = provider_pattern.search(col)
                if match:
                    provider_indices.add(int(match.group(1)))
            
            provider_indices = sorted(provider_indices)
            
            # If no provider groups, check if there's still negotiated price data
            if not provider_indices:
                # Check if there's negotiated price data without provider groups
                price_cols = [col for col in df.columns if f'negotiated_rates__{rate_idx}__negotiated_prices__0__' in col]
                if price_cols:
                    provider_indices = [0]  # Create a dummy provider group
            
            # For each provider group, create a row
            for provider_idx in provider_indices:
                # Skip if no data exists
                npi_col = f'negotiated_rates__{rate_idx}__provider_groups__{provider_idx}__npi'
                price_rate_col = f'negotiated_rates__{rate_idx}__negotiated_prices__0__negotiated_rate'
                
                # Check if this combination has actual data
                has_provider_data = npi_col in df.columns and pd.notna(row.get(npi_col))
                has_price_data = price_rate_col in df.columns and pd.notna(row.get(price_rate_col))
                
                if not has_provider_data and not has_price_data:
                    continue
                
                # Create output row
                output_row = key_values.copy()
                
                # Map the columns with dynamic indices to fixed column names
                column_mapping = {
                    'negotiated_rates__0__provider_groups__0__npi': f'negotiated_rates__{rate_idx}__provider_groups__{provider_idx}__npi',
                    'negotiated_rates__0__provider_groups__0__tin__type': f'negotiated_rates__{rate_idx}__provider_groups__{provider_idx}__tin__type',
                    'negotiated_rates__0__provider_groups__0__tin__value': f'negotiated_rates__{rate_idx}__provider_groups__{provider_idx}__tin__value',
                    'negotiated_rates__0__negotiated_prices__0__negotiated_type': f'negotiated_rates__{rate_idx}__negotiated_prices__0__negotiated_type',
                    'negotiated_rates__0__negotiated_prices__0__negotiated_rate': f'negotiated_rates__{rate_idx}__negotiated_prices__0__negotiated_rate',
                    'negotiated_rates__0__negotiated_prices__0__expiration_date': f'negotiated_rates__{rate_idx}__negotiated_prices__0__expiration_date',
                    'negotiated_rates__0__negotiated_prices__0__service_code': f'negotiated_rates__{rate_idx}__negotiated_prices__0__service_code',
                    'negotiated_rates__0__negotiated_prices__0__billing_class': f'negotiated_rates__{rate_idx}__negotiated_prices__0__billing_class',
                    'negotiated_rates__1__negotiated_prices__0__billing_code_modifier': f'negotiated_rates__{rate_idx}__negotiated_prices__0__billing_code_modifier'
                }
                
                # Add the mapped columns
                for output_col, input_col in column_mapping.items():
                    if input_col in df.columns:
                        output_row[output_col] = row[input_col]
                    else:
                        output_row[output_col] = np.nan
                
                # Also check for billing_code_modifier in the current rate group
                modifier_col = f'negotiated_rates__{rate_idx}__negotiated_prices__0__billing_code_modifier'
                if modifier_col in df.columns:
                    output_row['negotiated_rates__1__negotiated_prices__0__billing_code_modifier'] = row[modifier_col]
                
                output_rows.append(output_row)
    
    # Create output dataframe
    result_df = pd.DataFrame(output_rows)
    
    # Ensure all required columns exist (add empty columns if missing)
    required_columns = [
        'name',
        'billing_code_type',
        'billing_code_type_version',
        'billing_code',
        'negotiated_rates__0__provider_groups__0__npi',
        'negotiated_rates__0__provider_groups__0__tin__type',
        'negotiated_rates__0__provider_groups__0__tin__value',
        'negotiated_rates__0__negotiated_prices__0__negotiated_type',
        'negotiated_rates__0__negotiated_prices__0__negotiated_rate',
        'negotiated_rates__0__negotiated_prices__0__expiration_date',
        'negotiated_rates__0__negotiated_prices__0__service_code',
        'negotiated_rates__0__negotiated_prices__0__billing_class',
        'negotiated_rates__1__negotiated_prices__0__billing_code_modifier'
    ]
    
    for col in required_columns:
        if col not in result_df.columns:
            result_df[col] = np.nan
    
    # Reorder columns to match desired output
    result_df = result_df[required_columns]
    
    # Remove rows where all negotiated data is empty
    negotiated_cols = required_columns[4:]  # All columns after the key columns
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
            transform_to_database_format(input_file, output_file)
                
        except Exception as e:
            print(f"Error processing {input_file}: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Single file processing
    # transform_to_database_format('your_file.csv', 'your_file_database_ready.csv')
    
    # Multiple files processing
    process_multiple_files(
        input_pattern='*.csv',  # Process all CSV files in current directory
        output_dir='database_ready_files'
    )
