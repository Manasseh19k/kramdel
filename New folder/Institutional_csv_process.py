import pandas as pd
import numpy as np
import glob
import os
import re

def process_csv_with_institutional_split(input_file, output_file, column_keywords, 
                                        maintain_columns, billing_code_col='billing_code'):
    """
    Process a CSV file to create separate rows for each section ending with 'institutional' value.
    
    Parameters:
    - input_file: Path to input CSV file
    - output_file: Path to output CSV file
    - column_keywords: List of keywords to look for in column names
    - maintain_columns: List of column names to maintain in all rows
    - billing_code_col: Name of the billing code column (default: 'billing_code')
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Ensure billing code column exists
    if billing_code_col not in df.columns:
        raise ValueError(f"'{billing_code_col}' column not found in the CSV file")
    
    # Ensure all maintain columns exist
    valid_maintain_columns = []
    for col in maintain_columns:
        if col in df.columns:
            valid_maintain_columns.append(col)
        else:
            print(f"Warning: '{col}' column not found in the CSV file")
    
    # Ensure billing code is in maintain columns
    if billing_code_col not in valid_maintain_columns:
        valid_maintain_columns.insert(0, billing_code_col)
    
    print(f"Maintaining columns: {valid_maintain_columns}")
    
    # Create a list to store all output rows
    all_output_rows = []
    
    # Process each row
    for idx, row in df.iterrows():
        # Find all columns containing 'institutional' value (case-insensitive)
        institutional_columns = []
        for col_idx, (col_name, value) in enumerate(row.items()):
            if pd.notna(value) and str(value).lower().strip() == 'institutional':
                institutional_columns.append((col_idx, col_name))
        
        if not institutional_columns:
            # No institutional values - create a single row with all keyword-matching columns
            output_row = {}
            
            # Add maintain columns
            for col in valid_maintain_columns:
                output_row[col] = row[col]
            
            # Add keyword-matching columns
            for col in df.columns:
                if col not in output_row and any(keyword.lower() in col.lower() for keyword in column_keywords):
                    output_row[col] = row[col]
            
            output_row['row_type'] = 'standard'
            all_output_rows.append(output_row)
        else:
            # Has institutional values - create separate rows for each section
            columns_list = list(df.columns)
            
            # Identify sections based on negotiated_rates indices
            sections = []
            for col_idx, col_name in institutional_columns:
                # Extract the negotiated_rates index from column name
                match = re.search(r'negotiated_rates__(\d+)__', col_name)
                if match:
                    rate_index = int(match.group(1))
                    
                    # Find all columns for this negotiated_rate section
                    section_columns = []
                    pattern = f'negotiated_rates__{rate_index}__'
                    
                    for c in columns_list:
                        if pattern in c:
                            section_columns.append(c)
                    
                    if section_columns:
                        sections.append({
                            'index': rate_index,
                            'columns': section_columns,
                            'institutional_col': col_name
                        })
            
            # Create a row for each section
            for section_idx, section in enumerate(sections):
                output_row = {}
                
                # Add maintain columns
                for col in valid_maintain_columns:
                    output_row[col] = row[col]
                
                # Add columns from this section that match keywords
                for col in section['columns']:
                    if any(keyword.lower() in col.lower() for keyword in column_keywords):
                        output_row[col] = row[col]
                
                # Ensure the institutional column is included
                output_row[section['institutional_col']] = row[section['institutional_col']]
                
                output_row['row_type'] = f'institutional_{section["index"]}'
                output_row['section_index'] = section['index']
                
                all_output_rows.append(output_row)
    
    # Create output dataframe
    result_df = pd.DataFrame(all_output_rows)
    
    # Reorder columns
    cols_order = valid_maintain_columns + ['row_type', 'section_index']
    other_cols = [col for col in result_df.columns if col not in cols_order]
    
    # Sort other columns to keep negotiated_rates sections together
    def sort_key(col):
        match = re.search(r'negotiated_rates__(\d+)__', col)
        if match:
            return (int(match.group(1)), col)
        return (999, col)
    
    other_cols_sorted = sorted(other_cols, key=sort_key)
    final_col_order = cols_order + other_cols_sorted
    
    # Only include columns that exist in the result
    final_col_order = [col for col in final_col_order if col in result_df.columns]
    result_df = result_df[final_col_order]
    
    # Save to CSV
    result_df.to_csv(output_file, index=False)
    print(f"Processed data saved to: {output_file}")
    print(f"Created {len(result_df)} rows from {len(df)} original rows")
    
    return result_df

def process_multiple_csv_files(input_pattern, output_dir, column_keywords, 
                              maintain_columns, billing_code_col='billing_code'):
    """
    Process multiple CSV files matching a pattern.
    
    Parameters:
    - input_pattern: Glob pattern for input files (e.g., '*.csv')
    - output_dir: Directory to save processed files
    - column_keywords: List of keywords to look for in column names
    - maintain_columns: List of column names to maintain in all rows
    - billing_code_col: Name of the billing code column
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
        output_file = os.path.join(output_dir, f"{name_without_ext}_processed.csv")
        
        print(f"\nProcessing: {input_file}")
        try:
            process_csv_with_institutional_split(
                input_file, 
                output_file, 
                column_keywords, 
                maintain_columns,
                billing_code_col
            )
        except Exception as e:
            print(f"Error processing {input_file}: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Define the keywords you want to look for in column names
    # Based on your data structure, these capture the key pricing/negotiation fields
    column_keywords = [
        'negotiated_rate',
        'negotiated_type',
        'negotiated_price',
        'billing_class',
        'service_code',
        'expiration_date',
        'provider_groups',
        'npi',
        'tin',
        'additional_information'
    ]
    
    # Define columns to maintain in all rows
    # These are the base columns that should appear in every output row
    maintain_columns = [
        'billing_code',
        'reporting_entity_name',
        'reporting_entity_type',
        'name',
        'description',
        'billing_code_type',
        'billing_code_type_version',
        'negotiation_arrangement',
        'last_updated_on',
        'version'
    ]
    
    # Single file processing example
    # process_csv_with_institutional_split(
    #     input_file='your_file.csv',
    #     output_file='your_file_processed.csv',
    #     column_keywords=column_keywords,
    #     maintain_columns=maintain_columns,
    #     billing_code_col='billing_code'
    # )
    
    # Multiple files processing
    process_multiple_csv_files(
        input_pattern='*.csv',  # Process all CSV files in current directory
        output_dir='processed_files',
        column_keywords=column_keywords,
        maintain_columns=maintain_columns,
        billing_code_col='billing_code'  # Note: lowercase based on your data
    )
