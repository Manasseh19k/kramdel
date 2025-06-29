import pandas as pd
import numpy as np
import glob
import os

def process_csv_with_institutional_split(input_file, output_file, column_keywords, 
                                        maintain_columns, billing_code_col='Billing Code'):
    """
    Process a CSV file to extract columns with certain keywords and create new rows
    when 'Institutional' value is found.
    
    Parameters:
    - input_file: Path to input CSV file
    - output_file: Path to output CSV file
    - column_keywords: List of keywords to look for in column names
    - maintain_columns: List of column names to maintain in both regular and institutional rows
    - billing_code_col: Name of the billing code column (default: 'Billing Code')
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Ensure billing code column exists
    if billing_code_col not in df.columns:
        raise ValueError(f"'{billing_code_col}' column not found in the CSV file")
    
    # Ensure all maintain columns exist
    for col in maintain_columns:
        if col not in df.columns:
            print(f"Warning: '{col}' column not found in the CSV file")
    
    # Get valid maintain columns (that actually exist in the dataframe)
    valid_maintain_columns = [col for col in maintain_columns if col in df.columns]
    
    # Ensure billing code is in maintain columns
    if billing_code_col not in valid_maintain_columns:
        valid_maintain_columns.insert(0, billing_code_col)
    
    # Find columns that contain any of the keywords (case-insensitive)
    keyword_matching_columns = []
    for col in df.columns:
        if any(keyword.lower() in col.lower() for keyword in column_keywords):
            keyword_matching_columns.append(col)
    
    print(f"Found {len(keyword_matching_columns)} columns matching keywords: {keyword_matching_columns}")
    print(f"Maintaining columns: {valid_maintain_columns}")
    
    # Create a list to store all rows (original and institutional)
    all_rows = []
    
    # Process each row
    for idx, row in df.iterrows():
        # Check if 'Institutional' appears in any column of this row
        institutional_found = False
        institutional_col_name = None
        institutional_col_index = None
        
        for col_idx, (col_name, value) in enumerate(row.items()):
            if pd.notna(value) and str(value).strip() == 'Institutional':
                institutional_found = True
                institutional_col_name = col_name
                institutional_col_index = list(df.columns).index(col_name)
                break
        
        if institutional_found:
            # Create regular row
            regular_row = {}
            
            # Add maintain columns
            for col in valid_maintain_columns:
                regular_row[col] = row[col]
            
            # Add keyword-matching columns up to and including the institutional column
            for col in keyword_matching_columns:
                col_index = list(df.columns).index(col)
                if col_index <= institutional_col_index:
                    regular_row[col] = row[col]
            
            # Always include the institutional column in the regular row
            if institutional_col_name not in regular_row:
                regular_row[institutional_col_name] = row[institutional_col_name]
            
            regular_row['Row_Type'] = 'Regular'
            all_rows.append(regular_row)
            
            # Create institutional row
            institutional_row = {}
            
            # Add maintain columns
            for col in valid_maintain_columns:
                institutional_row[col] = row[col]
            
            # Get keyword-matching columns after the 'Institutional' column
            columns_after_institutional = df.columns[institutional_col_index + 1:]
            for col in columns_after_institutional:
                if any(keyword.lower() in col.lower() for keyword in column_keywords):
                    institutional_row[col] = row[col]
            
            institutional_row['Row_Type'] = 'Institutional'
            all_rows.append(institutional_row)
        else:
            # Just add the regular row with all keyword-matching columns
            regular_row = {}
            
            # Add maintain columns
            for col in valid_maintain_columns:
                regular_row[col] = row[col]
            
            # Add all keyword-matching columns
            for col in keyword_matching_columns:
                if col not in regular_row:  # Avoid duplicates
                    regular_row[col] = row[col]
            
            regular_row['Row_Type'] = 'Regular'
            all_rows.append(regular_row)
    
    # Create output dataframe
    result_df = pd.DataFrame(all_rows)
    
    # Reorder columns: maintain columns first, then row type, then others
    cols_order = valid_maintain_columns + ['Row_Type']
    other_cols = [col for col in result_df.columns if col not in cols_order]
    final_col_order = cols_order + other_cols
    result_df = result_df[final_col_order]
    
    # Save to CSV
    result_df.to_csv(output_file, index=False)
    print(f"Processed data saved to: {output_file}")
    
    return result_df

def process_multiple_csv_files(input_pattern, output_dir, column_keywords, 
                              maintain_columns, billing_code_col='Billing Code'):
    """
    Process multiple CSV files matching a pattern.
    
    Parameters:
    - input_pattern: Glob pattern for input files (e.g., '*.csv')
    - output_dir: Directory to save processed files
    - column_keywords: List of keywords to look for in column names
    - maintain_columns: List of column names to maintain in both rows
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
    column_keywords = [
        'price',
        'rate',
        'charge',
        'cost',
        'fee',
        'amount',
        'negotiated',
        'allowed'
    ]
    
    # Define columns to maintain in both regular and institutional rows
    # These columns will appear in both rows when a split occurs
    maintain_columns = [
        'Billing Code',
        'Provider',
        'Service Description',
        'Effective Date'
        # Add other columns you want to keep in both rows
    ]
    
    # Single file processing
    # process_csv_with_institutional_split(
    #     input_file='example.csv',
    #     output_file='example_processed.csv',
    #     column_keywords=column_keywords,
    #     maintain_columns=maintain_columns,
    #     billing_code_col='Billing Code'
    # )
    
    # Multiple files processing
    process_multiple_csv_files(
        input_pattern='*.csv',  # Process all CSV files in current directory
        output_dir='processed_files',
        column_keywords=column_keywords,
        maintain_columns=maintain_columns,
        billing_code_col='Billing Code'  # Change this if your billing code column has a different name
    )
