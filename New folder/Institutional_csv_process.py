import pandas as pd
import numpy as np
import glob
import os

def process_csv_with_institutional_split(input_file, output_file, column_keywords, billing_code_col='Billing Code'):
    """
    Process a CSV file to extract columns with certain keywords and create new rows
    when 'Institutional' value is found.
    
    Parameters:
    - input_file: Path to input CSV file
    - output_file: Path to output CSV file
    - column_keywords: List of keywords to look for in column names
    - billing_code_col: Name of the billing code column (default: 'Billing Code')
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Ensure billing code column exists
    if billing_code_col not in df.columns:
        raise ValueError(f"'{billing_code_col}' column not found in the CSV file")
    
    # Find columns that contain any of the keywords (case-insensitive)
    matching_columns = [billing_code_col]  # Always include billing code
    for col in df.columns:
        if col != billing_code_col and any(keyword.lower() in col.lower() for keyword in column_keywords):
            matching_columns.append(col)
    
    print(f"Found {len(matching_columns)} matching columns: {matching_columns}")
    
    # Create a list to store all rows (original and institutional)
    all_rows = []
    
    # Process each row
    for idx, row in df.iterrows():
        # Create base row with matching columns
        base_row = {col: row[col] for col in matching_columns}
        
        # Check if 'Institutional' appears in any column of this row
        institutional_found = False
        institutional_col_index = None
        
        for col_idx, (col_name, value) in enumerate(row.items()):
            if pd.notna(value) and str(value).strip() == 'Institutional':
                institutional_found = True
                institutional_col_index = list(df.columns).index(col_name)
                break
        
        if institutional_found:
            # Add the regular row first
            regular_row = base_row.copy()
            all_rows.append(regular_row)
            
            # Create institutional row
            institutional_row = {billing_code_col: row[billing_code_col]}
            
            # Get columns after the 'Institutional' column that match our keywords
            columns_after_institutional = df.columns[institutional_col_index + 1:]
            for col in columns_after_institutional:
                if any(keyword.lower() in col.lower() for keyword in column_keywords):
                    institutional_row[col] = row[col]
            
            # Add row type identifier
            regular_row['Row_Type'] = 'Regular'
            institutional_row['Row_Type'] = 'Institutional'
            
            all_rows.append(institutional_row)
        else:
            # Just add the regular row
            base_row['Row_Type'] = 'Regular'
            all_rows.append(base_row)
    
    # Create output dataframe
    result_df = pd.DataFrame(all_rows)
    
    # Reorder columns to have billing code first, then row type
    cols = [billing_code_col, 'Row_Type'] + [col for col in result_df.columns if col not in [billing_code_col, 'Row_Type']]
    result_df = result_df[cols]
    
    # Save to CSV
    result_df.to_csv(output_file, index=False)
    print(f"Processed data saved to: {output_file}")
    
    return result_df

def process_multiple_csv_files(input_pattern, output_dir, column_keywords, billing_code_col='Billing Code'):
    """
    Process multiple CSV files matching a pattern.
    
    Parameters:
    - input_pattern: Glob pattern for input files (e.g., '*.csv')
    - output_dir: Directory to save processed files
    - column_keywords: List of keywords to look for in column names
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
            process_csv_with_institutional_split(input_file, output_file, column_keywords, billing_code_col)
        except Exception as e:
            print(f"Error processing {input_file}: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Define the keywords you want to look for in column names
    # Modify these based on your specific needs
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
    
    # Single file processing
    # process_csv_with_institutional_split(
    #     input_file='example.csv',
    #     output_file='example_processed.csv',
    #     column_keywords=column_keywords
    # )
    
    # Multiple files processing
    process_multiple_csv_files(
        input_pattern='*.csv',  # Process all CSV files in current directory
        output_dir='processed_files',
        column_keywords=column_keywords,
        billing_code_col='Billing Code'  # Change this if your billing code column has a different name
    )




# Mini-High

import pandas as pd

def explode_institutional(
    df: pd.DataFrame,
    billing_col: str,
    value_keyword: str,
    extra_column_keywords: list[str]
) -> pd.DataFrame:
    """
    For each row in df, find every column where the cell == value_keyword.
    For each such match, build a new row:
      - billing_col  → original billing code
      - "Type"       → the matched cell (i.e. value_keyword)
      - any column   → whose header contains any of extra_column_keywords
                        (pulled from that same input row)
    """
    records = []
    for _, row in df.iterrows():
        billing = row[billing_col]
        # find columns where the cell exactly matches "Institutional"
        inst_cols = [col for col in df.columns if row[col] == value_keyword]
        for inst_col in inst_cols:
            rec = {
                billing_col: billing,
                "Type": row[inst_col],
                "_Source_Column": inst_col
            }
            # pull in any other columns whose header contains your keywords
            for kw in extra_column_keywords:
                for c in df.columns:
                    if kw.lower() in c.lower():
                        rec[c] = row[c]
            records.append(rec)

    return pd.DataFrame.from_records(records)


if __name__ == "__main__":
    # 1) load
    df = pd.read_csv("your_extracted.csv")

    # 2) configure: 
    billing_col           = "Billing Code"
    value_keyword         = "Institutional"
    # tweak this list to match whatever other pieces of data you need
    extra_column_keywords = ["Amount", "Date", "Fee", "Status"]

    # 3) explode to long form
    exploded = explode_institutional(
        df,
        billing_col,
        value_keyword,
        extra_column_keywords
    )

    # 4) save
    exploded.to_csv("institutional_exploded.csv", index=False)
    print(f"Written {len(exploded)} rows to institutional_exploded.csv")
