"""
Practical Example: Reshaping Medical Billing CSV Data
Shows before/after transformation with institutional handling
"""

import pandas as pd
import numpy as np
from csv_reshaper import reshape_billing_csv, analyze_csv_structure

# Example 1: Create sample data to demonstrate the transformation
def create_sample_billing_data():
    """Create a sample billing CSV to demonstrate the reshaping"""
    
    # Sample wide format data
    data = {
        'Billing Code': ['99213', '99214', '99215', '99216'],
        'Provider_Name': ['Hospital A', 'Hospital A', 'Hospital B', 'Hospital B'],
        'Service_Type': ['Office Visit', 'Office Visit', 'Consultation', 'Consultation'],
        'In_Network_Rate_1': [150.00, 200.00, 250.00, 300.00],
        'In_Network_Rate_2': [145.00, 195.00, 245.00, 295.00],
        'Out_Network_Rate': [300.00, 400.00, 500.00, 600.00],
        'Special_Rate_Type': ['Standard', 'Institutional', 'Standard', 'Institutional'],
        'Special_Rate_Amount': [160.00, 210.00, 260.00, 310.00],
        'Institutional_Rate_1': [np.nan, 'Institutional', np.nan, 'Institutional'],
        'Institutional_Rate_2': [np.nan, 220.00, np.nan, 320.00],
        'Institutional_Rate_3': [np.nan, 225.00, np.nan, 325.00],
        'Medicare_Rate': [120.00, 160.00, 200.00, 240.00],
        'Medicaid_Rate': [100.00, 140.00, 180.00, 220.00]
    }
    
    df = pd.DataFrame(data)
    df.to_csv('sample_wide_billing.csv', index=False)
    print("Created sample_wide_billing.csv")
    print("\nSample of wide format data:")
    print(df.head())
    return df

# Example 2: Demonstrate the reshaping
def demonstrate_reshaping():
    """Show how the reshaping works"""
    
    print("\n" + "="*60)
    print("DEMONSTRATING CSV RESHAPING")
    print("="*60)
    
    # Create sample data
    create_sample_billing_data()
    
    # Analyze structure
    print("\n1. Analyzing CSV structure:")
    analyze_csv_structure('sample_wide_billing.csv')
    
    # Reshape the data
    print("\n2. Reshaping the data:")
    reshape_billing_csv(
        'sample_wide_billing.csv',
        'sample_long_billing.csv',
        billing_code_column='Billing Code',
        column_keywords=['rate', 'Rate'],  # Case variations
        exclude_keywords=['Type'],  # Exclude columns with 'Type' in name
        institutional_trigger='Institutional',
        value_column_name='Rate_Amount',
        type_column_name='Rate_Category',
        keep_columns=['Provider_Name', 'Service_Type']
    )
    
    # Show results
    print("\n3. Results of reshaping:")
    df_long = pd.read_csv('sample_long_billing.csv')
    print(f"Original shape: 4 rows × 13 columns")
    print(f"Reshaped shape: {df_long.shape[0]} rows × {df_long.shape[1]} columns")
    print("\nFirst 10 rows of reshaped data:")
    print(df_long.head(10))
    
    # Show institutional rows
    print("\n4. Institutional rows created:")
    inst_rows = df_long[df_long['Row_Type'] == 'Institutional']
    print(f"Found {len(inst_rows)} institutional rows")
    print(inst_rows.head())

# Example 3: Real-world usage for your case
def process_your_billing_file():
    """Example for processing your actual billing file"""
    
    print("\n" + "="*60)
    print("PROCESSING YOUR BILLING FILE")
    print("="*60)
    
    input_file = "your_extracted_data.csv"  # Your CSV from JSON extraction
    output_file = "reshaped_billing_data.csv"
    
    # Step 1: Analyze your file structure first
    print("\nStep 1: Analyzing your CSV structure")
    print("Run this first to understand your columns:")
    print("""
    analyze_csv_structure('your_extracted_data.csv', sample_rows=20)
    """)
    
    # Step 2: Based on the analysis, reshape with appropriate keywords
    print("\nStep 2: Reshape based on your column patterns")
    print("Example command based on common MRF column patterns:")
    
    example_code = """
    reshape_billing_csv(
        input_file,
        output_file,
        billing_code_column='billing_code',  # or 'Billing Code', 'CPT_Code', etc.
        column_keywords=[
            'negotiated_rate',
            'negotiated_price', 
            'allowed_amount',
            'billed_charge',
            'rate',
            'price'
        ],
        exclude_keywords=[
            'type',
            'modifier',
            'description'
        ],
        institutional_trigger='Institutional',
        value_column_name='Negotiated_Amount',
        type_column_name='Rate_Type',
        keep_columns=[
            'billing_code_type',
            'billing_code_type_version',
            'provider_reference',
            'npi'  # if available
        ]
    )
    """
    print(example_code)
    
    # Step 3: Handle special cases
    print("\nStep 3: For complex institutional handling:")
    complex_example = """
    from csv_reshaper import CSVReshaper
    
    # Define custom patterns for your specific column naming
    reshaper = CSVReshaper()
    reshaper.reshape_with_pattern_matching(
        input_file,
        output_file,
        billing_code_column='billing_code',
        column_patterns={
            'in_network': r'.*in.*network.*negotiated.*',
            'out_network': r'.*out.*network.*allowed.*',
            'institutional': r'.*institutional.*',
            'medicare': r'.*medicare.*rate.*',
            'medicaid': r'.*medicaid.*rate.*'
        },
        institutional_handling={
            'trigger_value': 'Institutional',
            'look_ahead_columns': 5,  # Check next 5 columns
            'create_separate_category': True
        }
    )
    """
    print(complex_example)

# Example 4: Post-processing the reshaped data
def post_process_reshaped_data():
    """Additional processing after reshaping"""
    
    print("\n" + "="*60)
    print("POST-PROCESSING RESHAPED DATA")
    print("="*60)
    
    # Read the reshaped data
    df = pd.read_csv('sample_long_billing.csv')
    
    # 1. Filter out zero or null rates
    print("\n1. Filtering out zero/null rates:")
    df_filtered = df[df['Rate_Amount'].notna() & (df['Rate_Amount'] > 0)]
    print(f"Rows before filtering: {len(df)}")
    print(f"Rows after filtering: {len(df_filtered)}")
    
    # 2. Add rate statistics per billing code
    print("\n2. Adding statistics per billing code:")
    stats = df_filtered.groupby('Billing Code')['Rate_Amount'].agg([
        'count', 'mean', 'min', 'max', 'std'
    ]).round(2)
    print(stats)
    
    # 3. Identify institutional vs regular rates
    print("\n3. Comparing institutional vs regular rates:")
    rate_comparison = df_filtered.groupby(['Billing Code', 'Row_Type'])['Rate_Amount'].agg([
        'count', 'mean'
    ]).round(2)
    print(rate_comparison)
    
    # 4. Export final processed data
    df_filtered.to_csv('final_processed_billing.csv', index=False)
    print("\n4. Exported final processed data to 'final_processed_billing.csv'")

# Run the examples
if __name__ == "__main__":
    print("Medical Billing CSV Reshaper - Examples")
    print("\nThis script demonstrates how to reshape wide-format billing data")
    print("into long format with special handling for institutional rates.")
    
    # Run demonstrations
    demonstrate_reshaping()
    
    print("\n" + "="*60)
    print("INSTRUCTIONS FOR YOUR USE CASE")
    print("="*60)
    
    instructions = """
    1. First, analyze your CSV structure:
       analyze_csv_structure('your_file.csv')
    
    2. Identify:
       - The exact name of your billing code column
       - Keywords that appear in rate/price columns
       - The value that indicates institutional rates
       - Any additional columns you want to keep
    
    3. Run the reshaper with your specific parameters:
       reshape_billing_csv(
           'your_input.csv',
           'your_output.csv',
           billing_code_column='your_billing_code_column_name',
           column_keywords=['keywords', 'in', 'your', 'rate', 'columns'],
           institutional_trigger='Institutional',  # or whatever indicates institutional
           keep_columns=['columns', 'to', 'preserve']
       )
    
    4. The output will have:
       - One row per rate value
       - Billing code repeated for each rate
       - Rate type/category to identify the source column
       - Special handling for institutional rates
       - Preserved columns from the original data
    """
    print(instructions)
    
    # Show sample output structure
    print("\nEXPECTED OUTPUT STRUCTURE:")
    print("-" * 60)
    print("Billing Code | Provider_Name | Rate_Category | Rate_Amount | Row_Type")
    print("99213       | Hospital A    | In_Network_1  | 150.00      | Regular")
    print("99213       | Hospital A    | In_Network_2  | 145.00      | Regular")
    print("99214       | Hospital A    | Inst_Rate_2   | 220.00      | Institutional")
    print("...")
    
    # Clean up sample files (optional)
    import os
    # os.remove('sample_wide_billing.csv')
    # os.remove('sample_long_billing.csv')
