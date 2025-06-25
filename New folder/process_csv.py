"""
CSV Reshaper - Transform wide format CSV to long format with conditional row creation
Specifically designed for medical billing data with multiple rate columns
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Union, Optional, Dict
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVReshaper:
    """Reshape CSV files from wide to long format with custom logic"""
    
    @staticmethod
    def reshape_billing_data(
        input_csv: Union[str, Path],
        output_csv: Union[str, Path],
        billing_code_column: str = 'Billing Code',
        column_keywords: List[str] = None,
        exclude_keywords: List[str] = None,
        institutional_trigger: str = 'Institutional',
        value_column_name: str = 'Rate',
        type_column_name: str = 'Rate Type',
        keep_columns: List[str] = None,
        chunk_size: int = 10000
    ) -> None:
        """
        Reshape billing data from wide to long format
        
        Args:
            input_csv: Path to input CSV file
            output_csv: Path to output CSV file
            billing_code_column: Name of the billing code column
            column_keywords: Keywords to identify columns to process (e.g., ['rate', 'price', 'amount'])
            exclude_keywords: Keywords to exclude columns
            institutional_trigger: Value that triggers new row creation
            value_column_name: Name for the melted value column
            type_column_name: Name for the column identifying the original column
            keep_columns: Additional columns to keep with each row
            chunk_size: Process in chunks for large files
        """
        input_csv = Path(input_csv)
        output_csv = Path(output_csv)
        
        # Default keywords if not provided
        if column_keywords is None:
            column_keywords = ['rate', 'price', 'negotiated', 'amount', 'charge']
        
        if exclude_keywords is None:
            exclude_keywords = []
        
        logger.info(f"Processing {input_csv}")
        
        # Process in chunks for memory efficiency
        first_chunk = True
        total_rows_written = 0
        
        for chunk_num, df_chunk in enumerate(pd.read_csv(input_csv, chunksize=chunk_size)):
            logger.info(f"Processing chunk {chunk_num + 1}")
            
            # Identify columns to process
            rate_columns = CSVReshaper._identify_rate_columns(
                df_chunk.columns,
                column_keywords,
                exclude_keywords,
                billing_code_column,
                keep_columns
            )
            
            if not rate_columns:
                logger.warning("No rate columns found matching keywords")
                continue
            
            # Process the chunk
            reshaped_df = CSVReshaper._process_chunk(
                df_chunk,
                billing_code_column,
                rate_columns,
                institutional_trigger,
                value_column_name,
                type_column_name,
                keep_columns
            )
            
            # Write to output
            if first_chunk:
                reshaped_df.to_csv(output_csv, index=False)
                first_chunk = False
            else:
                reshaped_df.to_csv(output_csv, mode='a', header=False, index=False)
            
            total_rows_written += len(reshaped_df)
            logger.info(f"Wrote {len(reshaped_df)} rows from chunk {chunk_num + 1}")
        
        logger.info(f"Completed: {total_rows_written} total rows written to {output_csv}")
    
    @staticmethod
    def _identify_rate_columns(
        all_columns: List[str],
        keywords: List[str],
        exclude_keywords: List[str],
        billing_code_column: str,
        keep_columns: Optional[List[str]]
    ) -> List[str]:
        """Identify columns that contain rate/price data based on keywords"""
        rate_columns = []
        
        # Columns to always exclude
        exclude_cols = {billing_code_column}
        if keep_columns:
            exclude_cols.update(keep_columns)
        
        for col in all_columns:
            if col in exclude_cols:
                continue
            
            col_lower = col.lower()
            
            # Check exclude keywords first
            if any(excl.lower() in col_lower for excl in exclude_keywords):
                continue
            
            # Check include keywords
            if any(kw.lower() in col_lower for kw in keywords):
                rate_columns.append(col)
        
        logger.info(f"Found {len(rate_columns)} rate columns to process")
        return rate_columns
    
    @staticmethod
    def _process_chunk(
        df: pd.DataFrame,
        billing_code_column: str,
        rate_columns: List[str],
        institutional_trigger: str,
        value_column_name: str,
        type_column_name: str,
        keep_columns: Optional[List[str]]
    ) -> pd.DataFrame:
        """Process a chunk of data"""
        result_rows = []
        
        # Identify base columns to keep
        id_vars = [billing_code_column]
        if keep_columns:
            id_vars.extend([col for col in keep_columns if col in df.columns])
        
        # Process each row
        for idx, row in df.iterrows():
            billing_code = row[billing_code_column]
            
            # Skip if no billing code
            if pd.isna(billing_code):
                continue
            
            # Base row data
            base_data = {col: row[col] for col in id_vars}
            
            # Process rate columns
            institutional_found = False
            institutional_columns = []
            regular_columns = []
            
            for col in rate_columns:
                value = row[col]
                
                # Check if this column contains institutional trigger
                if not pd.isna(value) and str(value).strip() == institutional_trigger:
                    institutional_found = True
                    institutional_columns.append(col)
                else:
                    regular_columns.append(col)
            
            # Create rows for regular columns
            for col in regular_columns:
                value = row[col]
                if not pd.isna(value) and str(value).strip() != '':
                    row_data = base_data.copy()
                    row_data[type_column_name] = col
                    row_data[value_column_name] = value
                    row_data['Row_Type'] = 'Regular'
                    result_rows.append(row_data)
            
            # If institutional found, create additional rows
            if institutional_found:
                # Find columns after institutional columns
                for inst_col in institutional_columns:
                    col_index = df.columns.get_loc(inst_col)
                    
                    # Look for subsequent columns with values
                    for next_col_idx in range(col_index + 1, len(df.columns)):
                        next_col = df.columns[next_col_idx]
                        
                        if next_col not in rate_columns:
                            continue
                        
                        next_value = row[next_col]
                        if not pd.isna(next_value) and str(next_value).strip() != '':
                            row_data = base_data.copy()
                            row_data[type_column_name] = f"{inst_col} -> {next_col}"
                            row_data[value_column_name] = next_value
                            row_data['Row_Type'] = 'Institutional'
                            result_rows.append(row_data)
        
        return pd.DataFrame(result_rows)
    
    @staticmethod
    def reshape_with_pattern_matching(
        input_csv: Union[str, Path],
        output_csv: Union[str, Path],
        billing_code_column: str = 'Billing Code',
        column_patterns: Dict[str, str] = None,
        institutional_handling: Dict[str, any] = None
    ) -> None:
        """
        Advanced reshaping with pattern matching for column identification
        
        Args:
            input_csv: Input CSV path
            output_csv: Output CSV path
            billing_code_column: Billing code column name
            column_patterns: Dict of pattern_name: regex_pattern
            institutional_handling: Dict with institutional processing rules
        """
        input_csv = Path(input_csv)
        output_csv = Path(output_csv)
        
        # Default patterns
        if column_patterns is None:
            column_patterns = {
                'negotiated_rate': r'.*negotiated.*rate.*',
                'allowed_amount': r'.*allowed.*amount.*',
                'charge_amount': r'.*charge.*',
                'institutional': r'.*institutional.*'
            }
        
        # Read first chunk to analyze structure
        df_sample = pd.read_csv(input_csv, nrows=1000)
        
        # Categorize columns
        column_categories = {}
        for col in df_sample.columns:
            for pattern_name, pattern in column_patterns.items():
                if re.match(pattern, col, re.IGNORECASE):
                    if pattern_name not in column_categories:
                        column_categories[pattern_name] = []
                    column_categories[pattern_name].append(col)
                    break
        
        logger.info("Column categories found:")
        for cat, cols in column_categories.items():
            logger.info(f"  {cat}: {len(cols)} columns")
        
        # Process file
        first_chunk = True
        for chunk in pd.read_csv(input_csv, chunksize=10000):
            reshaped = CSVReshaper._reshape_chunk_advanced(
                chunk,
                billing_code_column,
                column_categories,
                institutional_handling
            )
            
            if first_chunk:
                reshaped.to_csv(output_csv, index=False)
                first_chunk = False
            else:
                reshaped.to_csv(output_csv, mode='a', header=False, index=False)
    
    @staticmethod
    def _reshape_chunk_advanced(
        df: pd.DataFrame,
        billing_code_column: str,
        column_categories: Dict[str, List[str]],
        institutional_handling: Optional[Dict]
    ) -> pd.DataFrame:
        """Advanced chunk reshaping with category-based logic"""
        result_rows = []
        
        for idx, row in df.iterrows():
            billing_code = row[billing_code_column]
            
            if pd.isna(billing_code):
                continue
            
            # Process each category
            for category, columns in column_categories.items():
                if category == 'institutional' and institutional_handling:
                    # Special handling for institutional
                    for col in columns:
                        if str(row[col]).strip().lower() == 'institutional':
                            # Create institutional rows
                            CSVReshaper._create_institutional_rows(
                                row, billing_code, col, df.columns, result_rows
                            )
                else:
                    # Regular processing
                    for col in columns:
                        value = row[col]
                        if not pd.isna(value) and str(value).strip() != '':
                            result_rows.append({
                                billing_code_column: billing_code,
                                'Category': category,
                                'Original_Column': col,
                                'Value': value
                            })
        
        return pd.DataFrame(result_rows)
    
    @staticmethod
    def _create_institutional_rows(row, billing_code, inst_col, all_columns, result_rows):
        """Create rows for institutional data"""
        col_index = all_columns.get_loc(inst_col)
        
        # Look ahead for related columns
        for i in range(col_index + 1, min(col_index + 5, len(all_columns))):
            next_col = all_columns[i]
            value = row[next_col]
            
            if not pd.isna(value) and str(value).strip() != '':
                result_rows.append({
                    'Billing Code': billing_code,
                    'Category': 'institutional',
                    'Original_Column': f"{inst_col} -> {next_col}",
                    'Value': value,
                    'Institutional_Flag': True
                })


# Convenience functions
def reshape_billing_csv(input_csv: str, output_csv: str, **kwargs):
    """Reshape billing data CSV from wide to long format"""
    reshaper = CSVReshaper()
    reshaper.reshape_billing_data(input_csv, output_csv, **kwargs)

def analyze_csv_structure(csv_path: str, sample_rows: int = 10):
    """Analyze CSV structure to help identify columns"""
    df = pd.read_csv(csv_path, nrows=sample_rows)
    
    print(f"\nCSV Structure Analysis: {csv_path}")
    print(f"Shape: {df.shape}")
    print(f"\nColumns ({len(df.columns)} total):")
    
    # Group columns by keywords
    groups = {
        'billing': [],
        'rate': [],
        'amount': [],
        'price': [],
        'institutional': [],
        'other': []
    }
    
    for col in df.columns:
        col_lower = col.lower()
        categorized = False
        
        for keyword in ['billing', 'rate', 'amount', 'price', 'institutional']:
            if keyword in col_lower:
                groups[keyword].append(col)
                categorized = True
                break
        
        if not categorized:
            groups['other'].append(col)
    
    for group, cols in groups.items():
        if cols:
            print(f"\n{group.upper()} columns ({len(cols)}):")
            for col in cols[:5]:  # Show first 5
                print(f"  - {col}")
            if len(cols) > 5:
                print(f"  ... and {len(cols) - 5} more")

# Example usage
if __name__ == "__main__":
    print("CSV Reshaper - Wide to Long Format")
    print("\nExample usage:")
    
    examples = """
    # 1. Basic reshaping
    reshape_billing_csv(
        'wide_billing_data.csv',
        'long_billing_data.csv',
        billing_code_column='Billing Code',
        column_keywords=['rate', 'price', 'negotiated'],
        keep_columns=['Provider_Name', 'Service_Date']
    )
    
    # 2. With institutional handling
    reshape_billing_csv(
        'complex_billing.csv',
        'reshaped_billing.csv',
        billing_code_column='CPT_Code',
        column_keywords=['negotiated_rate', 'allowed_amount'],
        institutional_trigger='Institutional',
        value_column_name='Amount',
        type_column_name='Rate_Type'
    )
    
    # 3. First analyze structure
    analyze_csv_structure('your_file.csv')
    
    # 4. Advanced pattern matching
    from csv_reshaper import CSVReshaper
    
    reshaper = CSVReshaper()
    reshaper.reshape_with_pattern_matching(
        'input.csv',
        'output.csv',
        column_patterns={
            'in_network_rate': r'.*in.*network.*rate.*',
            'out_network_rate': r'.*out.*network.*rate.*',
            'institutional': r'.*institutional.*'
        }
    )
    """
    print(examples)
