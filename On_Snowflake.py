import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import csv
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Any

class SnowflakeDataValidator:
    def __init__(self, session: Session = None):
        """
        Initialize the Snowflake Data Validator for Snowpark environment
        
        Args:
            session: Snowflake Snowpark session (if None, will use current session context)
        """
        self.session = session
        self.validation_results = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # If no session provided, try to get current session context
        if self.session is None:
            try:
                from snowflake.snowpark.context import get_active_session
                self.session = get_active_session()
                self.logger.info("Using active Snowpark session")
            except Exception as e:
                self.logger.error(f"No active session found: {str(e)}")
                raise Exception("No Snowpark session available. Please provide a session or run in Snowflake environment.")
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get table information including row count and column details
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table information
        """
        try:
            # Get the table as a Snowpark DataFrame
            df = self.session.table(table_name)
            
            # Get row count
            row_count = df.count()
            
            # Get column information from schema
            columns_info = df.schema.fields
            
            columns = []
            for field in columns_info:
                columns.append({
                    'name': field.name,
                    'type': str(field.datatype),
                    'nullable': field.nullable
                })
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'columns': columns,
                'column_names': [col['name'] for col in columns]
            }
            
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {str(e)}")
            return None
    
    def compare_table_structures(self, table1_info: Dict, table2_info: Dict) -> List[Dict]:
        """
        Compare table structures and return differences
        
        Args:
            table1_info: Information about first table
            table2_info: Information about second table
            
        Returns:
            List of structure differences
        """
        differences = []
        
        # Compare row counts
        if table1_info['row_count'] != table2_info['row_count']:
            differences.append({
                'type': 'row_count_mismatch',
                'table1': table1_info['table_name'],
                'table1_count': table1_info['row_count'],
                'table2': table2_info['table_name'],
                'table2_count': table2_info['row_count'],
                'difference': abs(table1_info['row_count'] - table2_info['row_count'])
            })
        
        # Compare column names
        table1_cols = set(table1_info['column_names'])
        table2_cols = set(table2_info['column_names'])
        
        missing_in_table2 = table1_cols - table2_cols
        missing_in_table1 = table2_cols - table1_cols
        
        if missing_in_table2:
            differences.append({
                'type': 'columns_missing_in_table2',
                'table1': table1_info['table_name'],
                'table2': table2_info['table_name'],
                'missing_columns': list(missing_in_table2)
            })
        
        if missing_in_table1:
            differences.append({
                'type': 'columns_missing_in_table1',
                'table1': table1_info['table_name'],
                'table2': table2_info['table_name'],
                'missing_columns': list(missing_in_table1)
            })
        
        # Compare column types for common columns
        common_columns = table1_cols & table2_cols
        table1_col_dict = {col['name']: col for col in table1_info['columns']}
        table2_col_dict = {col['name']: col for col in table2_info['columns']}
        
        for col_name in common_columns:
            if table1_col_dict[col_name]['type'] != table2_col_dict[col_name]['type']:
                differences.append({
                    'type': 'column_type_mismatch',
                    'table1': table1_info['table_name'],
                    'table2': table2_info['table_name'],
                    'column': col_name,
                    'table1_type': table1_col_dict[col_name]['type'],
                    'table2_type': table2_col_dict[col_name]['type']
                })
        
        return differences
    
    def compare_data_rows(self, table1_name: str, table2_name: str, 
                         key_columns: List[str] = None) -> Tuple[List[Dict], List[Dict]]:
        """
        Compare data rows between two tables using Snowpark
        
        Args:
            table1_name: Name of first table
            table2_name: Name of second table
            key_columns: List of columns to use as keys for comparison
            
        Returns:
            Tuple of (mismatched_rows, summary_stats)
        """
        mismatched_rows = []
        summary_stats = []
        
        try:
            # Get table DataFrames
            df1 = self.session.table(table1_name)
            df2 = self.session.table(table2_name)
            
            # Get common columns
            table1_info = self.get_table_info(table1_name)
            table2_info = self.get_table_info(table2_name)
            
            common_columns = list(set(table1_info['column_names']) & set(table2_info['column_names']))
            
            if not common_columns:
                self.logger.warning("No common columns found between tables")
                return mismatched_rows, summary_stats
            
            # If no key columns specified, try to use all common columns as keys
            if not key_columns:
                key_columns = common_columns
            else:
                # Validate key columns exist in both tables
                key_columns = [col for col in key_columns if col in common_columns]
            
            if not key_columns:
                self.logger.error("No valid key columns found for comparison")
                return mismatched_rows, summary_stats
            
            # Select only common columns from both tables
            df1_selected = df1.select([col(c) for c in common_columns])
            df2_selected = df2.select([col(c) for c in common_columns])
            
            # Add row hash for comparison (this helps identify different rows)
            # We'll use SQL to create a hash of all column values
            hash_cols_1 = [f"COALESCE(CAST({c} AS STRING), 'NULL')" for c in common_columns]
            hash_expr_1 = f"HASH({', '.join(hash_cols_1)})"
            
            hash_cols_2 = [f"COALESCE(CAST({c} AS STRING), 'NULL')" for c in common_columns]
            hash_expr_2 = f"HASH({', '.join(hash_cols_2)})"
            
            # Create temporary views for complex comparison
            df1_selected.create_or_replace_temp_view("temp_table1")
            df2_selected.create_or_replace_temp_view("temp_table2")
            
            # Find rows that exist in table1 but not in table2 (based on key columns)
            key_join_condition = " AND ".join([f"t1.{col} = t2.{col}" for col in key_columns])
            
            # SQL query to find mismatched rows
            comparison_sql = f"""
            SELECT 
                {', '.join([f't1.{col} as t1_{col}' for col in common_columns])},
                {', '.join([f't2.{col} as t2_{col}' for col in common_columns])},
                'MISMATCH' as status
            FROM temp_table1 t1
            FULL OUTER JOIN temp_table2 t2 ON {key_join_condition}
            WHERE 
                (t1.{key_columns[0]} IS NOT NULL AND t2.{key_columns[0]} IS NOT NULL 
                 AND ({hash_expr_1}) != ({hash_expr_2}))
                OR (t1.{key_columns[0]} IS NOT NULL AND t2.{key_columns[0]} IS NULL)
                OR (t1.{key_columns[0]} IS NULL AND t2.{key_columns[0]} IS NOT NULL)
            """
            
            # Execute comparison query
            comparison_result = self.session.sql(comparison_sql)
            comparison_df = comparison_result.to_pandas()
            
            # Process results
            for _, row in comparison_df.iterrows():
                # Identify which columns have differences
                differences = {}
                key_values = {}
                
                for col in common_columns:
                    t1_val = row.get(f't1_{col}')
                    t2_val = row.get(f't2_{col}')
                    
                    # Store key values
                    if col in key_columns:
                        key_values[col] = t1_val if pd.notna(t1_val) else t2_val
                    
                    # Check for differences
                    if pd.isna(t1_val) and pd.isna(t2_val):
                        continue  # Both null, no difference
                    elif pd.isna(t1_val) or pd.isna(t2_val) or t1_val != t2_val:
                        differences[col] = {
                            'table1_value': t1_val if pd.notna(t1_val) else None,
                            'table2_value': t2_val if pd.notna(t2_val) else None
                        }
                
                if differences:
                    mismatch_record = {
                        'key_values': key_values,
                        'differences': differences,
                        'table1': table1_name,
                        'table2': table2_name
                    }
                    mismatched_rows.append(mismatch_record)
            
            # Generate summary statistics
            summary_stats.append({
                'comparison_type': 'data_rows',
                'table1': table1_name,
                'table2': table2_name,
                'total_mismatched_rows': len(mismatched_rows),
                'common_columns': common_columns,
                'key_columns': key_columns
            })
            
        except Exception as e:
            self.logger.error(f"Error comparing data rows: {str(e)}")
            
        return mismatched_rows, summary_stats
    
    def write_to_snowflake_stage(self, data: List[Dict], filename: str, stage_name: str = "@~/"):
        """
        Write data to Snowflake stage as CSV
        
        Args:
            data: List of dictionaries to write
            filename: Name of the file
            stage_name: Snowflake stage name (default: user stage)
        """
        if not data:
            self.logger.info(f"No data to write to {filename}")
            return
        
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(data)
            
            # Create Snowpark DataFrame
            snowpark_df = self.session.create_dataframe(df)
            
            # Write to stage
            snowpark_df.write.mode("overwrite").option("header", True).csv(f"{stage_name}{filename}")
            
            self.logger.info(f"Data written to Snowflake stage: {stage_name}{filename}")
            
        except Exception as e:
            self.logger.error(f"Error writing to Snowflake stage: {str(e)}")
    
    def write_mismatched_rows_to_stage(self, mismatched_rows: List[Dict], filename: str = None, stage_name: str = "@~/"):
        """
        Write mismatched rows to Snowflake stage
        
        Args:
            mismatched_rows: List of mismatched row dictionaries
            filename: Output filename (optional)
            stage_name: Snowflake stage name
        """
        if not filename:
            filename = f"mismatched_rows_{self.timestamp}.csv"
        
        if not mismatched_rows:
            self.logger.info("No mismatched rows to write")
            return
        
        # Flatten the data for CSV format
        flattened_data = []
        for row in mismatched_rows:
            key_values_str = ", ".join([f"{k}={v}" for k, v in row['key_values'].items()])
            key_columns_str = ", ".join(row['key_values'].keys())
            
            for column, diff in row['differences'].items():
                flattened_data.append({
                    'table1': row['table1'],
                    'table2': row['table2'],
                    'key_columns': key_columns_str,
                    'key_values': key_values_str,
                    'column': column,
                    'table1_value': diff['table1_value'],
                    'table2_value': diff['table2_value']
                })
        
        self.write_to_snowflake_stage(flattened_data, filename, stage_name)
    
    def write_validation_log_to_stage(self, structure_differences: List[Dict], 
                                    summary_stats: List[Dict], filename: str = None, stage_name: str = "@~/"):
        """
        Write validation log to Snowflake stage
        
        Args:
            structure_differences: List of structure difference dictionaries
            summary_stats: List of summary statistics
            filename: Output filename (optional)
            stage_name: Snowflake stage name
        """
        if not filename:
            filename = f"validation_log_{self.timestamp}.csv"
        
        log_data = []
        
        # Add structure differences
        for diff in structure_differences:
            log_data.append({
                'timestamp': self.timestamp,
                'validation_type': diff['type'],
                'status': 'FAILED',
                'table1': diff.get('table1', ''),
                'table2': diff.get('table2', ''),
                'details': str(diff)
            })
        
        # Add summary statistics
        for stat in summary_stats:
            status = 'PASSED' if stat.get('total_mismatched_rows', 0) == 0 else 'FAILED'
            log_data.append({
                'timestamp': self.timestamp,
                'validation_type': stat['comparison_type'],
                'status': status,
                'table1': stat['table1'],
                'table2': stat['table2'],
                'details': str(stat)
            })
        
        self.write_to_snowflake_stage(log_data, filename, stage_name)
    
    def validate_tables(self, table1_name: str, table2_name: str, 
                       key_columns: List[str] = None, stage_name: str = "@~/") -> Dict[str, Any]:
        """
        Main validation function that performs all comparisons
        
        Args:
            table1_name: Name of first table (prem)
            table2_name: Name of second table (outbound)
            key_columns: List of columns to use as keys for row comparison
            stage_name: Snowflake stage name for output files
            
        Returns:
            Dictionary containing validation results
        """
        self.logger.info(f"Starting validation between {table1_name} and {table2_name}")
        
        # Get table information
        table1_info = self.get_table_info(table1_name)
        table2_info = self.get_table_info(table2_name)
        
        if not table1_info or not table2_info:
            self.logger.error("Failed to get table information")
            return None
        
        # Compare table structures
        structure_differences = self.compare_table_structures(table1_info, table2_info)
        
        # Compare data rows
        mismatched_rows, summary_stats = self.compare_data_rows(
            table1_name, table2_name, key_columns
        )
        
        # Write results to Snowflake stage
        self.write_mismatched_rows_to_stage(mismatched_rows, stage_name=stage_name)
        self.write_validation_log_to_stage(structure_differences, summary_stats, stage_name=stage_name)
        
        # Prepare results summary
        results = {
            'table1_info': table1_info,
            'table2_info': table2_info,
            'structure_differences': structure_differences,
            'mismatched_rows_count': len(mismatched_rows),
            'summary_stats': summary_stats,
            'validation_passed': len(structure_differences) == 0 and len(mismatched_rows) == 0
        }
        
        self.logger.info(f"Validation completed. Found {len(structure_differences)} structure differences and {len(mismatched_rows)} mismatched rows")
        
        return results


def main():
    """
    Main function to run the data validation in Snowflake environment
    """
    # Table names
    table1_name = 'PREM'  # Replace with your actual table name
    table2_name = 'OUTBOUND'  # Replace with your actual table name
    
    # Key columns for comparison (specify columns that uniquely identify rows)
    key_columns = ['ID']  # Replace with your actual key columns
    
    # Snowflake stage for output files (default is user stage)
    stage_name = "@~/"  # You can change this to a named stage if needed
    
    try:
        # Create validator instance (will use current Snowpark session)
        validator = SnowflakeDataValidator()
        
        # Run validation
        results = validator.validate_tables(table1_name, table2_name, key_columns, stage_name)
        
        if results:
            print(f"\nValidation Results:")
            print(f"Table 1 ({table1_name}): {results['table1_info']['row_count']} rows")
            print(f"Table 2 ({table2_name}): {results['table2_info']['row_count']} rows")
            print(f"Structure differences: {len(results['structure_differences'])}")
            print(f"Mismatched rows: {results['mismatched_rows_count']}")
            print(f"Validation passed: {results['validation_passed']}")
            print(f"Output files written to stage: {stage_name}")
            
            # Print structure differences
            if results['structure_differences']:
                print("\nStructure Differences:")
                for diff in results['structure_differences']:
                    print(f"  - {diff['type']}: {diff}")
        
        return results
            
    except Exception as e:
        print(f"Error during validation: {str(e)}")
        return None


# For use as Snowflake stored procedure
def validate_tables_sp(session: Session, table1_name: str, table2_name: str, 
                      key_columns_str: str = None, stage_name: str = "@~/"):
    """
    Stored procedure version of the validation function
    
    Args:
        session: Snowpark session
        table1_name: Name of first table
        table2_name: Name of second table
        key_columns_str: Comma-separated string of key column names
        stage_name: Snowflake stage for output files
    
    Returns:
        Validation results as string
    """
    try:
        # Parse key columns
        key_columns = None
        if key_columns_str:
            key_columns = [col.strip() for col in key_columns_str.split(',')]
        
        # Create validator
        validator = SnowflakeDataValidator(session)
        
        # Run validation
        results = validator.validate_tables(table1_name, table2_name, key_columns, stage_name)
        
        if results:
            return f"Validation completed. Structure differences: {len(results['structure_differences'])}, Mismatched rows: {results['mismatched_rows_count']}, Validation passed: {results['validation_passed']}"
        else:
            return "Validation failed to complete"
            
    except Exception as e:
        return f"Error during validation: {str(e)}"


if __name__ == "__main__":
    main()
