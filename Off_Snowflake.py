import snowflake.connector
import pandas as pd
import csv
from datetime import datetime
import os
import logging
from typing import Dict, List, Tuple, Any

class SnowflakeDataValidator:
    def __init__(self, connection_params: Dict[str, str]):
        """
        Initialize the Snowflake Data Validator
        
        Args:
            connection_params: Dictionary containing Snowflake connection parameters
                - user: Snowflake username
                - password: Snowflake password
                - account: Snowflake account identifier
                - warehouse: Snowflake warehouse name
                - database: Snowflake database name
                - schema: Snowflake schema name
        """
        self.connection_params = connection_params
        self.connection = None
        self.validation_results = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(**self.connection_params)
            self.logger.info("Successfully connected to Snowflake")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {str(e)}")
            return False
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from Snowflake")
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get table information including row count and column details
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table information
        """
        cursor = self.connection.cursor()
        
        try:
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get column information
            cursor.execute(f"DESCRIBE TABLE {table_name}")
            columns_info = cursor.fetchall()
            
            columns = []
            for col_info in columns_info:
                columns.append({
                    'name': col_info[0],
                    'type': col_info[1],
                    'nullable': col_info[2] == 'Y',
                    'default': col_info[3],
                    'primary_key': col_info[4] == 'Y'
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
        finally:
            cursor.close()
    
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
    
    def get_data_sample(self, table_name: str, limit: int = 10000) -> pd.DataFrame:
        """
        Get data sample from table
        
        Args:
            table_name: Name of the table
            limit: Maximum number of rows to fetch
            
        Returns:
            pandas DataFrame with table data
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            df = pd.read_sql(query, self.connection)
            return df
        except Exception as e:
            self.logger.error(f"Error fetching data from {table_name}: {str(e)}")
            return pd.DataFrame()
    
    def compare_data_rows(self, table1_name: str, table2_name: str, 
                         key_columns: List[str] = None, 
                         batch_size: int = 10000) -> Tuple[List[Dict], List[Dict]]:
        """
        Compare data rows between two tables
        
        Args:
            table1_name: Name of first table
            table2_name: Name of second table
            key_columns: List of columns to use as keys for comparison
            batch_size: Number of rows to process at once
            
        Returns:
            Tuple of (mismatched_rows, summary_stats)
        """
        cursor = self.connection.cursor()
        mismatched_rows = []
        summary_stats = []
        
        try:
            # Get common columns
            table1_info = self.get_table_info(table1_name)
            table2_info = self.get_table_info(table2_name)
            
            common_columns = list(set(table1_info['column_names']) & set(table2_info['column_names']))
            
            if not common_columns:
                self.logger.warning("No common columns found between tables")
                return mismatched_rows, summary_stats
            
            # If no key columns specified, use all common columns
            if not key_columns:
                key_columns = common_columns
            else:
                # Validate key columns exist in both tables
                key_columns = [col for col in key_columns if col in common_columns]
            
            # Build comparison query
            select_cols = ", ".join([f"t1.{col} as t1_{col}, t2.{col} as t2_{col}" 
                                   for col in common_columns])
            
            join_condition = " AND ".join([f"t1.{col} = t2.{col}" for col in key_columns])
            
            # Find rows that exist in both tables but have different values
            comparison_query = f"""
            SELECT {select_cols}
            FROM {table1_name} t1
            FULL OUTER JOIN {table2_name} t2 ON {join_condition}
            WHERE t1.{key_columns[0]} IS NOT NULL AND t2.{key_columns[0]} IS NOT NULL
            """
            
            # Add conditions to check for differences in non-key columns
            non_key_columns = [col for col in common_columns if col not in key_columns]
            if non_key_columns:
                diff_conditions = []
                for col in non_key_columns:
                    diff_conditions.append(f"(t1.{col} != t2.{col} OR (t1.{col} IS NULL AND t2.{col} IS NOT NULL) OR (t1.{col} IS NOT NULL AND t2.{col} IS NULL))")
                
                comparison_query += " AND (" + " OR ".join(diff_conditions) + ")"
            
            cursor.execute(comparison_query)
            results = cursor.fetchall()
            
            # Process results
            column_names = [desc[0] for desc in cursor.description]
            
            for row in results:
                row_dict = dict(zip(column_names, row))
                
                # Identify which columns have differences
                differences = {}
                for col in common_columns:
                    t1_val = row_dict.get(f't1_{col}')
                    t2_val = row_dict.get(f't2_{col}')
                    
                    if t1_val != t2_val:
                        differences[col] = {
                            'table1_value': t1_val,
                            'table2_value': t2_val
                        }
                
                if differences:
                    mismatch_record = {
                        'key_values': {col: row_dict.get(f't1_{col}') for col in key_columns},
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
            
        finally:
            cursor.close()
        
        return mismatched_rows, summary_stats
    
    def write_mismatched_rows_to_csv(self, mismatched_rows: List[Dict], filename: str = None):
        """
        Write mismatched rows to CSV file
        
        Args:
            mismatched_rows: List of mismatched row dictionaries
            filename: Output filename (optional)
        """
        if not filename:
            filename = f"mismatched_rows_{self.timestamp}.csv"
        
        if not mismatched_rows:
            self.logger.info("No mismatched rows to write")
            return
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['table1', 'table2', 'key_columns', 'key_values', 'column', 'table1_value', 'table2_value']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                
                for row in mismatched_rows:
                    key_values_str = ", ".join([f"{k}={v}" for k, v in row['key_values'].items()])
                    key_columns_str = ", ".join(row['key_values'].keys())
                    
                    for column, diff in row['differences'].items():
                        writer.writerow({
                            'table1': row['table1'],
                            'table2': row['table2'],
                            'key_columns': key_columns_str,
                            'key_values': key_values_str,
                            'column': column,
                            'table1_value': diff['table1_value'],
                            'table2_value': diff['table2_value']
                        })
            
            self.logger.info(f"Mismatched rows written to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error writing mismatched rows to CSV: {str(e)}")
    
    def write_validation_log(self, structure_differences: List[Dict], 
                           summary_stats: List[Dict], filename: str = None):
        """
        Write validation log to CSV file
        
        Args:
            structure_differences: List of structure difference dictionaries
            summary_stats: List of summary statistics
            filename: Output filename (optional)
        """
        if not filename:
            filename = f"validation_log_{self.timestamp}.csv"
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['timestamp', 'validation_type', 'status', 'table1', 'table2', 'details']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                
                # Write structure differences
                for diff in structure_differences:
                    writer.writerow({
                        'timestamp': self.timestamp,
                        'validation_type': diff['type'],
                        'status': 'FAILED',
                        'table1': diff.get('table1', ''),
                        'table2': diff.get('table2', ''),
                        'details': str(diff)
                    })
                
                # Write summary statistics
                for stat in summary_stats:
                    status = 'PASSED' if stat.get('total_mismatched_rows', 0) == 0 else 'FAILED'
                    writer.writerow({
                        'timestamp': self.timestamp,
                        'validation_type': stat['comparison_type'],
                        'status': status,
                        'table1': stat['table1'],
                        'table2': stat['table2'],
                        'details': str(stat)
                    })
            
            self.logger.info(f"Validation log written to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error writing validation log: {str(e)}")
    
    def validate_tables(self, table1_name: str, table2_name: str, 
                       key_columns: List[str] = None) -> Dict[str, Any]:
        """
        Main validation function that performs all comparisons
        
        Args:
            table1_name: Name of first table (prem)
            table2_name: Name of second table (outbound)
            key_columns: List of columns to use as keys for row comparison
            
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
        
        # Write results to files
        self.write_mismatched_rows_to_csv(mismatched_rows)
        self.write_validation_log(structure_differences, summary_stats)
        
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
    Main function to run the data validation
    """
    # Snowflake connection parameters
    connection_params = {
        'user': 'your_username',
        'password': 'your_password',
        'account': 'your_account',
        'warehouse': 'your_warehouse',
        'database': 'your_database',
        'schema': 'your_schema'
    }
    
    # Table names
    table1_name = 'prem'  # Replace with your actual table name
    table2_name = 'outbound'  # Replace with your actual table name
    
    # Key columns for comparison (optional - specify columns that uniquely identify rows)
    key_columns = ['id']  # Replace with your actual key columns
    
    # Create validator instance
    validator = SnowflakeDataValidator(connection_params)
    
    try:
        # Connect to Snowflake
        if validator.connect():
            # Run validation
            results = validator.validate_tables(table1_name, table2_name, key_columns)
            
            if results:
                print(f"\nValidation Results:")
                print(f"Table 1 ({table1_name}): {results['table1_info']['row_count']} rows")
                print(f"Table 2 ({table2_name}): {results['table2_info']['row_count']} rows")
                print(f"Structure differences: {len(results['structure_differences'])}")
                print(f"Mismatched rows: {results['mismatched_rows_count']}")
                print(f"Validation passed: {results['validation_passed']}")
                
                # Print structure differences
                if results['structure_differences']:
                    print("\nStructure Differences:")
                    for diff in results['structure_differences']:
                        print(f"  - {diff['type']}: {diff}")
            
        else:
            print("Failed to connect to Snowflake")
            
    except Exception as e:
        print(f"Error during validation: {str(e)}")
        
    finally:
        validator.disconnect()


if __name__ == "__main__":
    main()
