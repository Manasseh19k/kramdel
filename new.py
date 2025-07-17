import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import csv
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Any

class DevProdMemberEnrollmentValidator:
    def __init__(self, session: Session = None):
        """
        Initialize the DEV vs PROD Member Enrollment Validator
        
        Args:
            session: Snowflake Snowpark session (if None, will use current session context)
        """
        self.session = session
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Table names
        self.dev_table = 'BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER'
        self.prod_table = 'BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER_PROD'
        
        # Comparison key columns
        self.comparison_columns = [
            "MEMBER_ID", 
            "UMI_ID", 
            "GROUP_NUMBER", 
            "ELIGIBILITY_START_DATE"
        ]
        
        # Output columns for mismatch reports
        self.output_columns = [
            "MEMBER_ID", 
            "LAST_NAME", 
            "FIRST_NAME", 
            "UMI_ID", 
            "GROUP_NUMBER", 
            "ELIGIBILITY_START_DATE"
        ]
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Get session context
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
        Get comprehensive table information
        
        Args:
            table_name: Fully qualified table name
            
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
            
            # Check for required columns
            table_columns = [col['name'].upper() for col in columns]
            missing_comparison_cols = [col for col in self.comparison_columns if col.upper() not in table_columns]
            missing_output_cols = [col for col in self.output_columns if col.upper() not in table_columns]
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'columns': columns,
                'column_names': [col['name'] for col in columns],
                'missing_comparison_columns': missing_comparison_cols,
                'missing_output_columns': missing_output_cols,
                'has_all_comparison_columns': len(missing_comparison_cols) == 0,
                'has_all_output_columns': len(missing_output_cols) == 0
            }
            
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {str(e)}")
            return None
    
    def validate_table_requirements(self, dev_info: Dict, prod_info: Dict) -> List[Dict]:
        """
        Validate that both tables have required columns
        
        Args:
            dev_info: Information about DEV table
            prod_info: Information about PROD table
            
        Returns:
            List of validation issues
        """
        issues = []
        
        if not dev_info['has_all_comparison_columns']:
            issues.append({
                'type': 'missing_comparison_columns_dev',
                'table': dev_info['table_name'],
                'missing_columns': dev_info['missing_comparison_columns'],
                'severity': 'CRITICAL'
            })
        
        if not prod_info['has_all_comparison_columns']:
            issues.append({
                'type': 'missing_comparison_columns_prod',
                'table': prod_info['table_name'],
                'missing_columns': prod_info['missing_comparison_columns'],
                'severity': 'CRITICAL'
            })
        
        if not dev_info['has_all_output_columns']:
            issues.append({
                'type': 'missing_output_columns_dev',
                'table': dev_info['table_name'],
                'missing_columns': dev_info['missing_output_columns'],
                'severity': 'WARNING'
            })
        
        if not prod_info['has_all_output_columns']:
            issues.append({
                'type': 'missing_output_columns_prod',
                'table': prod_info['table_name'],
                'missing_columns': prod_info['missing_output_columns'],
                'severity': 'WARNING'
            })
        
        return issues
    
    def compare_member_enrollment_data(self) -> Tuple[List[Dict], List[Dict], List[Dict], Dict]:
        """
        Compare DEV and PROD member enrollment data using MEMBER_ID as primary key
        
        Logic:
        1. Match records by MEMBER_ID only
        2. For matched records, compare UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE values
        3. Report any differences in these 3 columns as mismatches
        4. Report MEMBER_IDs that exist in one table but not the other
        
        Returns:
            Tuple of (mismatched_rows, dev_only_rows, prod_only_rows, summary_stats)
        """
        mismatched_rows = []
        dev_only_rows = []
        prod_only_rows = []
        summary_stats = {}
        
        try:
            # Get table DataFrames
            dev_df = self.session.table(self.dev_table)
            prod_df = self.session.table(self.prod_table)
            
            # Get common columns between both tables
            dev_columns = [field.name.upper() for field in dev_df.schema.fields]
            prod_columns = [field.name.upper() for field in prod_df.schema.fields]
            common_columns = list(set(dev_columns) & set(prod_columns))
            
            # Ensure comparison columns are available
            available_comparison_cols = [col for col in self.comparison_columns if col.upper() in common_columns]
            if len(available_comparison_cols) != len(self.comparison_columns):
                self.logger.error("Not all comparison columns found in both tables")
                missing = set(self.comparison_columns) - set(available_comparison_cols)
                self.logger.error(f"Missing columns: {missing}")
                return mismatched_rows, dev_only_rows, prod_only_rows, summary_stats
            
            # Create temporary views
            dev_df.create_or_replace_temp_view("temp_dev")
            prod_df.create_or_replace_temp_view("temp_prod")
            
            # 1. Find MEMBER_IDs in DEV but not in PROD
            self.logger.info("Finding MEMBER_IDs in DEV but not in PROD...")
            dev_only_sql = f"""
            SELECT d.MEMBER_ID, d.UMI_ID, d.GROUP_NUMBER, d.ELIGIBILITY_START_DATE
            FROM temp_dev d
            LEFT JOIN temp_prod p ON COALESCE(CAST(d.MEMBER_ID AS STRING), 'NULL') = COALESCE(CAST(p.MEMBER_ID AS STRING), 'NULL')
            WHERE p.MEMBER_ID IS NULL
            """
            
            dev_only_result = self.session.sql(dev_only_sql)
            dev_only_df = dev_only_result.to_pandas()
            
            for _, row in dev_only_df.iterrows():
                dev_only_rows.append(row.to_dict())
            
            # 2. Find MEMBER_IDs in PROD but not in DEV
            self.logger.info("Finding MEMBER_IDs in PROD but not in DEV...")
            prod_only_sql = f"""
            SELECT p.MEMBER_ID, p.UMI_ID, p.GROUP_NUMBER, p.ELIGIBILITY_START_DATE
            FROM temp_prod p
            LEFT JOIN temp_dev d ON COALESCE(CAST(p.MEMBER_ID AS STRING), 'NULL') = COALESCE(CAST(d.MEMBER_ID AS STRING), 'NULL')
            WHERE d.MEMBER_ID IS NULL
            """
            
            prod_only_result = self.session.sql(prod_only_sql)
            prod_only_df = prod_only_result.to_pandas()
            
            for _, row in prod_only_df.iterrows():
                prod_only_rows.append(row.to_dict())
            
            # 3. Find matching MEMBER_IDs with different values in comparison columns
            self.logger.info("Finding MEMBER_IDs with mismatched comparison column values...")
            
            # Get available output columns
            available_output_cols = [col for col in self.output_columns if col.upper() in common_columns]
            
            # Build select columns for mismatch query (include output columns from DEV table)
            select_columns = []
            for col in available_output_cols:
                select_columns.append(f"d.{col}")
            
            # Add comparison columns with both DEV and PROD values for comparison
            for col in ["UMI_ID", "GROUP_NUMBER", "ELIGIBILITY_START_DATE"]:  # Exclude MEMBER_ID since it's the join key
                select_columns.extend([f"d.{col} as DEV_{col}", f"p.{col} as PROD_{col}"])
            
            # Create comparison conditions for non-MEMBER_ID columns
            comparison_conditions = []
            for col in ["UMI_ID", "GROUP_NUMBER", "ELIGIBILITY_START_DATE"]:
                comparison_conditions.append(f"""
                    (COALESCE(CAST(d.{col} AS STRING), 'NULL') != COALESCE(CAST(p.{col} AS STRING), 'NULL'))
                """)
            
            # Build the mismatch query - join on MEMBER_ID only, compare other columns
            mismatch_sql = f"""
            SELECT {', '.join(select_columns)}
            FROM temp_dev d
            INNER JOIN temp_prod p ON 
                COALESCE(CAST(d.MEMBER_ID AS STRING), 'NULL') = COALESCE(CAST(p.MEMBER_ID AS STRING), 'NULL')
            WHERE {' OR '.join(comparison_conditions)}
            """
            
            mismatch_result = self.session.sql(mismatch_sql)
            mismatch_df = mismatch_result.to_pandas()
            
            # Process mismatched rows
            for _, row in mismatch_df.iterrows():
                # Extract output column values (from DEV table)
                output_values = {}
                for col in available_output_cols:
                    output_values[col] = row[col] if col in row else None
                
                # Find which comparison columns have differences (excluding MEMBER_ID)
                for col in ["UMI_ID", "GROUP_NUMBER", "ELIGIBILITY_START_DATE"]:
                    dev_val = row.get(f'DEV_{col}')
                    prod_val = row.get(f'PROD_{col}')
                    
                    # Check if values are different
                    if pd.isna(dev_val) and pd.isna(prod_val):
                        continue  # Both are null, no difference
                    elif pd.isna(dev_val) or pd.isna(prod_val) or str(dev_val) != str(prod_val):
                        # Create a separate record for each mismatched column
                        mismatch_record = {
                            **output_values,  # Include all output columns from DEV table
                            'NAME_OF_COLUMN(S)_THAT_HAVE_THE_MISMATCH': col,
                            'DEV_TABLE_VALUE': dev_val if pd.notna(dev_val) else None,
                            'PROD_TABLE_VALUE': prod_val if pd.notna(prod_val) else None
                        }
                        mismatched_rows.append(mismatch_record)
            
            # Generate summary statistics
            summary_stats = {
                'timestamp': self.timestamp,
                'dev_table': self.dev_table,
                'prod_table': self.prod_table,
                'total_dev_rows': dev_df.count(),
                'total_prod_rows': prod_df.count(),
                'rows_only_in_dev': len(dev_only_rows),
                'rows_only_in_prod': len(prod_only_rows),
                'mismatched_data_points': len(mismatched_rows),
                'comparison_columns': available_comparison_cols,
                'common_columns_count': len(common_columns),
                'validation_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            self.logger.info(f"Data comparison completed:")
            self.logger.info(f"  - Total DEV rows: {summary_stats['total_dev_rows']}")
            self.logger.info(f"  - Total PROD rows: {summary_stats['total_prod_rows']}")
            self.logger.info(f"  - DEV only rows: {len(dev_only_rows)}")
            self.logger.info(f"  - PROD only rows: {len(prod_only_rows)}")
            self.logger.info(f"  - Mismatched data points: {len(mismatched_rows)}")
            
        except Exception as e:
            self.logger.error(f"Error comparing member enrollment data: {str(e)}")
            raise
            
        return mismatched_rows, dev_only_rows, prod_only_rows, summary_stats
    
    def create_snowflake_table(self, table_name: str, data: List[Dict]):
        """
        Create Snowflake table with validation results
        
        Args:
            table_name: Name of the table to create
            data: List of dictionaries containing the data
        """
        if not data:
            self.logger.info(f"No data to create table {table_name}")
            return
        
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(data)
            
            # Clean column names (remove special characters for Snowflake)
            df.columns = [col.replace('(', '').replace(')', '').replace(' ', '_') for col in df.columns]
            
            # Create Snowpark DataFrame
            snowpark_df = self.session.create_dataframe(df)
            
            # Create table
            snowpark_df.write.mode("overwrite").save_as_table(table_name)
            
            self.logger.info(f"Created Snowflake table: {table_name} with {len(data)} rows")
            
        except Exception as e:
            self.logger.error(f"Error creating Snowflake table {table_name}: {str(e)}")
    
    def write_csv_file(self, data: List[Dict], filename: str):
        """
        Write data to CSV file
        
        Args:
            data: List of dictionaries to write
            filename: Name of the CSV file
        """
        if not data:
            self.logger.info(f"No data to write to {filename}")
            return
        
        try:
            # Convert to DataFrame for easier CSV writing
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False)
            
            self.logger.info(f"Created CSV file: {filename} with {len(data)} rows")
            
        except Exception as e:
            self.logger.error(f"Error writing CSV file {filename}: {str(e)}")
    
    def write_to_snowflake_stage(self, data: List[Dict], filename: str, stage_name: str = "@~/"):
        """
        Write data to Snowflake stage as CSV
        
        Args:
            data: List of dictionaries to write
            filename: Name of the file
            stage_name: Snowflake stage name
        """
        if not data:
            self.logger.info(f"No data to write to stage {filename}")
            return
        
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(data)
            
            # Create Snowpark DataFrame
            snowpark_df = self.session.create_dataframe(df)
            
            # Write to stage
            snowpark_df.write.mode("overwrite").option("header", True).csv(f"{stage_name}{filename}")
            
            self.logger.info(f"Created stage file: {stage_name}{filename}")
            
        except Exception as e:
            self.logger.error(f"Error writing to stage: {str(e)}")
    
    def validate_member_enrollment_tables(self, create_tables: bool = True, 
                                        write_local_csv: bool = True, 
                                        stage_name: str = "@~/") -> Dict[str, Any]:
        """
        Main validation function for DEV vs PROD member enrollment tables
        
        Args:
            create_tables: Whether to create result tables in Snowflake
            write_local_csv: Whether to write local CSV files
            stage_name: Snowflake stage for CSV files
            
        Returns:
            Dictionary containing comprehensive validation results
        """
        self.logger.info("="*80)
        self.logger.info("STARTING DEV vs PROD MEMBER ENROLLMENT VALIDATION")
        self.logger.info("="*80)
        self.logger.info(f"DEV Table: {self.dev_table}")
        self.logger.info(f"PROD Table: {self.prod_table}")
        self.logger.info(f"Timestamp: {self.timestamp}")
        
        # Get table information
        dev_info = self.get_table_info(self.dev_table)
        prod_info = self.get_table_info(self.prod_table)
        
        if not dev_info or not prod_info:
            self.logger.error("Failed to get table information")
            return None
        
        # Validate table requirements
        validation_issues = self.validate_table_requirements(dev_info, prod_info)
        
        # Check for critical issues
        critical_issues = [issue for issue in validation_issues if issue.get('severity') == 'CRITICAL']
        if critical_issues:
            self.logger.error("Critical validation issues found - cannot proceed with data comparison")
            return {
                'validation_status': 'FAILED',
                'critical_issues': critical_issues,
                'validation_issues': validation_issues
            }
        
        # Perform data comparison
        mismatched_rows, dev_only_rows, prod_only_rows, summary_stats = self.compare_member_enrollment_data()
        
        # Generate output names with timestamp
        mismatch_table_name = f"MEMBER_ENROLLMENT_MISMATCHES_{self.timestamp}"
        dev_only_table_name = f"MEMBER_ENROLLMENT_DEV_ONLY_{self.timestamp}"
        prod_only_table_name = f"MEMBER_ENROLLMENT_PROD_ONLY_{self.timestamp}"
        
        mismatch_csv = f"member_enrollment_mismatches_{self.timestamp}.csv"
        dev_only_csv = f"member_enrollment_dev_only_{self.timestamp}.csv"
        prod_only_csv = f"member_enrollment_prod_only_{self.timestamp}.csv"
        summary_csv = f"member_enrollment_validation_summary_{self.timestamp}.csv"
        
        # Create Snowflake tables
        created_tables = {}
        if create_tables:
            if mismatched_rows:
                self.create_snowflake_table(mismatch_table_name, mismatched_rows)
                created_tables['mismatches'] = mismatch_table_name
            
            if dev_only_rows:
                self.create_snowflake_table(dev_only_table_name, dev_only_rows)
                created_tables['dev_only'] = dev_only_table_name
            
            if prod_only_rows:
                self.create_snowflake_table(prod_only_table_name, prod_only_rows)
                created_tables['prod_only'] = prod_only_table_name
        
        # Write local CSV files
        local_files = {}
        if write_local_csv:
            if mismatched_rows:
                self.write_csv_file(mismatched_rows, mismatch_csv)
                local_files['mismatches'] = mismatch_csv
            
            if dev_only_rows:
                self.write_csv_file(dev_only_rows, dev_only_csv)
                local_files['dev_only'] = dev_only_csv
            
            if prod_only_rows:
                self.write_csv_file(prod_only_rows, prod_only_csv)
                local_files['prod_only'] = prod_only_csv
            
            # Always write summary
            self.write_csv_file([summary_stats], summary_csv)
            local_files['summary'] = summary_csv
        
        # Write to Snowflake stage
        stage_files = {}
        if mismatched_rows:
            self.write_to_snowflake_stage(mismatched_rows, mismatch_csv, stage_name)
            stage_files['mismatches'] = f"{stage_name}{mismatch_csv}"
        
        if dev_only_rows:
            self.write_to_snowflake_stage(dev_only_rows, dev_only_csv, stage_name)
            stage_files['dev_only'] = f"{stage_name}{dev_only_csv}"
        
        if prod_only_rows:
            self.write_to_snowflake_stage(prod_only_rows, prod_only_csv, stage_name)
            stage_files['prod_only'] = f"{stage_name}{prod_only_csv}"
        
        # Always write summary to stage
        self.write_to_snowflake_stage([summary_stats], summary_csv, stage_name)
        stage_files['summary'] = f"{stage_name}{summary_csv}"
        
        # Calculate overall validation status
        validation_passed = (
            len(mismatched_rows) == 0 and 
            len(dev_only_rows) == 0 and 
            len(prod_only_rows) == 0 and
            len(validation_issues) == 0
        )
        
        # Prepare comprehensive results
        results = {
            'validation_status': 'PASSED' if validation_passed else 'FAILED',
            'timestamp': self.timestamp,
            'dev_table_info': dev_info,
            'prod_table_info': prod_info,
            'validation_issues': validation_issues,
            'summary_statistics': summary_stats,
            'results_summary': {
                'mismatched_rows_count': len(mismatched_rows),
                'dev_only_rows_count': len(dev_only_rows),
                'prod_only_rows_count': len(prod_only_rows),
                'total_issues_found': len(mismatched_rows) + len(dev_only_rows) + len(prod_only_rows)
            },
            'created_tables': created_tables if create_tables else None,
            'local_files': local_files if write_local_csv else None,
            'stage_files': stage_files
        }
        
        return results
    
    def print_validation_report(self, results: Dict[str, Any]):
        """
        Print a comprehensive validation report
        
        Args:
            results: Results dictionary from validation
        """
        print("\n" + "="*80)
        print("DEV vs PROD MEMBER ENROLLMENT VALIDATION REPORT")
        print("="*80)
        print(f"Timestamp: {results['timestamp']}")
        print(f"Validation Status: {results['validation_status']}")
        
        print(f"\nTable Information:")
        print(f"  DEV Table: {results['dev_table_info']['table_name']}")
        print(f"    - Row Count: {results['dev_table_info']['row_count']:,}")
        print(f"  PROD Table: {results['prod_table_info']['table_name']}")
        print(f"    - Row Count: {results['prod_table_info']['row_count']:,}")
        
        summary = results['results_summary']
        print(f"\nValidation Results:")
        print(f"  Data Mismatches: {summary['mismatched_rows_count']:,}")
        print(f"  Rows only in DEV: {summary['dev_only_rows_count']:,}")
        print(f"  Rows only in PROD: {summary['prod_only_rows_count']:,}")
        print(f"  Total Issues Found: {summary['total_issues_found']:,}")
        
        if results.get('validation_issues'):
            print(f"\nValidation Issues:")
            for issue in results['validation_issues']:
                print(f"  - {issue['type']}: {issue.get('missing_columns', 'N/A')} ({issue['severity']})")
        
        if results.get('created_tables'):
            print(f"\nCreated Snowflake Tables:")
            for table_type, table_name in results['created_tables'].items():
                print(f"  {table_type}: {table_name}")
        
        if results.get('local_files'):
            print(f"\nLocal CSV Files:")
            for file_type, filename in results['local_files'].items():
                print(f"  {file_type}: {filename}")
        
        print(f"\nSnowflake Stage Files:")
        for file_type, filepath in results['stage_files'].items():
            print(f"  {file_type}: {filepath}")
        
        stats = results['summary_statistics']
        print(f"\nComparison Details:")
        print(f"  Comparison Columns: {', '.join(stats['comparison_columns'])}")
        print(f"  Common Columns Count: {stats['common_columns_count']}")
        print(f"  Validation Date: {stats['validation_date']}")


def main():
    """
    Main function to run DEV vs PROD member enrollment validation
    """
    try:
        # Configuration
        create_tables = True  # Create result tables in Snowflake
        write_local_csv = True  # Write local CSV files
        stage_name = "@~/"  # Snowflake stage for files
        
        # Create validator instance
        validator = DevProdMemberEnrollmentValidator()
        
        # Run validation
        results = validator.validate_member_enrollment_tables(
            create_tables=create_tables,
            write_local_csv=write_local_csv,
            stage_name=stage_name
        )
        
        if results:
            # Print comprehensive report
            validator.print_validation_report(results)
            
            # Return results for further processing if needed
            return results
        else:
            print("Validation failed to complete")
            return None
            
    except Exception as e:
        print(f"Error during validation: {str(e)}")
        logging.error(f"Validation error: {str(e)}")
        return None


# For use as Snowflake stored procedure
def validate_member_enrollment_sp(session: Session, create_tables: bool = True, 
                                stage_name: str = "@~/"):
    """
    Stored procedure version of member enrollment validation
    
    Args:
        session: Snowpark session
        create_tables: Whether to create result tables
        stage_name: Snowflake stage for output files
    
    Returns:
        Validation results summary as string
    """
    try:
        validator = DevProdMemberEnrollmentValidator(session)
        results = validator.validate_member_enrollment_tables(
            create_tables=create_tables, 
            write_local_csv=False,  # No local files in stored procedure
            stage_name=stage_name
        )
        
        if results:
            summary = results['results_summary']
            return f"Member Enrollment Validation {results['validation_status']}: Mismatches={summary['mismatched_rows_count']}, DEV Only={summary['dev_only_rows_count']}, PROD Only={summary['prod_only_rows_count']}"
        else:
            return "Member enrollment validation failed to complete"
            
    except Exception as e:
        return f"Error during validation: {str(e)}"


if __name__ == "__main__":
    main()
