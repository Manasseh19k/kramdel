import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import csv
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Any

class HealthcareDataValidator:
    def __init__(self, session: Session = None):
        """
        Initialize the Healthcare Data Validator for Snowpark environment
        
        Args:
            session: Snowflake Snowpark session (if None, will use current session context)
        """
        self.session = session
        self.validation_results = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Business key columns for healthcare data validation
        self.business_key_columns = [
            "MEMBER_ID", 
            "UMI_ID", 
            "GROUP_NUMBER", 
            "ELIGIBILITY_START_DATE"
        ]
        
        # Required columns for output reports
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
            
            # Check if required business key columns exist
            table_columns = [col['name'].upper() for col in columns]
            missing_business_keys = [key for key in self.business_key_columns if key.upper() not in table_columns]
            missing_output_cols = [col for col in self.output_columns if col.upper() not in table_columns]
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'columns': columns,
                'column_names': [col['name'] for col in columns],
                'missing_business_keys': missing_business_keys,
                'missing_output_columns': missing_output_cols,
                'has_all_business_keys': len(missing_business_keys) == 0,
                'has_all_output_columns': len(missing_output_cols) == 0
            }
            
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {str(e)}")
            return None
    
    def validate_business_requirements(self, table1_info: Dict, table2_info: Dict) -> List[Dict]:
        """
        Validate that both tables have required business key columns
        
        Args:
            table1_info: Information about PREM table
            table2_info: Information about OUTBOUND table
            
        Returns:
            List of validation issues
        """
        issues = []
        
        if not table1_info['has_all_business_keys']:
            issues.append({
                'type': 'missing_business_keys_prem',
                'table': table1_info['table_name'],
                'missing_columns': table1_info['missing_business_keys'],
                'severity': 'CRITICAL'
            })
        
        if not table2_info['has_all_business_keys']:
            issues.append({
                'type': 'missing_business_keys_outbound',
                'table': table2_info['table_name'],
                'missing_columns': table2_info['missing_business_keys'],
                'severity': 'CRITICAL'
            })
        
        if not table1_info['has_all_output_columns']:
            issues.append({
                'type': 'missing_output_columns_prem',
                'table': table1_info['table_name'],
                'missing_columns': table1_info['missing_output_columns'],
                'severity': 'WARNING'
            })
        
        if not table2_info['has_all_output_columns']:
            issues.append({
                'type': 'missing_output_columns_outbound',
                'table': table2_info['table_name'],
                'missing_columns': table2_info['missing_output_columns'],
                'severity': 'WARNING'
            })
        
        return issues
    
    def find_data_mismatches(self, prem_table: str, outbound_table: str) -> Tuple[List[Dict], List[Dict], List[Dict], Dict]:
        """
        Find data mismatches based on business key matching
        
        Args:
            prem_table: Name of PREM table
            outbound_table: Name of OUTBOUND table
            
        Returns:
            Tuple of (mismatched_rows, prem_only_rows, outbound_only_rows, summary_stats)
        """
        mismatched_rows = []
        prem_only_rows = []
        outbound_only_rows = []
        summary_stats = {}
        
        try:
            # Get table DataFrames
            prem_df = self.session.table(prem_table)
            outbound_df = self.session.table(outbound_table)
            
            # Get common columns between both tables
            prem_columns = [field.name.upper() for field in prem_df.schema.fields]
            outbound_columns = [field.name.upper() for field in outbound_df.schema.fields]
            common_columns = list(set(prem_columns) & set(outbound_columns))
            
            # Ensure business key columns are available
            available_business_keys = [key for key in self.business_key_columns if key.upper() in common_columns]
            if not available_business_keys:
                self.logger.error("No business key columns found in both tables")
                return mismatched_rows, prem_only_rows, outbound_only_rows, summary_stats
            
            # Create business key join condition
            join_conditions = []
            for key in available_business_keys:
                join_conditions.append(f"COALESCE(p.{key}, 'NULL') = COALESCE(o.{key}, 'NULL')")
            
            join_condition = " AND ".join(join_conditions)
            business_key_list = ", ".join(available_business_keys)
            
            # Create temporary views
            prem_df.create_or_replace_temp_view("temp_prem")
            outbound_df.create_or_replace_temp_view("temp_outbound")
            
            # 1. Find rows in PREM but not in OUTBOUND
            prem_only_sql = f"""
            SELECT {business_key_list}
            FROM temp_prem p
            LEFT JOIN temp_outbound o ON {join_condition}
            WHERE o.{available_business_keys[0]} IS NULL
            """
            
            prem_only_result = self.session.sql(prem_only_sql)
            prem_only_df = prem_only_result.to_pandas()
            
            for _, row in prem_only_df.iterrows():
                prem_only_rows.append(row.to_dict())
            
            # 2. Find rows in OUTBOUND but not in PREM
            outbound_only_sql = f"""
            SELECT {business_key_list}
            FROM temp_outbound o
            LEFT JOIN temp_prem p ON {join_condition}
            WHERE p.{available_business_keys[0]} IS NULL
            """
            
            outbound_only_result = self.session.sql(outbound_only_sql)
            outbound_only_df = outbound_only_result.to_pandas()
            
            for _, row in outbound_only_df.iterrows():
                outbound_only_rows.append(row.to_dict())
            
            # 3. Find matching rows with different values
            # Build column comparison conditions
            comparison_conditions = []
            select_columns = []
            
            # Add business key columns to select
            for key in available_business_keys:
                select_columns.append(f"p.{key}")
            
            # Add output columns to select if available
            available_output_cols = [col for col in self.output_columns if col.upper() in common_columns]
            for col in available_output_cols:
                if col not in available_business_keys:
                    select_columns.append(f"p.{col}")
            
            # Compare all common columns
            for col in common_columns:
                if col in available_business_keys:
                    continue  # Skip business key columns in comparison
                
                comparison_conditions.append(f"""
                    (COALESCE(CAST(p.{col} AS STRING), 'NULL') != COALESCE(CAST(o.{col} AS STRING), 'NULL'))
                """)
                select_columns.extend([f"p.{col} as PREM_{col}", f"o.{col} as OUTBOUND_{col}"])
            
            if comparison_conditions:
                mismatch_sql = f"""
                SELECT {', '.join(select_columns)}
                FROM temp_prem p
                INNER JOIN temp_outbound o ON {join_condition}
                WHERE {' OR '.join(comparison_conditions)}
                """
                
                mismatch_result = self.session.sql(mismatch_sql)
                mismatch_df = mismatch_result.to_pandas()
                
                # Process mismatched rows
                for _, row in mismatch_df.iterrows():
                    # Extract business key values
                    business_key_values = {}
                    for key in available_business_keys:
                        business_key_values[key] = row[key]
                    
                    # Extract output column values
                    output_values = {}
                    for col in available_output_cols:
                        output_values[col] = row[col] if col in row else None
                    
                    # Find which columns have differences
                    for col in common_columns:
                        if col in available_business_keys:
                            continue
                        
                        prem_val = row.get(f'PREM_{col}')
                        outbound_val = row.get(f'OUTBOUND_{col}')
                        
                        if pd.isna(prem_val) and pd.isna(outbound_val):
                            continue
                        elif pd.isna(prem_val) or pd.isna(outbound_val) or str(prem_val) != str(outbound_val):
                            mismatch_record = {
                                **output_values,  # Include all output columns
                                'NAME_OF_COLUMN_THAT_DOESN\'T_MATCH': col,
                                'PREM_TABLE_VALUE': prem_val if pd.notna(prem_val) else None,
                                'OUTBOUND_TABLE_VALUE': outbound_val if pd.notna(outbound_val) else None
                            }
                            mismatched_rows.append(mismatch_record)
            
            # Generate summary statistics
            summary_stats = {
                'prem_table': prem_table,
                'outbound_table': outbound_table,
                'total_prem_rows': prem_df.count(),
                'total_outbound_rows': outbound_df.count(),
                'rows_only_in_prem': len(prem_only_rows),
                'rows_only_in_outbound': len(outbound_only_rows),
                'mismatched_data_points': len(mismatched_rows),
                'business_keys_used': available_business_keys,
                'common_columns_compared': [col for col in common_columns if col not in available_business_keys]
            }
            
        except Exception as e:
            self.logger.error(f"Error finding data mismatches: {str(e)}")
            
        return mismatched_rows, prem_only_rows, outbound_only_rows, summary_stats
    
    def create_result_table(self, table_name: str, data: List[Dict], table_type: str):
        """
        Create Snowflake table with validation results
        
        Args:
            table_name: Name of the table to create
            data: List of dictionaries containing the data
            table_type: Type of table ('mismatch', 'prem_only', 'outbound_only')
        """
        if not data:
            self.logger.info(f"No data to create table {table_name}")
            return
        
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(data)
            
            # Create Snowpark DataFrame
            snowpark_df = self.session.create_dataframe(df)
            
            # Create table
            snowpark_df.write.mode("overwrite").save_as_table(table_name)
            
            self.logger.info(f"Created table {table_name} with {len(data)} rows")
            
        except Exception as e:
            self.logger.error(f"Error creating table {table_name}: {str(e)}")
    
    def write_to_csv(self, data: List[Dict], filename: str, write_local: bool = True):
        """
        Write data to CSV file
        
        Args:
            data: List of dictionaries to write
            filename: Name of the CSV file
            write_local: Whether to write to local filesystem
        """
        if not data:
            self.logger.info(f"No data to write to {filename}")
            return filename
        
        if not write_local:
            return filename
        
        try:
            # Convert to DataFrame for easier CSV writing
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False)
            
            self.logger.info(f"Data written to CSV: {filename} ({len(data)} rows)")
            
        except Exception as e:
            self.logger.error(f"Error writing to CSV {filename}: {str(e)}")
            
        return filename
    
    def write_to_stage(self, data: List[Dict], filename: str, stage_name: str = "@~/"):
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
            
            self.logger.info(f"Data written to stage: {stage_name}{filename}")
            
        except Exception as e:
            self.logger.error(f"Error writing to stage: {str(e)}")
    
    def validate_healthcare_data(self, prem_table: str, outbound_table: str, 
                               create_tables: bool = True, write_local_csv: bool = True, 
                               stage_name: str = "@~/") -> Dict[str, Any]:
        """
        Main validation function for healthcare data
        
        Args:
            prem_table: Name of PREM table
            outbound_table: Name of OUTBOUND table
            create_tables: Whether to create result tables in Snowflake
            write_local_csv: Whether to write local CSV files
            stage_name: Snowflake stage for CSV files
            
        Returns:
            Dictionary containing validation results
        """
        self.logger.info(f"Starting healthcare data validation between {prem_table} and {outbound_table}")
        
        # Get table information
        prem_info = self.get_table_info(prem_table)
        outbound_info = self.get_table_info(outbound_table)
        
        if not prem_info or not outbound_info:
            self.logger.error("Failed to get table information")
            return None
        
        # Validate business requirements
        business_validation_issues = self.validate_business_requirements(prem_info, outbound_info)
        
        # Check for critical issues
        critical_issues = [issue for issue in business_validation_issues if issue.get('severity') == 'CRITICAL']
        if critical_issues:
            self.logger.error("Critical validation issues found - cannot proceed with data comparison")
            return {
                'validation_status': 'FAILED',
                'critical_issues': critical_issues,
                'business_validation_issues': business_validation_issues
            }
        
        # Find data mismatches
        mismatched_rows, prem_only_rows, outbound_only_rows, summary_stats = self.find_data_mismatches(
            prem_table, outbound_table
        )
        
        # Generate output filenames
        mismatch_table_name = f"DATA_VALIDATION_MISMATCHES_{self.timestamp}"
        prem_only_table_name = f"DATA_VALIDATION_PREM_ONLY_{self.timestamp}"
        outbound_only_table_name = f"DATA_VALIDATION_OUTBOUND_ONLY_{self.timestamp}"
        
        mismatch_csv = f"data_mismatches_{self.timestamp}.csv"
        prem_only_csv = f"prem_only_rows_{self.timestamp}.csv"
        outbound_only_csv = f"outbound_only_rows_{self.timestamp}.csv"
        summary_csv = f"validation_summary_{self.timestamp}.csv"
        
        # Create Snowflake tables
        if create_tables:
            self.create_result_table(mismatch_table_name, mismatched_rows, 'mismatch')
            self.create_result_table(prem_only_table_name, prem_only_rows, 'prem_only')
            self.create_result_table(outbound_only_table_name, outbound_only_rows, 'outbound_only')
        
        # Write CSV files
        local_files = {}
        if write_local_csv:
            local_files['mismatches'] = self.write_to_csv(mismatched_rows, mismatch_csv)
            local_files['prem_only'] = self.write_to_csv(prem_only_rows, prem_only_csv)
            local_files['outbound_only'] = self.write_to_csv(outbound_only_rows, outbound_only_csv)
            local_files['summary'] = self.write_to_csv([summary_stats], summary_csv)
        
        # Write to Snowflake stage
        self.write_to_stage(mismatched_rows, mismatch_csv, stage_name)
        self.write_to_stage(prem_only_rows, prem_only_csv, stage_name)
        self.write_to_stage(outbound_only_rows, outbound_only_csv, stage_name)
        self.write_to_stage([summary_stats], summary_csv, stage_name)
        
        # Calculate validation status
        validation_passed = (
            len(mismatched_rows) == 0 and 
            len(prem_only_rows) == 0 and 
            len(outbound_only_rows) == 0 and
            len(business_validation_issues) == 0
        )
        
        # Prepare comprehensive results
        results = {
            'validation_status': 'PASSED' if validation_passed else 'FAILED',
            'timestamp': self.timestamp,
            'prem_table_info': prem_info,
            'outbound_table_info': outbound_info,
            'business_validation_issues': business_validation_issues,
            'summary_stats': summary_stats,
            'mismatched_rows_count': len(mismatched_rows),
            'prem_only_rows_count': len(prem_only_rows),
            'outbound_only_rows_count': len(outbound_only_rows),
            'created_tables': {
                'mismatches': mismatch_table_name if create_tables else None,
                'prem_only': prem_only_table_name if create_tables else None,
                'outbound_only': outbound_only_table_name if create_tables else None
            } if create_tables else None,
            'local_files': local_files if write_local_csv else None,
            'stage_files': {
                'mismatches': f"{stage_name}{mismatch_csv}",
                'prem_only': f"{stage_name}{prem_only_csv}",
                'outbound_only': f"{stage_name}{outbound_only_csv}",
                'summary': f"{stage_name}{summary_csv}"
            }
        }
        
        # Log summary
        self.logger.info(f"Healthcare data validation completed:")
        self.logger.info(f"  - Validation Status: {results['validation_status']}")
        self.logger.info(f"  - Data Mismatches: {len(mismatched_rows)}")
        self.logger.info(f"  - PREM Only Rows: {len(prem_only_rows)}")
        self.logger.info(f"  - OUTBOUND Only Rows: {len(outbound_only_rows)}")
        self.logger.info(f"  - Business Validation Issues: {len(business_validation_issues)}")
        
        return results


def main():
    """
    Main function to run healthcare data validation
    """
    # Table names - update these with your actual table names
    prem_table = 'PREM'
    outbound_table = 'OUTBOUND'
    
    # Configuration options
    create_tables = True  # Create result tables in Snowflake
    write_local_csv = True  # Write local CSV files
    stage_name = "@~/"  # Snowflake stage for files
    
    try:
        # Create validator instance
        validator = HealthcareDataValidator()
        
        # Run validation
        results = validator.validate_healthcare_data(
            prem_table, outbound_table, 
            create_tables, write_local_csv, stage_name
        )
        
        if results:
            print(f"\n{'='*60}")
            print(f"HEALTHCARE DATA VALIDATION RESULTS")
            print(f"{'='*60}")
            print(f"Timestamp: {results['timestamp']}")
            print(f"Validation Status: {results['validation_status']}")
            print(f"\nTable Information:")
            print(f"  PREM Table: {results['prem_table_info']['row_count']} rows")
            print(f"  OUTBOUND Table: {results['outbound_table_info']['row_count']} rows")
            
            print(f"\nValidation Results:")
            print(f"  Data Mismatches: {results['mismatched_rows_count']}")
            print(f"  Rows only in PREM: {results['prem_only_rows_count']}")
            print(f"  Rows only in OUTBOUND: {results['outbound_only_rows_count']}")
            
            if results['business_validation_issues']:
                print(f"\nBusiness Validation Issues:")
                for issue in results['business_validation_issues']:
                    print(f"  - {issue['type']}: {issue.get('missing_columns', 'N/A')} ({issue['severity']})")
            
            if results.get('created_tables'):
                print(f"\nCreated Tables:")
                for table_type, table_name in results['created_tables'].items():
                    if table_name:
                        print(f"  {table_type}: {table_name}")
            
            if results.get('local_files'):
                print(f"\nLocal CSV Files:")
                for file_type, filename in results['local_files'].items():
                    print(f"  {file_type}: {filename}")
            
            print(f"\nStage Files:")
            for file_type, filepath in results['stage_files'].items():
                print(f"  {file_type}: {filepath}")
            
            print(f"\nSummary Statistics:")
            summary = results['summary_stats']
            print(f"  Business Keys Used: {', '.join(summary.get('business_keys_used', []))}")
            print(f"  Columns Compared: {len(summary.get('common_columns_compared', []))}")
            
        return results
            
    except Exception as e:
        print(f"Error during healthcare data validation: {str(e)}")
        return None


# For use as Snowflake stored procedure
def validate_healthcare_data_sp(session: Session, prem_table: str, outbound_table: str, 
                              create_tables: bool = True, stage_name: str = "@~/"):
    """
    Stored procedure version of healthcare data validation
    
    Args:
        session: Snowpark session
        prem_table: Name of PREM table
        outbound_table: Name of OUTBOUND table
        create_tables: Whether to create result tables
        stage_name: Snowflake stage for output files
    
    Returns:
        Validation results summary as string
    """
    try:
        validator = HealthcareDataValidator(session)
        results = validator.validate_healthcare_data(
            prem_table, outbound_table, create_tables, False, stage_name
        )
        
        if results:
            return f"Healthcare Data Validation {results['validation_status']}: Mismatches={results['mismatched_rows_count']}, PREM Only={results['prem_only_rows_count']}, OUTBOUND Only={results['outbound_only_rows_count']}"
        else:
            return "Healthcare data validation failed to complete"
            
    except Exception as e:
        return f"Error during healthcare data validation: {str(e)}"


if __name__ == "__main__":
    main()
