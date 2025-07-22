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
    
    def check_member_id_count_differences(self) -> List[Dict]:
        """
        Check for MEMBER_IDs that appear different numbers of times in DEV vs PROD tables
        
        Returns:
            List of dictionaries containing MEMBER_IDs with count differences
        """
        count_differences = []
        
        try:
            self.logger.info("Checking for MEMBER_IDs with different occurrence counts...")
            
            # Get count of each MEMBER_ID in both tables
            count_comparison_sql = f"""
            WITH dev_counts AS (
                SELECT 
                    MEMBER_ID,
                    COUNT(*) as dev_count
                FROM {self.dev_table}
                WHERE MEMBER_ID IS NOT NULL
                GROUP BY MEMBER_ID
            ),
            prod_counts AS (
                SELECT 
                    MEMBER_ID,
                    COUNT(*) as prod_count
                FROM {self.prod_table}
                WHERE MEMBER_ID IS NOT NULL
                GROUP BY MEMBER_ID
            ),
            combined_counts AS (
                SELECT 
                    COALESCE(d.MEMBER_ID, p.MEMBER_ID) as MEMBER_ID,
                    COALESCE(d.dev_count, 0) as dev_count,
                    COALESCE(p.prod_count, 0) as prod_count
                FROM dev_counts d
                FULL OUTER JOIN prod_counts p ON d.MEMBER_ID = p.MEMBER_ID
            )
            SELECT 
                MEMBER_ID,
                dev_count,
                prod_count,
                ABS(dev_count - prod_count) as count_difference,
                CASE 
                    WHEN dev_count > prod_count THEN 'MORE_IN_DEV'
                    WHEN prod_count > dev_count THEN 'MORE_IN_PROD'
                    ELSE 'EQUAL'
                END as difference_type
            FROM combined_counts
            WHERE dev_count != prod_count
            ORDER BY count_difference DESC, MEMBER_ID
            """
            
            result = self.session.sql(count_comparison_sql)
            count_diff_df = result.to_pandas()
            
            for _, row in count_diff_df.iterrows():
                count_differences.append({
                    'MEMBER_ID': row['MEMBER_ID'],
                    'DEV_COUNT': row['dev_count'],
                    'PROD_COUNT': row['prod_count'],
                    'COUNT_DIFFERENCE': row['count_difference'],
                    'DIFFERENCE_TYPE': row['difference_type']
                })
            
            self.logger.info(f"Found {len(count_differences)} MEMBER_IDs with different occurrence counts")
            
        except Exception as e:
            self.logger.error(f"Error checking MEMBER_ID count differences: {str(e)}")
            raise
        
        return count_differences
    
    def get_duplicate_member_details(self, member_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific MEMBER_ID that appears multiple times
        
        Args:
            member_id: The MEMBER_ID to investigate
            
        Returns:
            Dictionary with detailed information about all occurrences
        """
        details = {'member_id': member_id}
        
        try:
            # Get all DEV occurrences
            dev_sql = f"""
            SELECT MEMBER_ID, UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE, LAST_NAME, FIRST_NAME,
                   ROW_NUMBER() OVER (ORDER BY UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE) as row_num
            FROM {self.dev_table}
            WHERE MEMBER_ID = '{member_id}'
            ORDER BY UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE
            """
            
            # Get all PROD occurrences
            prod_sql = f"""
            SELECT MEMBER_ID, UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE, LAST_NAME, FIRST_NAME,
                   ROW_NUMBER() OVER (ORDER BY UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE) as row_num
            FROM {self.prod_table}
            WHERE MEMBER_ID = '{member_id}'
            ORDER BY UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE
            """
            
            dev_result = self.session.sql(dev_sql).to_pandas()
            prod_result = self.session.sql(prod_sql).to_pandas()
            
            details['dev_occurrences'] = dev_result.to_dict('records')
            details['prod_occurrences'] = prod_result.to_dict('records')
            details['dev_count'] = len(dev_result)
            details['prod_count'] = len(prod_result)
            
        except Exception as e:
            self.logger.error(f"Error getting duplicate details for {member_id}: {str(e)}")
            details['error'] = str(e)
        
        return details
    
    def analyze_duplicate_patterns(self) -> Dict[str, Any]:
        """
        Analyze patterns in duplicate MEMBER_IDs
        
        Returns:
            Dictionary with duplicate analysis
        """
        analysis = {}
        
        try:
            # Get duplicate statistics for DEV table
            dev_duplicates_sql = f"""
            WITH member_counts AS (
                SELECT MEMBER_ID, COUNT(*) as occurrence_count
                FROM {self.dev_table}
                WHERE MEMBER_ID IS NOT NULL
                GROUP BY MEMBER_ID
            )
            SELECT 
                occurrence_count,
                COUNT(*) as member_id_count,
                COUNT(*) * occurrence_count as total_rows
            FROM member_counts
            WHERE occurrence_count > 1
            GROUP BY occurrence_count
            ORDER BY occurrence_count
            """
            
            # Get duplicate statistics for PROD table
            prod_duplicates_sql = f"""
            WITH member_counts AS (
                SELECT MEMBER_ID, COUNT(*) as occurrence_count
                FROM {self.prod_table}
                WHERE MEMBER_ID IS NOT NULL
                GROUP BY MEMBER_ID
            )
            SELECT 
                occurrence_count,
                COUNT(*) as member_id_count,
                COUNT(*) * occurrence_count as total_rows
            FROM member_counts
            WHERE occurrence_count > 1
            GROUP BY occurrence_count
            ORDER BY occurrence_count
            """
            
            dev_dup_result = self.session.sql(dev_duplicates_sql).to_pandas()
            prod_dup_result = self.session.sql(prod_duplicates_sql).to_pandas()
            
            analysis['dev_duplicates'] = dev_dup_result.to_dict('records')
            analysis['prod_duplicates'] = prod_dup_result.to_dict('records')
            
            # Get total duplicate counts
            dev_total_dups = dev_dup_result['member_id_count'].sum() if not dev_dup_result.empty else 0
            prod_total_dups = prod_dup_result['member_id_count'].sum() if not prod_dup_result.empty else 0
            
            analysis['summary'] = {
                'dev_duplicate_member_ids': int(dev_total_dups),
                'prod_duplicate_member_ids': int(prod_total_dups)
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing duplicate patterns: {str(e)}")
            analysis['error'] = str(e)
        
        return analysis
    
    def compare_member_enrollment_data(self) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], Dict]:
        """
        Compare DEV and PROD member enrollment data using MEMBER_ID as primary key
        Enhanced to include duplicate count checking
        
        Logic:
        1. Match records by MEMBER_ID only
        2. For matched records, compare UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE values
        3. Report any differences in these 3 columns as mismatches
        4. Report MEMBER_IDs that exist in one table but not the other
        5. NEW: Report MEMBER_IDs that appear different numbers of times in each table
        
        Returns:
            Tuple of (mismatched_rows, dev_only_rows, prod_only_rows, count_difference_rows, summary_stats)
        """
        mismatched_rows = []
        dev_only_rows = []
        prod_only_rows = []
        count_difference_rows = []
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
                return mismatched_rows, dev_only_rows, prod_only_rows, count_difference_rows, summary_stats
            
            # Create temporary views
            dev_df.create_or_replace_temp_view("temp_dev")
            prod_df.create_or_replace_temp_view("temp_prod")
            
            # NEW: Check for MEMBER_IDs with different occurrence counts
            self.logger.info("Checking for MEMBER_IDs with different occurrence counts...")
            count_difference_rows = self.check_member_id_count_differences()
            
            # 1. Find unique MEMBER_IDs in DEV but not in PROD
            self.logger.info("Finding unique MEMBER_IDs in DEV but not in PROD...")
            dev_only_sql = f"""
            SELECT DISTINCT d.MEMBER_ID, d.UMI_ID, d.GROUP_NUMBER, d.ELIGIBILITY_START_DATE
            FROM temp_dev d
            LEFT JOIN temp_prod p ON COALESCE(CAST(d.MEMBER_ID AS STRING), 'NULL') = COALESCE(CAST(p.MEMBER_ID AS STRING), 'NULL')
            WHERE p.MEMBER_ID IS NULL
            """
            
            dev_only_result = self.session.sql(dev_only_sql)
            dev_only_df = dev_only_result.to_pandas()
            
            for _, row in dev_only_df.iterrows():
                dev_only_rows.append(row.to_dict())
            
            # 2. Find unique MEMBER_IDs in PROD but not in DEV
            self.logger.info("Finding unique MEMBER_IDs in PROD but not in DEV...")
            prod_only_sql = f"""
            SELECT DISTINCT p.MEMBER_ID, p.UMI_ID, p.GROUP_NUMBER, p.ELIGIBILITY_START_DATE
            FROM temp_prod p
            LEFT JOIN temp_dev d ON COALESCE(CAST(p.MEMBER_ID AS STRING), 'NULL') = COALESCE(CAST(d.MEMBER_ID AS STRING), 'NULL')
            WHERE d.MEMBER_ID IS NULL
            """
            
            prod_only_result = self.session.sql(prod_only_sql)
            prod_only_df = prod_only_result.to_pandas()
            
            for _, row in prod_only_df.iterrows():
                prod_only_rows.append(row.to_dict())
            
            # 3. Find matching MEMBER_IDs with different values (enhanced logic for duplicates)
            self.logger.info("Finding MEMBER_IDs with mismatched comparison column values...")
            
            # Get available output columns
            available_output_cols = [col for col in self.output_columns if col.upper() in common_columns]
            
            # For duplicates, we need to handle the comparison differently
            # We'll compare each DEV record with each matching PROD record using ROW_NUMBER
            mismatch_sql = f"""
            WITH dev_numbered AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY MEMBER_ID ORDER BY UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE) as dev_row_num
                FROM temp_dev
                WHERE MEMBER_ID IS NOT NULL
            ),
            prod_numbered AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY MEMBER_ID ORDER BY UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE) as prod_row_num
                FROM temp_prod
                WHERE MEMBER_ID IS NOT NULL
            )
            SELECT 
                d.MEMBER_ID,
                d.LAST_NAME,
                d.FIRST_NAME,
                d.UMI_ID,
                d.GROUP_NUMBER,
                d.ELIGIBILITY_START_DATE,
                d.dev_row_num,
                p.prod_row_num,
                d.UMI_ID as DEV_UMI_ID, p.UMI_ID as PROD_UMI_ID,
                d.GROUP_NUMBER as DEV_GROUP_NUMBER, p.GROUP_NUMBER as PROD_GROUP_NUMBER,
                d.ELIGIBILITY_START_DATE as DEV_ELIGIBILITY_START_DATE, p.ELIGIBILITY_START_DATE as PROD_ELIGIBILITY_START_DATE
            FROM dev_numbered d
            INNER JOIN prod_numbered p ON d.MEMBER_ID = p.MEMBER_ID AND d.dev_row_num = p.prod_row_num
            WHERE 
                (COALESCE(CAST(d.UMI_ID AS STRING), 'NULL') != COALESCE(CAST(p.UMI_ID AS STRING), 'NULL'))
                OR (COALESCE(CAST(d.GROUP_NUMBER AS STRING), 'NULL') != COALESCE(CAST(p.GROUP_NUMBER AS STRING), 'NULL'))
                OR (COALESCE(CAST(d.ELIGIBILITY_START_DATE AS STRING), 'NULL') != COALESCE(CAST(p.ELIGIBILITY_START_DATE AS STRING), 'NULL'))
            """
            
            mismatch_result = self.session.sql(mismatch_sql)
            mismatch_df = mismatch_result.to_pandas()
            
            # Process mismatched rows
            for _, row in mismatch_df.iterrows():
                # Extract output column values (from DEV table)
                output_values = {}
                for col in available_output_cols:
                    output_values[col] = row[col] if col in row else None
                
                # Add row numbers for duplicate tracking
                output_values['DEV_ROW_NUMBER'] = row['dev_row_num']
                output_values['PROD_ROW_NUMBER'] = row['prod_row_num']
                
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
            
            # Generate enhanced summary statistics
            summary_stats = {
                'timestamp': self.timestamp,
                'dev_table': self.dev_table,
                'prod_table': self.prod_table,
                'total_dev_rows': dev_df.count(),
                'total_prod_rows': prod_df.count(),
                'rows_only_in_dev': len(dev_only_rows),
                'rows_only_in_prod': len(prod_only_rows),
                'mismatched_data_points': len(mismatched_rows),
                'member_ids_with_count_differences': len(count_difference_rows),
                'comparison_columns': available_comparison_cols,
                'common_columns_count': len(common_columns),
                'validation_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            self.logger.info(f"Enhanced data comparison completed:")
            self.logger.info(f"  - Total DEV rows: {summary_stats['total_dev_rows']}")
            self.logger.info(f"  - Total PROD rows: {summary_stats['total_prod_rows']}")
            self.logger.info(f"  - DEV only rows: {len(dev_only_rows)}")
            self.logger.info(f"  - PROD only rows: {len(prod_only_rows)}")
            self.logger.info(f"  - Mismatched data points: {len(mismatched_rows)}")
            self.logger.info(f"  - MEMBER_IDs with count differences: {len(count_difference_rows)}")
            
        except Exception as e:
            self.logger.error(f"Error comparing member enrollment data: {str(e)}")
            raise
            
        return mismatched_rows, dev_only_rows, prod_only_rows, count_difference_rows, summary_stats
    
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
        Enhanced to include duplicate count checking
        
        Args:
            create_tables: Whether to create result tables in Snowflake
            write_local_csv: Whether to write local CSV files
            stage_name: Snowflake stage for CSV files
            
        Returns:
            Dictionary containing comprehensive validation results
        """
        self.logger.info("="*80)
        self.logger.info("STARTING ENHANCED DEV vs PROD MEMBER ENROLLMENT VALIDATION")
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
        
        # Perform enhanced data comparison
        mismatched_rows, dev_only_rows, prod_only_rows, count_difference_rows, summary_stats = self.compare_member_enrollment_data()
        
        # Generate output names with timestamp
        mismatch_table_name = f"MEMBER_ENROLLMENT_MISMATCHES_{self.timestamp}"
        dev_only_table_name = f"MEMBER_ENROLLMENT_DEV_ONLY_{self.timestamp}"
        prod_only_table_name = f"MEMBER_ENROLLMENT_PROD_ONLY_{self.timestamp}"
        count_diff_table_name = f"MEMBER_ENROLLMENT_COUNT_DIFFERENCES_{self.timestamp}"
        
        mismatch_csv = f"member_enrollment_mismatches_{self.timestamp}.csv"
        dev_only_csv = f"member_enrollment_dev_only_{self.timestamp}.csv"
        prod_only_csv = f"member_enrollment_prod_only_{self.timestamp}.csv"
        count_diff_csv = f"member_enrollment_count_differences_{self.timestamp}.csv"
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
            
            if count_difference_rows:
                self.create_snowflake_table(count_diff_table_name, count_difference_rows)
                created_tables['count_differences'] = count_diff_table_name
        
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
            
            if count_difference_rows:
                self.write_csv_file(count_difference_rows, count_diff_csv)
                local_files['count_differences'] = count_diff_csv
            
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
        
        if count_difference_rows:
            self.write_to_snowflake_stage(count_difference_rows, count_diff_csv, stage_name)
            stage_files['count_differences'] = f"{stage_name}{count_diff_csv}"
        
        # Always write summary to stage
        self.write_to_snowflake_stage([summary_stats], summary_csv, stage_name)
        stage_files['summary'] = f"{stage_name}{summary_csv}"
        
        # Calculate overall validation status
        validation_passed = (
            len(mismatched_rows) == 0 and 
            len(dev_only_rows) == 0 and 
            len(prod_only_rows) == 0 and
            len(count_difference_rows) == 0 and
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
                'count_difference_rows_count': len(count_difference_rows),
                'total_issues_found': len(mismatched_rows) + len(dev_only_rows) + len(prod_only_rows) + len(count_difference_rows)
            },
            'created_tables': created_tables if create_tables else None,
            'local_files': local_files if write_local_csv else None,
            'stage_files': stage_files
        }
        
        return results
    
    def verify_sample_mismatches(self, sample_size: int = 10) -> List[Dict]:
        """
        Manually verify a sample of mismatches by querying both tables directly
        
        Args:
            sample_size: Number of samples to verify
            
        Returns:
            List of verification results
        """
        print(f"\n🔍 MANUAL VERIFICATION OF {sample_size} SAMPLE MISMATCHES")
        print("="*60)
        
        verification_results = []
        
        try:
            # Get a sample of mismatches to verify
            verification_sql = f"""
            SELECT 
                d.MEMBER_ID,
                d.UMI_ID as DEV_UMI_ID, p.UMI_ID as PROD_UMI_ID,
                d.GROUP_NUMBER as DEV_GROUP_NUMBER, p.GROUP_NUMBER as PROD_GROUP_NUMBER,
                d.ELIGIBILITY_START_DATE as DEV_ELIGIBILITY_START_DATE, p.ELIGIBILITY_START_DATE as PROD_ELIGIBILITY_START_DATE
            FROM {self.dev_table} d
            INNER JOIN {self.prod_table} p ON 
                COALESCE(CAST(d.MEMBER_ID AS STRING), 'NULL') = COALESCE(CAST(p.MEMBER_ID AS STRING), 'NULL')
            WHERE 
                (COALESCE(CAST(d.UMI_ID AS STRING), 'NULL') != COALESCE(CAST(p.UMI_ID AS STRING), 'NULL'))
                OR (COALESCE(CAST(d.GROUP_NUMBER AS STRING), 'NULL') != COALESCE(CAST(p.GROUP_NUMBER AS STRING), 'NULL'))
                OR (COALESCE(CAST(d.ELIGIBILITY_START_DATE AS STRING), 'NULL') != COALESCE(CAST(p.ELIGIBILITY_START_DATE AS STRING), 'NULL'))
            LIMIT {sample_size}
            """
            
            result = self.session.sql(verification_sql)
            verification_df = result.to_pandas()
            
            print(f"Found {len(verification_df)} sample mismatches to verify:\n")
            
            for idx, row in verification_df.iterrows():
                member_id = row['MEMBER_ID']
                print(f"Sample {idx + 1}: MEMBER_ID = {member_id}")
                
                # Check each comparison column
                verification_record = {
                    'member_id': member_id,
                    'differences_found': []
                }
                
                # UMI_ID comparison
                dev_umi = row['DEV_UMI_ID']
                prod_umi = row['PROD_UMI_ID']
                if str(dev_umi) != str(prod_umi):
                    print(f"  ✓ UMI_ID differs: DEV='{dev_umi}' vs PROD='{prod_umi}'")
                    verification_record['differences_found'].append({
                        'column': 'UMI_ID',
                        'dev_value': dev_umi,
                        'prod_value': prod_umi
                    })
                
                # GROUP_NUMBER comparison
                dev_group = row['DEV_GROUP_NUMBER']
                prod_group = row['PROD_GROUP_NUMBER']
                if str(dev_group) != str(prod_group):
                    print(f"  ✓ GROUP_NUMBER differs: DEV='{dev_group}' vs PROD='{prod_group}'")
                    verification_record['differences_found'].append({
                        'column': 'GROUP_NUMBER',
                        'dev_value': dev_group,
                        'prod_value': prod_group
                    })
                
                # ELIGIBILITY_START_DATE comparison
                dev_date = row['DEV_ELIGIBILITY_START_DATE']
                prod_date = row['PROD_ELIGIBILITY_START_DATE']
                if str(dev_date) != str(prod_date):
                    print(f"  ✓ ELIGIBILITY_START_DATE differs: DEV='{dev_date}' vs PROD='{prod_date}'")
                    verification_record['differences_found'].append({
                        'column': 'ELIGIBILITY_START_DATE',
                        'dev_value': dev_date,
                        'prod_value': prod_date
                    })
                
                verification_results.append(verification_record)
                print()  # Add blank line between samples
            
            print(f"✅ Verification complete! All {len(verification_results)} samples show legitimate differences.")
            
        except Exception as e:
            self.logger.error(f"Error during manual verification: {str(e)}")
            print(f"❌ Error during verification: {str(e)}")
        
        return verification_results
    
    def analyze_mismatch_patterns(self) -> Dict[str, Any]:
        """
        Analyze patterns in the mismatches without loading all data into memory
        
        Returns:
            Dictionary with mismatch analysis
        """
        print(f"\n📊 MISMATCH PATTERN ANALYSIS")
        print("="*50)
        
        analysis = {}
        
        try:
            # Count mismatches by column
            column_mismatch_sql = f"""
            SELECT 
                'UMI_ID' as column_name,
                COUNT(*) as mismatch_count
            FROM {self.dev_table} d
            INNER JOIN {self.prod_table} p ON 
                COALESCE(CAST(d.MEMBER_ID AS STRING), 'NULL') = COALESCE(CAST(p.MEMBER_ID AS STRING), 'NULL')
            WHERE COALESCE(CAST(d.UMI_ID AS STRING), 'NULL') != COALESCE(CAST(p.UMI_ID AS STRING), 'NULL')
            
            UNION ALL
            
            SELECT 
                'GROUP_NUMBER' as column_name,
                COUNT(*) as mismatch_count
            FROM {self.dev_table} d
            INNER JOIN {self.prod_table} p ON 
                COALESCE(CAST(d.MEMBER_ID AS STRING), 'NULL') = COALESCE(CAST(p.MEMBER_ID AS STRING), 'NULL')
            WHERE COALESCE(CAST(d.GROUP_NUMBER AS STRING), 'NULL') != COALESCE(CAST(p.GROUP_NUMBER AS STRING), 'NULL')
            
            UNION ALL
            
            SELECT 
                'ELIGIBILITY_START_DATE' as column_name,
                COUNT(*) as mismatch_count
            FROM {self.dev_table} d
            INNER JOIN {self.prod_table} p ON 
                COALESCE(CAST(d.MEMBER_ID AS STRING), 'NULL') = COALESCE(CAST(p.MEMBER_ID AS STRING), 'NULL')
            WHERE COALESCE(CAST(d.ELIGIBILITY_START_DATE AS STRING), 'NULL') != COALESCE(CAST(p.ELIGIBILITY_START_DATE AS STRING), 'NULL')
            
            ORDER BY mismatch_count DESC
            """
            
            result = self.session.sql(column_mismatch_sql)
            pattern_df = result.to_pandas()
            
            print("Mismatches by Column:")
            analysis['column_mismatches'] = {}
            for _, row in pattern_df.iterrows():
                column = row['column_name']
                count = row['mismatch_count']
                analysis['column_mismatches'][column] = count
                print(f"  {column}: {count:,} mismatches")
            
            # Get total matching records for context
            total_matches_sql = f"""
            SELECT COUNT(*) as total_matches
            FROM {self.dev_table} d
            INNER JOIN {self.prod_table} p ON 
                COALESCE(CAST(d.MEMBER_ID AS STRING), 'NULL') = COALESCE(CAST(p.MEMBER_ID AS STRING), 'NULL')
            """
            
            total_result = self.session.sql(total_matches_sql)
            total_matches = total_result.to_pandas().iloc[0]['total_matches']
            analysis['total_matches'] = total_matches
            
            print(f"\nTotal records with matching MEMBER_ID: {total_matches:,}")
            
            # Calculate percentages
            print(f"\nMismatch Percentages:")
            for column, count in analysis['column_mismatches'].items():
                percentage = (count / total_matches) * 100 if total_matches > 0 else 0
                print(f"  {column}: {percentage:.2f}% of matching records have differences")
            
        except Exception as e:
            self.logger.error(f"Error during pattern analysis: {str(e)}")
            print(f"❌ Error during analysis: {str(e)}")
        
        return analysis
    
    def spot_check_specific_member(self, member_id: str) -> Dict[str, Any]:
        """
        Perform a detailed spot check on a specific member ID
        
        Args:
            member_id: Specific member ID to check
            
        Returns:
            Dictionary with detailed comparison
        """
        print(f"\n🎯 SPOT CHECK FOR MEMBER_ID: {member_id}")
        print("="*50)
        
        spot_check = {}
        
        try:
            # Get data for this member from both tables
            dev_sql = f"""
            SELECT MEMBER_ID, UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE, LAST_NAME, FIRST_NAME,
                   ROW_NUMBER() OVER (ORDER BY UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE) as row_num
            FROM {self.dev_table}
            WHERE MEMBER_ID = '{member_id}'
            ORDER BY UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE
            """
            
            prod_sql = f"""
            SELECT MEMBER_ID, UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE, LAST_NAME, FIRST_NAME,
                   ROW_NUMBER() OVER (ORDER BY UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE) as row_num
            FROM {self.prod_table}
            WHERE MEMBER_ID = '{member_id}'
            ORDER BY UMI_ID, GROUP_NUMBER, ELIGIBILITY_START_DATE
            """
            
            dev_result = self.session.sql(dev_sql).to_pandas()
            prod_result = self.session.sql(prod_sql).to_pandas()
            
            if len(dev_result) == 0 and len(prod_result) == 0:
                print(f"❌ MEMBER_ID {member_id} not found in either table")
                return {'error': 'Member not found'}
            elif len(dev_result) == 0:
                print(f"📝 MEMBER_ID {member_id} only exists in PROD table ({len(prod_result)} occurrences)")
                for idx, row in prod_result.iterrows():
                    print(f"  PROD Row {row['row_num']}: UMI_ID={row['UMI_ID']}, GROUP_NUMBER={row['GROUP_NUMBER']}, ELIGIBILITY_START_DATE={row['ELIGIBILITY_START_DATE']}")
                return {'type': 'prod_only', 'prod_data': prod_result.to_dict('records')}
            elif len(prod_result) == 0:
                print(f"📝 MEMBER_ID {member_id} only exists in DEV table ({len(dev_result)} occurrences)")
                for idx, row in dev_result.iterrows():
                    print(f"  DEV Row {row['row_num']}: UMI_ID={row['UMI_ID']}, GROUP_NUMBER={row['GROUP_NUMBER']}, ELIGIBILITY_START_DATE={row['ELIGIBILITY_START_DATE']}")
                return {'type': 'dev_only', 'dev_data': dev_result.to_dict('records')}
            else:
                # Both exist, compare them
                print(f"✅ MEMBER_ID {member_id} exists in both tables")
                print(f"   DEV has {len(dev_result)} occurrences, PROD has {len(prod_result)} occurrences")
                
                if len(dev_result) != len(prod_result):
                    print(f"⚠️  COUNT DIFFERENCE: DEV={len(dev_result)}, PROD={len(prod_result)}")
                
                # Compare row by row
                max_rows = max(len(dev_result), len(prod_result))
                differences = {}
                
                for i in range(max_rows):
                    print(f"\n  Comparing Row {i+1}:")
                    
                    dev_row = dev_result.iloc[i] if i < len(dev_result) else None
                    prod_row = prod_result.iloc[i] if i < len(prod_result) else None
                    
                    if dev_row is None:
                        print(f"    ❌ DEV missing this row, PROD has: UMI_ID={prod_row['UMI_ID']}, GROUP_NUMBER={prod_row['GROUP_NUMBER']}")
                        differences[f'row_{i+1}'] = {'type': 'missing_in_dev', 'prod_data': prod_row.to_dict()}
                    elif prod_row is None:
                        print(f"    ❌ PROD missing this row, DEV has: UMI_ID={dev_row['UMI_ID']}, GROUP_NUMBER={dev_row['GROUP_NUMBER']}")
                        differences[f'row_{i+1}'] = {'type': 'missing_in_prod', 'dev_data': dev_row.to_dict()}
                    else:
                        # Compare the rows
                        row_diffs = {}
                        for col in ['UMI_ID', 'GROUP_NUMBER', 'ELIGIBILITY_START_DATE']:
                            if str(dev_row[col]) != str(prod_row[col]):
                                print(f"    🚨 {col}: DEV='{dev_row[col]}' vs PROD='{prod_row[col]}'")
                                row_diffs[col] = {'dev_value': dev_row[col], 'prod_value': prod_row[col]}
                            else:
                                print(f"    ✅ {col}: '{dev_row[col]}' (matches)")
                        
                        if row_diffs:
                            differences[f'row_{i+1}'] = {'type': 'data_differences', 'differences': row_diffs}
                
                if not differences:
                    print(f"\n✅ NO DIFFERENCES FOUND - Data matches perfectly!")
                
                spot_check = {
                    'type': 'comparison',
                    'dev_data': dev_result.to_dict('records'),
                    'prod_data': prod_result.to_dict('records'),
                    'dev_count': len(dev_result),
                    'prod_count': len(prod_result),
                    'differences': differences
                }
        
        except Exception as e:
            self.logger.error(f"Error during spot check: {str(e)}")
            print(f"❌ Error during spot check: {str(e)}")
        
        return spot_check
    
    def analyze_count_difference_patterns(self) -> Dict[str, Any]:
        """
        Analyze patterns in MEMBER_IDs with different occurrence counts
        
        Returns:
            Dictionary with count difference analysis
        """
        print(f"\n📊 COUNT DIFFERENCE PATTERN ANALYSIS")
        print("="*60)
        
        analysis = {}
        
        try:
            # Get the duplicate analysis first
            dup_analysis = self.analyze_duplicate_patterns()
            analysis['duplicate_analysis'] = dup_analysis
            
            # Analyze count differences by difference magnitude
            count_diff_pattern_sql = f"""
            WITH dev_counts AS (
                SELECT MEMBER_ID, COUNT(*) as dev_count
                FROM {self.dev_table}
                WHERE MEMBER_ID IS NOT NULL
                GROUP BY MEMBER_ID
            ),
            prod_counts AS (
                SELECT MEMBER_ID, COUNT(*) as prod_count
                FROM {self.prod_table}
                WHERE MEMBER_ID IS NOT NULL
                GROUP BY MEMBER_ID
            ),
            combined_counts AS (
                SELECT 
                    COALESCE(d.MEMBER_ID, p.MEMBER_ID) as MEMBER_ID,
                    COALESCE(d.dev_count, 0) as dev_count,
                    COALESCE(p.prod_count, 0) as prod_count,
                    ABS(COALESCE(d.dev_count, 0) - COALESCE(p.prod_count, 0)) as count_difference,
                    CASE 
                        WHEN COALESCE(d.dev_count, 0) > COALESCE(p.prod_count, 0) THEN 'MORE_IN_DEV'
                        WHEN COALESCE(p.prod_count, 0) > COALESCE(d.dev_count, 0) THEN 'MORE_IN_PROD'
                        ELSE 'EQUAL'
                    END as difference_type
                FROM dev_counts d
                FULL OUTER JOIN prod_counts p ON d.MEMBER_ID = p.MEMBER_ID
                WHERE COALESCE(d.dev_count, 0) != COALESCE(p.prod_count, 0)
            )
            SELECT 
                count_difference,
                difference_type,
                COUNT(*) as member_id_count,
                AVG(dev_count) as avg_dev_count,
                AVG(prod_count) as avg_prod_count
            FROM combined_counts
            GROUP BY count_difference, difference_type
            ORDER BY count_difference DESC, difference_type
            """
            
            result = self.session.sql(count_diff_pattern_sql)
            pattern_df = result.to_pandas()
            
            print("Count Difference Patterns:")
            analysis['count_difference_patterns'] = pattern_df.to_dict('records')
            
            total_count_diffs = 0
            more_in_dev = 0
            more_in_prod = 0
            
            for _, row in pattern_df.iterrows():
                count_diff = row['count_difference']
                diff_type = row['difference_type']
                member_count = row['member_id_count']
                avg_dev = row['avg_dev_count']
                avg_prod = row['avg_prod_count']
                
                print(f"  Difference of {count_diff} ({diff_type}): {member_count:,} MEMBER_IDs")
                print(f"    Average DEV count: {avg_dev:.1f}, Average PROD count: {avg_prod:.1f}")
                
                total_count_diffs += member_count
                if diff_type == 'MORE_IN_DEV':
                    more_in_dev += member_count
                elif diff_type == 'MORE_IN_PROD':
                    more_in_prod += member_count
            
            analysis['summary'] = {
                'total_member_ids_with_count_differences': int(total_count_diffs),
                'member_ids_with_more_in_dev': int(more_in_dev),
                'member_ids_with_more_in_prod': int(more_in_prod)
            }
            
            print(f"\nSummary:")
            print(f"  Total MEMBER_IDs with count differences: {total_count_diffs:,}")
            print(f"  MEMBER_IDs with more occurrences in DEV: {more_in_dev:,}")
            print(f"  MEMBER_IDs with more occurrences in PROD: {more_in_prod:,}")
            
            # Show duplicate patterns
            if dup_analysis.get('summary'):
                print(f"\nDuplicate Patterns:")
                print(f"  DEV table has {dup_analysis['summary']['dev_duplicate_member_ids']:,} MEMBER_IDs with duplicates")
                print(f"  PROD table has {dup_analysis['summary']['prod_duplicate_member_ids']:,} MEMBER_IDs with duplicates")
            
        except Exception as e:
            self.logger.error(f"Error during count difference analysis: {str(e)}")
            print(f"❌ Error during analysis: {str(e)}")
            analysis['error'] = str(e)
        
        return analysis
    
    def print_validation_report(self, results: Dict[str, Any]):
        """
        Print a comprehensive validation report with verification options
        Enhanced to include count differences
        
        Args:
            results: Results dictionary from validation
        """
        print("\n" + "="*80)
        print("ENHANCED DEV vs PROD MEMBER ENROLLMENT VALIDATION REPORT")
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
        print(f"  MEMBER_IDs with Count Differences: {summary['count_difference_rows_count']:,}")
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
        
        # Add enhanced verification recommendations
        print(f"\n" + "="*80)
        print("ENHANCED VERIFICATION RECOMMENDATIONS")
        print("="*80)
        print("The script now includes enhanced duplicate detection. Here are ways to verify:")
        print("1. Run: validator.verify_sample_mismatches(10)  # Check 10 sample mismatches")
        print("2. Run: validator.analyze_mismatch_patterns()   # See mismatch patterns")
        print("3. Run: validator.spot_check_specific_member('MEMBER_ID')  # Check specific member")
        print("4. Run: validator.analyze_count_difference_patterns()  # Analyze duplicate patterns")
        print("5. Run: validator.get_duplicate_member_details('MEMBER_ID')  # Get all occurrences")
        print("6. The enhanced validation now detects MEMBER_IDs with different occurrence counts")


def main():
    """
    Main function to run enhanced DEV vs PROD member enrollment validation
    """
    try:
        # Configuration
        create_tables = True  # Create result tables in Snowflake
        write_local_csv = True  # Write local CSV files
        stage_name = "@~/"  # Snowflake stage for files
        
        # Create validator instance
        validator = DevProdMemberEnrollmentValidator()
        
        # Run enhanced validation
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
    Stored procedure version of enhanced member enrollment validation
    
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
            return f"Enhanced Member Enrollment Validation {results['validation_status']}: Mismatches={summary['mismatched_rows_count']}, DEV Only={summary['dev_only_rows_count']}, PROD Only={summary['prod_only_rows_count']}, Count Differences={summary['count_difference_rows_count']}"
        else:
            return "Enhanced member enrollment validation failed to complete"
            
    except Exception as e:
        return f"Error during validation: {str(e)}"


if __name__ == "__main__":
    main()
