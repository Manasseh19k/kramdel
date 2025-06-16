"""
Enhancement to handle multiple different root keys in JSON files
Add these methods to your FileConverter class
"""

# Add these methods to your FileConverter class:

    @staticmethod
    def parse_json_all_root_keys(json_path: Path, 
                                root_keys: Optional[List[str]] = None,
                                compression: Optional[str] = None) -> Dict[str, List[Any]]:
        """
        Parse JSON file and extract data from multiple different root keys
        
        Args:
            json_path: Path to JSON file
            root_keys: List of root keys to extract. If None, extracts all root keys
            compression: Compression type if any
            
        Returns:
            Dictionary where keys are root key names and values are lists of all data found
        """
        all_data = {}
        
        with FileConverter._get_file_handle(json_path, 'rb', compression) as f:
            parser = ijson.kvitems(f, '')
            
            for key, value in parser:
                # If specific keys requested, only process those
                if root_keys and key not in root_keys:
                    continue
                
                # Initialize list for this key if not exists
                if key not in all_data:
                    all_data[key] = []
                
                # Add data
                if isinstance(value, list):
                    all_data[key].extend(value)
                else:
                    all_data[key].append(value)
        
        # Log findings
        for key, data_list in all_data.items():
            logger.info(f"Found {len(data_list)} records for root key '{key}'")
        
        return all_data

    @staticmethod
    def json_to_csv_multi_keys(json_path: Union[str, Path], 
                              csv_path: Union[str, Path],
                              root_keys: Optional[Union[str, List[str]]] = None,
                              combine_keys: bool = True,
                              add_source_column: bool = True,
                              max_rows: Optional[int] = None,
                              flatten: bool = True,
                              sep: str = '_',
                              compression: Optional[str] = None,
                              column_map: Optional[Dict[str, str]] = None,
                              schema: Optional[Dict] = None) -> None:
        """
        Convert JSON with multiple different root keys to CSV
        
        Args:
            json_path: Path to input JSON file
            csv_path: Path to output CSV file
            root_keys: Single key, list of keys, or None (extracts all keys)
            combine_keys: If True, combines data from all keys into one CSV
            add_source_column: If True, adds a column indicating which root key the data came from
            max_rows: Maximum number of rows to output
            flatten: Whether to flatten nested structures
            sep: Separator for flattened keys
            compression: Compression type
            column_map: Column mapping
            schema: JSON schema for validation
        """
        json_path = Path(json_path)
        csv_path = Path(csv_path)
        
        # Handle single key input
        if isinstance(root_keys, str):
            root_keys = [root_keys]
        
        try:
            # Parse all requested root keys
            all_root_data = FileConverter.parse_json_all_root_keys(json_path, root_keys, compression)
            
            if not all_root_data:
                logger.warning("No data found for specified root keys")
                pd.DataFrame().to_csv(csv_path, index=False)
                return
            
            if combine_keys:
                # Combine all data into single DataFrame
                combined_data = []
                
                for root_key, data_list in all_root_data.items():
                    for record in data_list:
                        if isinstance(record, dict):
                            # Add source column if requested
                            if add_source_column:
                                record = {'_source_key': root_key, **record}
                            
                            # Flatten if needed
                            if flatten:
                                record = FileConverter.flatten_json(record, sep=sep)
                        else:
                            # Handle non-dict data
                            if add_source_column:
                                record = {'_source_key': root_key, 'value': record}
                            else:
                                record = {'value': record}
                        
                        combined_data.append(record)
                        
                        if max_rows and len(combined_data) >= max_rows:
                            break
                    
                    if max_rows and len(combined_data) >= max_rows:
                        break
                
                # Convert to DataFrame
                df = pd.DataFrame(combined_data)
                
                # Apply column mapping
                if column_map:
                    df = FileConverter._map_columns(df, column_map)
                
                # Save to CSV
                df.to_csv(csv_path, index=False)
                logger.info(f"Combined {len(df)} rows from {len(all_root_data)} root keys into {csv_path}")
                
            else:
                # Save each root key to separate CSV files
                base_path = csv_path.parent
                base_name = csv_path.stem
                extension = csv_path.suffix
                
                for root_key, data_list in all_root_data.items():
                    # Create filename for this root key
                    key_csv_path = base_path / f"{base_name}_{root_key}{extension}"
                    
                    processed_data = []
                    for record in data_list[:max_rows] if max_rows else data_list:
                        if isinstance(record, dict):
                            if flatten:
                                record = FileConverter.flatten_json(record, sep=sep)
                        else:
                            record = {'value': record}
                        processed_data.append(record)
                    
                    # Convert to DataFrame and save
                    df = pd.DataFrame(processed_data)
                    
                    if column_map:
                        df = FileConverter._map_columns(df, column_map)
                    
                    df.to_csv(key_csv_path, index=False)
                    logger.info(f"Saved {len(df)} rows to {key_csv_path}")
                    
        except Exception as e:
            logger.error(f"Error converting JSON with multiple keys to CSV: {e}")
            raise

    @staticmethod
    def json_to_excel_multi_keys(json_path: Union[str, Path], 
                                excel_path: Union[str, Path],
                                root_keys: Optional[Union[str, List[str]]] = None,
                                combine_keys: bool = False,
                                sheets_by_key: bool = True,
                                add_source_column: bool = True,
                                max_rows_per_key: Optional[int] = None,
                                flatten: bool = True,
                                sep: str = '_',
                                compression: Optional[str] = None,
                                column_map: Optional[Dict[str, str]] = None,
                                schema: Optional[Dict] = None) -> None:
        """
        Convert JSON with multiple different root keys to Excel
        
        Args:
            json_path: Path to input JSON file
            excel_path: Path to output Excel file
            root_keys: Single key, list of keys, or None (extracts all keys)
            combine_keys: If True, combines all data into one sheet
            sheets_by_key: If True and combine_keys=False, creates separate sheet for each key
            add_source_column: If True, adds column indicating source root key
            max_rows_per_key: Maximum rows per root key
            flatten: Whether to flatten nested structures
            sep: Separator for flattened keys
            compression: Compression type
            column_map: Column mapping
            schema: JSON schema for validation
        """
        json_path = Path(json_path)
        excel_path = Path(excel_path)
        
        # Handle single key input
        if isinstance(root_keys, str):
            root_keys = [root_keys]
        
        try:
            # Parse all requested root keys
            all_root_data = FileConverter.parse_json_all_root_keys(json_path, root_keys, compression)
            
            if not all_root_data:
                logger.warning("No data found for specified root keys")
                pd.DataFrame().to_excel(excel_path, index=False)
                return
            
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                if combine_keys:
                    # Combine all data into single sheet
                    combined_data = []
                    
                    for root_key, data_list in all_root_data.items():
                        for i, record in enumerate(data_list):
                            if max_rows_per_key and i >= max_rows_per_key:
                                break
                                
                            if isinstance(record, dict):
                                if add_source_column:
                                    record = {'_source_key': root_key, **record}
                                if flatten:
                                    record = FileConverter.flatten_json(record, sep=sep)
                            else:
                                if add_source_column:
                                    record = {'_source_key': root_key, 'value': record}
                                else:
                                    record = {'value': record}
                            
                            combined_data.append(record)
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(combined_data)
                    
                    if column_map:
                        df = FileConverter._map_columns(df, column_map)
                    
                    # Save to Excel
                    df.to_excel(writer, sheet_name='Combined_Data', index=False)
                    logger.info(f"Combined {len(df)} rows into Excel sheet 'Combined_Data'")
                    
                else:
                    # Create separate sheet for each root key
                    for root_key, data_list in all_root_data.items():
                        processed_data = []
                        
                        for i, record in enumerate(data_list):
                            if max_rows_per_key and i >= max_rows_per_key:
                                break
                                
                            if isinstance(record, dict):
                                if flatten:
                                    record = FileConverter.flatten_json(record, sep=sep)
                            else:
                                record = {'value': record}
                            processed_data.append(record)
                        
                        # Convert to DataFrame
                        df = pd.DataFrame(processed_data)
                        
                        if column_map:
                            df = FileConverter._map_columns(df, column_map)
                        
                        # Sanitize sheet name (Excel has restrictions)
                        sheet_name = root_key[:31]  # Excel limit is 31 chars
                        sheet_name = sheet_name.replace('/', '_').replace('\\', '_')
                        
                        # Save to sheet
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        logger.info(f"Saved {len(df)} rows to sheet '{sheet_name}'")
                        
        except Exception as e:
            logger.error(f"Error converting JSON with multiple keys to Excel: {e}")
            raise

    @staticmethod
    def analyze_json_structure(json_path: Union[str, Path], 
                              compression: Optional[str] = None,
                              sample_size: int = 3) -> Dict[str, Any]:
        """
        Comprehensive analysis of JSON file structure including all root keys
        
        Args:
            json_path: Path to JSON file
            compression: Compression type
            sample_size: Number of samples to show per key
            
        Returns:
            Detailed analysis dictionary
        """
        json_path = Path(json_path)
        analysis = {
            'file_path': str(json_path),
            'file_size': json_path.stat().st_size,
            'root_keys': {},
            'total_unique_keys': 0,
            'total_records': 0,
            'recommendations': []
        }
        
        # Get all root keys and their counts
        key_counts = FileConverter.diagnose_json_root_keys(json_path, compression)
        analysis['total_unique_keys'] = len(key_counts)
        
        # Analyze each root key
        with FileConverter._get_file_handle(json_path, 'rb', compression) as f:
            parser = ijson.kvitems(f, '')
            
            for key, value in parser:
                if key not in analysis['root_keys']:
                    analysis['root_keys'][key] = {
                        'count': 0,
                        'data_types': set(),
                        'is_array': False,
                        'sample_structures': [],
                        'total_items': 0
                    }
                
                key_info = analysis['root_keys'][key]
                key_info['count'] += 1
                
                # Analyze data type and structure
                if isinstance(value, list):
                    key_info['is_array'] = True
                    key_info['total_items'] += len(value)
                    # Sample first few items
                    for item in value[:sample_size]:
                        if isinstance(item, dict):
                            structure = {'type': 'dict', 'keys': list(item.keys())[:10]}
                        else:
                            structure = {'type': type(item).__name__, 'value': str(item)[:50]}
                        if structure not in key_info['sample_structures']:
                            key_info['sample_structures'].append(structure)
                else:
                    key_info['total_items'] += 1
                    if isinstance(value, dict):
                        structure = {'type': 'dict', 'keys': list(value.keys())[:10]}
                    else:
                        structure = {'type': type(value).__name__, 'value': str(value)[:50]}
                    if structure not in key_info['sample_structures']:
                        key_info['sample_structures'].append(structure)
                
                key_info['data_types'].add(type(value).__name__)
        
        # Convert sets to lists for JSON serialization
        for key_info in analysis['root_keys'].values():
            key_info['data_types'] = list(key_info['data_types'])
        
        # Calculate total records
        analysis['total_records'] = sum(info['total_items'] for info in analysis['root_keys'].values())
        
        # Generate recommendations
        if analysis['total_unique_keys'] > 1:
            analysis['recommendations'].append(
                f"File contains {analysis['total_unique_keys']} different root keys. "
                "Consider using json_to_csv_multi_keys() or json_to_excel_multi_keys()."
            )
        
        # Check for duplicate keys
        for key, info in analysis['root_keys'].items():
            if info['count'] > 1:
                analysis['recommendations'].append(
                    f"Root key '{key}' appears {info['count']} times. "
                    "Use handle_multiple_roots=True to capture all occurrences."
                )
        
        return analysis


# Add these convenience functions at module level:

def json_to_csv_multi_keys(json_path: Union[str, Path], csv_path: Union[str, Path], **kwargs) -> None:
    """Convert JSON with multiple different root keys to CSV"""
    converter = FileConverter()
    converter.json_to_csv_multi_keys(json_path, csv_path, **kwargs)

def json_to_excel_multi_keys(json_path: Union[str, Path], excel_path: Union[str, Path], **kwargs) -> None:
    """Convert JSON with multiple different root keys to Excel"""
    converter = FileConverter()
    converter.json_to_excel_multi_keys(json_path, excel_path, **kwargs)

def analyze_json_structure(json_path: Union[str, Path], **kwargs) -> None:
    """Analyze and print comprehensive JSON structure information"""
    analysis = FileConverter.analyze_json_structure(json_path, **kwargs)
    
    print(f"\n{'='*60}")
    print(f"JSON Structure Analysis: {analysis['file_path']}")
    print(f"{'='*60}")
    print(f"File size: {analysis['file_size']:,} bytes")
    print(f"Total unique root keys: {analysis['total_unique_keys']}")
    print(f"Total records: {analysis['total_records']}")
    
    print(f"\nRoot Keys Detail:")
    print(f"{'-'*60}")
    
    for key, info in analysis['root_keys'].items():
        print(f"\nKey: '{key}'")
        print(f"  Occurrences: {info['count']}")
        print(f"  Total items: {info['total_items']}")
        print(f"  Data types: {', '.join(info['data_types'])}")
        print(f"  Is array: {info['is_array']}")
        print(f"  Sample structures:")
        for i, structure in enumerate(info['sample_structures'], 1):
            print(f"    {i}. {structure}")
    
    if analysis['recommendations']:
        print(f"\nRecommendations:")
        print(f"{'-'*60}")
        for rec in analysis['recommendations']:
            print(f"• {rec}")
    
    print(f"\n{'='*60}")


# Example usage scenarios:
"""
# Scenario 1: JSON with multiple different root keys
{
  "users": [{"id": 1, "name": "Alice"}],
  "products": [{"id": 101, "name": "Laptop"}],
  "orders": [{"id": 1001, "user_id": 1, "product_id": 101}]
}

# Usage:
analyze_json_structure('data.json')

# Extract all keys and combine into one CSV
json_to_csv_multi_keys('data.json', 'combined.csv', combine_keys=True)

# Extract specific keys only
json_to_csv_multi_keys('data.json', 'output.csv', root_keys=['users', 'orders'])

# Create separate CSV for each key
json_to_csv_multi_keys('data.json', 'output.csv', combine_keys=False)

# Excel with separate sheets for each key
json_to_excel_multi_keys('data.json', 'output.xlsx', sheets_by_key=True)


# Scenario 2: JSON with repeated root keys
{"data": [{"id": 1}]}
{"data": [{"id": 2}]}
{"info": {"version": "1.0"}}
{"data": [{"id": 3}]}

# Usage:
# This will extract all 3 'data' occurrences AND the 'info' key
json_to_csv_multi_keys('data.json', 'all_data.csv')
"""
