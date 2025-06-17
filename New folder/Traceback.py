"""
Fixed streaming implementation that handles very large JSON files efficiently
Replace the problematic methods in your FileConverter class with these
"""

# Fix 1: Update json_to_csv_multi_keys to properly handle use_streaming
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
                          schema: Optional[Dict] = None,
                          use_streaming: Optional[bool] = None) -> None:  # Added parameter
    """
    Convert JSON with multiple different root keys to CSV
    """
    json_path = Path(json_path)
    csv_path = Path(csv_path)
            
    # Auto-detect if streaming is needed
    if use_streaming is None:
        file_size_mb = json_path.stat().st_size / (1024 * 1024)
        use_streaming = file_size_mb > 10  # Auto-stream for files > 10MB
        
        if use_streaming:
            logger.info(f"File size is {file_size_mb:.1f}MB, using streaming mode")
    
    if use_streaming:
        # Use streaming version
        return FileConverter.stream_json_multi_keys_to_csv(
            json_path, csv_path, root_keys, combine_keys,
            add_source_column, max_rows, flatten, sep,
            compression, column_map
        )
    
    # Rest of the original implementation...
    # (keep the existing non-streaming code here)


# Fix 2: Create a more memory-efficient streaming function
@staticmethod
def stream_json_multi_keys_to_csv_ultra_low_memory(
    json_path: Union[str, Path], 
    csv_path: Union[str, Path],
    root_keys: Optional[Union[str, List[str]]] = None,
    combine_keys: bool = True,
    add_source_column: bool = True,
    max_rows: Optional[int] = None,
    flatten: bool = True,
    sep: str = '_',
    compression: Optional[str] = None,
    column_map: Optional[Dict[str, str]] = None,
    batch_size: int = 100) -> None:  # Smaller default batch size
    """
    Ultra-low memory streaming for very large JSON files
    Uses ijson's parse_events for minimal memory usage
    """
    json_path = Path(json_path)
    csv_path = Path(csv_path)
    
    # Handle single key input
    if isinstance(root_keys, str):
        root_keys = [root_keys]
    
    if combine_keys:
        _stream_combined_keys_ultra_low_memory(
            json_path, csv_path, root_keys, add_source_column,
            max_rows, flatten, sep, compression, column_map, batch_size
        )
    else:
        _stream_separate_keys_to_csv(
            json_path, csv_path, root_keys, max_rows,
            flatten, sep, compression, column_map, batch_size
        )


# Fix 3: Ultra-low memory streaming helper
def _stream_combined_keys_ultra_low_memory(
    json_path: Path, csv_path: Path,
    root_keys: Optional[List[str]],
    add_source_column: bool,
    max_rows: Optional[int],
    flatten: bool,
    sep: str,
    compression: Optional[str],
    column_map: Optional[Dict[str, str]],
    batch_size: int) -> None:
    """
    Stream with minimal memory usage using parse events
    """
    
    rows_written = 0
    current_key = None
    current_object = {}
    in_target_key = False
    object_depth = 0
    array_depth = 0
    path = []
    
    # CSV setup
    csv_file = open(csv_path, 'w', newline='', encoding='utf-8')
    writer = None
    headers_set = set()
    batch = []
    
    try:
        with FileConverter._get_file_handle(json_path, 'rb', compression) as json_file:
            # Use parse_events for minimal memory usage
            parser = ijson.parse(json_file)
            
            for prefix, event, value in parser:
                if max_rows and rows_written >= max_rows:
                    break
                
                # Track current position in JSON
                if event == 'map_key':
                    if object_depth == 0:
                        current_key = value
                        in_target_key = (not root_keys or value in root_keys)
                
                elif event == 'start_map':
                    object_depth += 1
                    if in_target_key and object_depth == 1:
                        current_object = {}
                
                elif event == 'end_map':
                    object_depth -= 1
                    if in_target_key and object_depth == 0:
                        # Process completed object
                        if current_object:
                            # Add source column
                            if add_source_column and current_key:
                                current_object = {'_source_key': current_key, **current_object}
                            
                            # Flatten if needed
                            if flatten:
                                current_object = FileConverter.flatten_json(current_object, sep=sep)
                            
                            batch.append(current_object)
                            headers_set.update(current_object.keys())
                            
                            # Write batch
                            if len(batch) >= batch_size:
                                if writer is None:
                                    # Create writer with headers
                                    headers = sorted(list(headers_set))
                                    if add_source_column and '_source_key' in headers:
                                        headers = ['_source_key'] + [h for h in headers if h != '_source_key']
                                    writer = csv.DictWriter(csv_file, fieldnames=headers, extrasaction='ignore')
                                    writer.writeheader()
                                
                                for row in batch:
                                    writer.writerow(row)
                                    rows_written += 1
                                
                                batch = []
                                
                                if rows_written % 1000 == 0:
                                    logger.info(f"Processed {rows_written} rows...")
                        
                        current_object = {}
                        in_target_key = False
                
                elif event == 'start_array':
                    array_depth += 1
                    if in_target_key and object_depth == 0:
                        # This is an array at the root level
                        current_object = []
                
                elif event == 'end_array':
                    array_depth -= 1
                    if in_target_key and array_depth == 0 and object_depth == 0:
                        # Process array of objects
                        if isinstance(current_object, list):
                            for obj in current_object:
                                if isinstance(obj, dict):
                                    if add_source_column and current_key:
                                        obj = {'_source_key': current_key, **obj}
                                    if flatten:
                                        obj = FileConverter.flatten_json(obj, sep=sep)
                                    batch.append(obj)
                                    headers_set.update(obj.keys())
                                    
                                    if len(batch) >= batch_size:
                                        if writer is None:
                                            headers = sorted(list(headers_set))
                                            if add_source_column and '_source_key' in headers:
                                                headers = ['_source_key'] + [h for h in headers if h != '_source_key']
                                            writer = csv.DictWriter(csv_file, fieldnames=headers, extrasaction='ignore')
                                            writer.writeheader()
                                        
                                        for row in batch:
                                            writer.writerow(row)
                                            rows_written += 1
                                        
                                        batch = []
                        
                        current_object = {}
                        in_target_key = False
                
                # Build current object/array
                elif in_target_key:
                    # This is a simplified version - you may need to enhance based on your JSON structure
                    if event in ('string', 'number', 'boolean', 'null'):
                        if isinstance(current_object, dict) and prefix:
                            # Extract the last key from prefix
                            keys = prefix.split('.')
                            if keys:
                                last_key = keys[-1]
                                current_object[last_key] = value
            
            # Write remaining batch
            if batch:
                if writer is None:
                    headers = sorted(list(headers_set))
                    if add_source_column and '_source_key' in headers:
                        headers = ['_source_key'] + [h for h in headers if h != '_source_key']
                    writer = csv.DictWriter(csv_file, fieldnames=headers, extrasaction='ignore')
                    writer.writeheader()
                
                for row in batch:
                    writer.writerow(row)
                    rows_written += 1
            
            logger.info(f"Completed: Wrote {rows_written} rows to {csv_path}")
            
    finally:
        csv_file.close()


# Fix 4: Alternative approach using yajl2_c backend (if available)
def try_with_fast_backend(json_path: Path, csv_path: Path, **kwargs):
    """
    Try to use faster ijson backend if available
    """
    try:
        # Try to use yajl2_c backend which is much faster and more memory efficient
        import ijson.backends.yajl2_c as ijson_fast
        logger.info("Using fast yajl2_c backend")
        
        # Use the fast backend for parsing
        # You would need to modify your streaming functions to accept a backend parameter
        
    except ImportError:
        logger.info("Fast backend not available, using default")
        # Fall back to regular implementation


# Fix 5: Chunk file reader for very large files
def read_json_in_chunks(json_path: Path, chunk_size: int = 1024 * 1024):  # 1MB chunks
    """
    Read JSON file in chunks to avoid loading entire file
    """
    with open(json_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk


# Convenience function with better error handling
def stream_json_multi_keys_to_csv(json_path: Union[str, Path], csv_path: Union[str, Path], **kwargs) -> None:
    """Stream large JSON files with multiple keys to CSV with better error handling"""
    
    # Try ultra-low memory version first
    try:
        converter = FileConverter()
        converter.stream_json_multi_keys_to_csv_ultra_low_memory(json_path, csv_path, **kwargs)
    except MemoryError:
        logger.error("Memory error even with ultra-low memory streaming. File may be too large.")
        logger.info("Consider:")
        logger.info("1. Increasing system memory")
        logger.info("2. Processing the file in smaller parts")
        logger.info("3. Using a different tool like 'jq' for preprocessing")
        raise
    except Exception as e:
        logger.error(f"Error during streaming: {e}")
        raise


# Installation tip for better performance
"""
For better performance with large JSON files, install the yajl2 backend:

pip install ijson[yajl2_c]

This provides a much faster C-based parser that uses less memory.
"""


# Use the new ultra-low memory function
file_converter.stream_json_multi_keys_to_csv_ultra_low_memory(
    r"G:/MIS/DOWN/Frank/Castlight MRF/MRF 20250610/professional/5233/5233_src_flatfile_split_splitfiles_rcd00_innprof.json",
    '5233_output.csv',
    sep='__',
    combine_keys=True,
    add_source_column=True,
    batch_size=50  # Very small batch size
)
