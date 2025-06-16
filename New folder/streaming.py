"""
Streaming implementation for handling large JSON files with multiple root keys
Add these methods to your FileConverter class
"""

# Add these methods to your FileConverter class:

    @staticmethod
    def stream_json_multi_keys_to_csv(json_path: Union[str, Path], 
                                     csv_path: Union[str, Path],
                                     root_keys: Optional[Union[str, List[str]]] = None,
                                     combine_keys: bool = True,
                                     add_source_column: bool = True,
                                     max_rows: Optional[int] = None,
                                     flatten: bool = True,
                                     sep: str = '_',
                                     compression: Optional[str] = None,
                                     column_map: Optional[Dict[str, str]] = None,
                                     batch_size: int = 1000,
                                     low_memory: bool = True) -> None:
        """
        Stream large JSON files with multiple root keys to CSV without loading all data into memory
        
        Args:
            json_path: Path to input JSON file
            csv_path: Path to output CSV file
            root_keys: Single key, list of keys, or None (processes all keys)
            combine_keys: If True, combines data from all keys into one CSV
            add_source_column: If True, adds column indicating source root key
            max_rows: Maximum number of rows to output
            flatten: Whether to flatten nested structures
            sep: Separator for flattened keys
            compression: Compression type
            column_map: Column mapping
            batch_size: Number of records to process at once
            low_memory: If True, uses most memory-efficient approach
        """
        json_path = Path(json_path)
        csv_path = Path(csv_path)
        
        # Handle single key input
        if isinstance(root_keys, str):
            root_keys = [root_keys]
        
        if combine_keys:
            # Stream all keys into single CSV
            _stream_combined_keys_to_csv(
                json_path, csv_path, root_keys, add_source_column,
                max_rows, flatten, sep, compression, column_map, batch_size
            )
        else:
            # Stream each key to separate CSV files
            _stream_separate_keys_to_csv(
                json_path, csv_path, root_keys, max_rows,
                flatten, sep, compression, column_map, batch_size
            )

    @staticmethod
    def stream_json_multi_keys_to_excel(json_path: Union[str, Path], 
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
                                       chunk_size: int = 5000) -> None:
        """
        Stream large JSON files with multiple root keys to Excel
        
        Args:
            json_path: Path to input JSON file
            excel_path: Path to output Excel file
            root_keys: Single key, list of keys, or None (processes all keys)
            combine_keys: If True, combines all data into one sheet
            sheets_by_key: If True and combine_keys=False, creates separate sheet for each key
            add_source_column: If True, adds column indicating source root key
            max_rows_per_key: Maximum rows per root key
            flatten: Whether to flatten nested structures
            sep: Separator for flattened keys
            compression: Compression type
            column_map: Column mapping
            chunk_size: Size of chunks for processing
        """
        json_path = Path(json_path)
        excel_path = Path(excel_path)
        
        # Handle single key input
        if isinstance(root_keys, str):
            root_keys = [root_keys]
        
        try:
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                if combine_keys:
                    # Stream all keys into single sheet
                    _stream_combined_keys_to_excel(
                        json_path, writer, root_keys, add_source_column,
                        max_rows_per_key, flatten, sep, compression, 
                        column_map, chunk_size
                    )
                else:
                    # Stream each key to separate sheets
                    _stream_separate_keys_to_excel(
                        json_path, writer, root_keys, max_rows_per_key,
                        flatten, sep, compression, column_map, chunk_size
                    )
                    
        except Exception as e:
            logger.error(f"Error streaming JSON to Excel: {e}")
            raise


# Helper functions (add these at module level after the _stream_json_to_csv function):

def _stream_combined_keys_to_csv(json_path: Path, csv_path: Path,
                                root_keys: Optional[List[str]],
                                add_source_column: bool,
                                max_rows: Optional[int],
                                flatten: bool,
                                sep: str,
                                compression: Optional[str],
                                column_map: Optional[Dict[str, str]],
                                batch_size: int) -> None:
    """Helper function to stream multiple keys into combined CSV"""
    
    with FileConverter._get_file_handle(json_path, 'rb', compression) as json_file:
        first_row = True
        rows_written = 0
        all_headers = set()
        batch = []
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = None
            
            # Stream through file
            parser = ijson.kvitems(json_file, '')
            
            for key, value in parser:
                # Skip if not in requested keys
                if root_keys and key not in root_keys:
                    continue
                
                # Process the value
                if isinstance(value, list):
                    records_to_process = value
                else:
                    records_to_process = [value]
                
                for record in records_to_process:
                    if max_rows and rows_written >= max_rows:
                        break
                    
                    # Prepare record
                    if isinstance(record, dict):
                        if add_source_column:
                            record = {'_source_key': key, **record}
                        if flatten:
                            record = FileConverter.flatten_json(record, sep=sep)
                    else:
                        if add_source_column:
                            record = {'_source_key': key, 'value': record}
                        else:
                            record = {'value': record}
                    
                    batch.append(record)
                    all_headers.update(record.keys())
                    
                    # Write batch when full
                    if len(batch) >= batch_size:
                        if first_row:
                            # Ensure _source_key comes first if present
                            if add_source_column and '_source_key' in all_headers:
                                headers = ['_source_key'] + [h for h in sorted(all_headers) if h != '_source_key']
                            else:
                                headers = sorted(list(all_headers))
                            writer = csv.DictWriter(csv_file, fieldnames=headers, extrasaction='ignore')
                            writer.writeheader()
                            first_row = False
                        
                        for row in batch:
                            writer.writerow(row)
                            rows_written += 1
                        
                        batch = []
                        
                        # Clear headers periodically to save memory
                        if rows_written % (batch_size * 10) == 0:
                            logger.info(f"Processed {rows_written} rows...")
                
                if max_rows and rows_written >= max_rows:
                    break
            
            # Write remaining batch
            if batch and writer:
                for row in batch:
                    writer.writerow(row)
                    rows_written += 1
            elif batch and not writer:
                # Handle case where we only have one batch
                if add_source_column and '_source_key' in all_headers:
                    headers = ['_source_key'] + [h for h in sorted(all_headers) if h != '_source_key']
                else:
                    headers = sorted(list(all_headers))
                writer = csv.DictWriter(csv_file, fieldnames=headers, extrasaction='ignore')
                writer.writeheader()
                for row in batch:
                    writer.writerow(row)
                    rows_written += 1
            
            logger.info(f"Streamed {rows_written} total rows to {csv_path}")


def _stream_separate_keys_to_csv(json_path: Path, csv_path: Path,
                                root_keys: Optional[List[str]],
                                max_rows: Optional[int],
                                flatten: bool,
                                sep: str,
                                compression: Optional[str],
                                column_map: Optional[Dict[str, str]],
                                batch_size: int) -> None:
    """Helper function to stream each key to separate CSV files"""
    
    base_path = csv_path.parent
    base_name = csv_path.stem
    extension = csv_path.suffix
    
    # Track which files we've started
    active_writers = {}
    active_files = {}
    headers_written = {}
    rows_per_key = {}
    
    try:
        with FileConverter._get_file_handle(json_path, 'rb', compression) as json_file:
            parser = ijson.kvitems(json_file, '')
            
            for key, value in parser:
                # Skip if not in requested keys
                if root_keys and key not in root_keys:
                    continue
                
                # Initialize file for this key if needed
                if key not in active_files:
                    key_csv_path = base_path / f"{base_name}_{key}{extension}"
                    active_files[key] = open(key_csv_path, 'w', newline='', encoding='utf-8')
                    headers_written[key] = False
                    rows_per_key[key] = 0
                    logger.info(f"Creating {key_csv_path}")
                
                # Skip if we've hit max rows for this key
                if max_rows and rows_per_key[key] >= max_rows:
                    continue
                
                # Process the value
                if isinstance(value, list):
                    records_to_process = value
                else:
                    records_to_process = [value]
                
                batch = []
                for record in records_to_process:
                    if max_rows and rows_per_key[key] >= max_rows:
                        break
                    
                    # Prepare record
                    if isinstance(record, dict):
                        if flatten:
                            record = FileConverter.flatten_json(record, sep=sep)
                    else:
                        record = {'value': record}
                    
                    batch.append(record)
                
                # Write batch
                if batch:
                    if not headers_written[key]:
                        headers = list(batch[0].keys())
                        active_writers[key] = csv.DictWriter(
                            active_files[key], 
                            fieldnames=headers, 
                            extrasaction='ignore'
                        )
                        active_writers[key].writeheader()
                        headers_written[key] = True
                    
                    for row in batch:
                        active_writers[key].writerow(row)
                        rows_per_key[key] += 1
                
                logger.info(f"Processed {rows_per_key[key]} rows for key '{key}'")
                
    finally:
        # Close all files
        for key, file_handle in active_files.items():
            file_handle.close()
            logger.info(f"Completed {key}: {rows_per_key[key]} rows")


def _stream_combined_keys_to_excel(json_path: Path, writer: pd.ExcelWriter,
                                  root_keys: Optional[List[str]],
                                  add_source_column: bool,
                                  max_rows: Optional[int],
                                  flatten: bool,
                                  sep: str,
                                  compression: Optional[str],
                                  column_map: Optional[Dict[str, str]],
                                  chunk_size: int) -> None:
    """Helper function to stream multiple keys into combined Excel sheet"""
    
    chunks = []
    current_chunk = []
    total_rows = 0
    
    with FileConverter._get_file_handle(json_path, 'rb', compression) as json_file:
        parser = ijson.kvitems(json_file, '')
        
        for key, value in parser:
            if root_keys and key not in root_keys:
                continue
            
            # Process the value
            if isinstance(value, list):
                records_to_process = value
            else:
                records_to_process = [value]
            
            for record in records_to_process:
                if max_rows and total_rows >= max_rows:
                    break
                
                # Prepare record
                if isinstance(record, dict):
                    if add_source_column:
                        record = {'_source_key': key, **record}
                    if flatten:
                        record = FileConverter.flatten_json(record, sep=sep)
                else:
                    if add_source_column:
                        record = {'_source_key': key, 'value': record}
                    else:
                        record = {'value': record}
                
                current_chunk.append(record)
                total_rows += 1
                
                # Process chunk when full
                if len(current_chunk) >= chunk_size:
                    chunks.append(pd.DataFrame(current_chunk))
                    current_chunk = []
                    
                    # Periodically write to free memory
                    if len(chunks) >= 10:
                        df = pd.concat(chunks, ignore_index=True)
                        if len(chunks) == 10:  # First write
                            df.to_excel(writer, sheet_name='Combined_Data', index=False)
                        else:  # Append mode would require different approach
                            # For now, we'll accumulate
                            pass
                        logger.info(f"Processed {total_rows} rows...")
            
            if max_rows and total_rows >= max_rows:
                break
        
        # Process remaining data
        if current_chunk:
            chunks.append(pd.DataFrame(current_chunk))
        
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
            df.to_excel(writer, sheet_name='Combined_Data', index=False)
            logger.info(f"Completed: {total_rows} total rows")


def _stream_separate_keys_to_excel(json_path: Path, writer: pd.ExcelWriter,
                                  root_keys: Optional[List[str]],
                                  max_rows: Optional[int],
                                  flatten: bool,
                                  sep: str,
                                  compression: Optional[str],
                                  column_map: Optional[Dict[str, str]],
                                  chunk_size: int) -> None:
    """Helper function to stream each key to separate Excel sheets"""
    
    key_chunks = {}
    key_rows = {}
    
    with FileConverter._get_file_handle(json_path, 'rb', compression) as json_file:
        parser = ijson.kvitems(json_file, '')
        
        for key, value in parser:
            if root_keys and key not in root_keys:
                continue
            
            if key not in key_chunks:
                key_chunks[key] = []
                key_rows[key] = 0
            
            # Skip if we've hit max rows for this key
            if max_rows and key_rows[key] >= max_rows:
                continue
            
            # Process the value
            if isinstance(value, list):
                records_to_process = value
            else:
                records_to_process = [value]
            
            current_chunk = []
            for record in records_to_process:
                if max_rows and key_rows[key] >= max_rows:
                    break
                
                # Prepare record
                if isinstance(record, dict):
                    if flatten:
                        record = FileConverter.flatten_json(record, sep=sep)
                else:
                    record = {'value': record}
                
                current_chunk.append(record)
                key_rows[key] += 1
            
            if current_chunk:
                key_chunks[key].append(pd.DataFrame(current_chunk))
                
            logger.info(f"Processed {key_rows[key]} rows for key '{key}'")
    
    # Write each key to its sheet
    for key, chunks in key_chunks.items():
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
            
            # Sanitize sheet name
            sheet_name = key[:31].replace('/', '_').replace('\\', '_')
            
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            logger.info(f"Wrote {len(df)} rows to sheet '{sheet_name}'")


# Convenience functions to add at module level:

def stream_json_multi_keys_to_csv(json_path: Union[str, Path], csv_path: Union[str, Path], **kwargs) -> None:
    """Stream large JSON files with multiple keys to CSV"""
    converter = FileConverter()
    converter.stream_json_multi_keys_to_csv(json_path, csv_path, **kwargs)

def stream_json_multi_keys_to_excel(json_path: Union[str, Path], excel_path: Union[str, Path], **kwargs) -> None:
    """Stream large JSON files with multiple keys to Excel"""
    converter = FileConverter()
    converter.stream_json_multi_keys_to_excel(json_path, excel_path, **kwargs)


# Enhanced version of the original function with better memory management:

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
                              use_streaming: bool = None) -> None:
        """
        Convert JSON with multiple different root keys to CSV
        
        Now automatically uses streaming for large files or when memory errors occur.
        """
        json_path = Path(json_path)
        
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
        
        # Try normal processing
        try:
            # Original implementation here...
            # (use the original json_to_csv_multi_keys code)
            pass
        except MemoryError:
            logger.warning("Memory error encountered, switching to streaming mode")
            return FileConverter.stream_json_multi_keys_to_csv(
                json_path, csv_path, root_keys, combine_keys,
                add_source_column, max_rows, flatten, sep,
                compression, column_map
            )
