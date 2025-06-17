import json
import csv
import pandas as pd
from pandas import json_normalize
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Union, List, Dict, Any, Optional
import ijson
import logging
import concurrent.futures
import gzip
import bz2
import lzma
import jsonschema
from functools import partial
from tqdm import tqdm

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileConverter:
    """Main converter class for handling various file formats."""
    @staticmethod
    def flatten_json(data: Union[Dict, List], parent_key: str='', sep: str='_') -> Dict:
        """ 
        Flatten a nested JSON structure into a flat dictionary.
        
        Args:
            data (Union[Dict, List]): The JSON data to flatten.
            parent_key (str): The base key to prepend to flattened keys.
            sep (str): Separator to use between keys.
        """
        items = []

        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k

                if isinstance(v, dict):
                    items.extend(FileConverter.flatten_json(v, new_key, sep).items())
                elif isinstance(v, list):
                    if len(v) > 0:
                        if isinstance(v[0], (dict, list)):
                            # If list contains dictionaries, flatten each item
                            for i, item in enumerate(v):
                                items.extend(FileConverter.flatten_json(item, f"{new_key}{sep}{i}", sep).items())
                        else:
                            # If list contains simple values, join them
                            items.append((new_key, ','.join(str(x) for x in v)))
                    else:
                        items.append((new_key, ''))
                else:
                    items.append((new_key, v))
        elif isinstance(data, list):
            # Handle a case where root is a list
            for i, item in enumerate(data):
                if isinstance(item, (dict, list)):
                    items.extend(FileConverter.flatten_json(item, f"{parent_key}{sep}{i}" if parent_key else str(i), sep).items())
                else:
                    items.append((f"{parent_key}{sep}{i}" if parent_key else str(i), item))
        else:
            items.append((parent_key, data))
        
        return dict(items)
    
    @staticmethod
    def normalize_nested_arrays(data: List[Dict], sep: str='_') -> pd.DataFrame:
        """ Normalize nested arrays in JSON data into a flat DataFrame.
        Args:
            data (List[Dict]): List of JSON records to normalize.
            sep (str): Separator to use for flattening nested keys.
        Returns:
            pd.DataFrame: A DataFrame with flattened structure/normalized data.
        """
        try:
            if data and isinstance(data[0], dict):
                df = json_normalize(data, sep=sep)

                # Check if there are still nested lists/dicts in the dataframe
                has_nested = False
                for col in df.columns:
                    if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                        has_nested = True
                        break
                
                if has_nested:
                    flattened_records = []
                    for record in data:
                        flattened = FileConverter.flatten_json(record, sep=sep)
                        flattened_records.append(flattened)
                    df = pd.DataFrame(flattened_records)
                return df
        except Exception as e:
            logger.warning(f"json_normalize failed, using manual flattening: {e}")

        flattened_records = []
        for record in data:
            flattened = FileConverter.flatten_json(record, sep=sep)
            flattened_records.append(flattened)
        
        return pd.DataFrame(flattened_records)
    
    @staticmethod
    def expand_arrays_to_rows(data: List[Dict], array_columns: List[str]=None) -> pd.DataFrame:
        """
        Expand arrays in specified columns to separate rows.
        Args:
            data (List[Dict]): List of JSON records to process.
            array_columns (List[str]): List of column paths that contain arrays to expand.
        Returns:
            pd.DataFrame: A DataFrame with arrays expanded to rows.
        """
        if not array_columns:
            return pd.DataFrame(data)

        df = json_normalize(data)

        for col_path in array_columns:
            if col_path in df.columns and df[col_path].notna().any():
                if df[col_path].apply(lambda x: isinstance(x, list)).any():
                    df = df.explode(col_path)

                    # If the expanded column contains dicts, normalize them
                    if df[col_path].apply(lambda x: isinstance(x, dict)).any():
                        normalized = json_normalize(df[col_path].dropna().to_list())
                        normalized.index = df[col_path].dropna().index

                        normalized.columns = [f"{col_path}.{subcol}" for subcol in normalized.columns]

                        # Merge back with original dataframe
                        df = df.drop(columns=[col_path]).join(normalized)
        return df.reset_index(drop=True)
    
    @staticmethod
    def _get_file_handle(file_path: Union[str, Path], mode: str, compression: Optional[str]=None):
        """ Get a file handle with optional compression.
        Args:
            file_path (Union[str, Path]): Path to the file.
            mode (str): Mode to open the file ('r', 'w', etc.).
            compression (Optional[str]): Compression type ('gzip', 'bz2', 'lzma', None).
         Returns:
            File handle: Opened file handle with specified mode and compression.
            """
        file_path = Path(file_path)
        if compression == 'gzip':
            return gzip.open(file_path, mode + 't', encoding='utf-8')
        elif compression == 'bz2':
            return bz2.open(file_path, mode + 't', encoding='utf-8')
        elif compression == 'lzma':
            return lzma.open(file_path, mode + 't', encoding='utf-8')
        else:
            if 'b' in mode:
                return open(file_path, mode)
            else:
                return open(file_path, mode, encoding='utf-8')
    
    @staticmethod
    def _validate_data(data: Any, schema: Dict) -> bool:
        """ Validate JSON data against a JSON schema.
        Args:
            data (Any): The JSON data to validate.
            schema (Dict): The JSON schema to validate against.
        Returns:
           bool: True if validation passes, raises an exception otherwise.
            """
        try:
            jsonschema.validate(instance=data, schema=schema)
        except jsonschema.exceptions.ValidationError as e:
            logger.error(f"Data validation failed: {e.message}")
            raise
    
    @staticmethod
    def _map_columns(df: pd.DataFrame, column_map: Dict[str, str]) -> pd.DataFrame:
        """ Map DataFrame columns to new names based on a provided mapping.
        Args:
            df (pd.DataFrame): The DataFrame to process.
            column_map (Dict[str, str]): A dictionary mapping old column names to new names.
        Returns:
            pd.DataFrame: DataFrame with renamed columns.
        """
        if column_map:
            df = df.rename(columns=column_map)
        return df
    
    @staticmethod
    def _process_chunk(chunk: List[Dict], flatten: bool, sep: str, column_map: Optional[Dict[str, str]], schema: Optional[Dict]) -> pd.DataFrame:
        """ Process a chunk of JSON data.
        Args:
            chunk (List[Dict]): A list of JSON records to process.
            flatten (bool): Whether to flatten nested structures.
            sep (str): Separator for flattening keys.
            column_map (Optional[Dict[str, str]]): Mapping of old column names to new names.
            schema (Optional[Dict]): JSON schema for validation.
        Returns:
            pd.DataFrame: Processed DataFrame.
        """
        if schema:
            for record in chunk:
                FileConverter._validate_data(record, schema)
        if flatten:
            processed = [FileConverter.flatten_json(record, sep=sep) for record in chunk]
        else:
            processed = chunk
        
        df = pd.DataFrame(processed)

        if column_map:
            df = FileConverter._map_columns(df, column_map)
        
        return df


    # New Method Duplicates in root key
    @staticmethod
    def parse_json_multiple_roots(json_path: Path, root_key: Optional[str]=None, compression: Optional[str]=None) -> List[Dict]:
        """
        Parse JSON file with multiple occurances of the same root keys.
        args:
            json_path (Path): Path to the JSON file.
            root_key (Optional[str]): Root key to extract data from if JSON is nested.
            compression (Optional[str]): Compression type ('gzip', 'bz2', 'lzma', None).
        Returns:
            List containing all data from the specified root key or all objects
        """
        all_data = []

        with FileConverter._get_file_handle(json_path, 'rb', compression) as f:
            if root_key:
                # Use ijson to parse all occurances of the root key
                parser = ijson.kvitems(f, '')

                for key, value in parser:
                    if key == root_key:
                        if isinstance(value, list):
                            all_data.extend(value)
                        else:
                            all_data.append(value)
                logger.info(f"Found {len(all_data)} records under root key '{root_key}' in {json_path}")
            else:
                # Try standard parsing first
                f.seek(0)
                try:
                    content = f.read()
                    if isinstance(content, bytes):
                        content = content.decode('utf-8')
                    data = json.loads(content)

                    if isinstance(data, list):
                        all_data = data
                    else:
                        all_data = [data]
                except json.JSONDecodeError:
                    # If standard parsing fails, use ijson to parse all objects
                    f.seek(0)
                    content = f.read()
                    if isinstance(content, bytes):
                        content = content.decode('utf-8')
                    
                    decoder = json.JSONDecoder()
                    idx = 0

                    while idx < len(content):
                        content = content[idx:].lstrip()
                        if not content:
                            break
                    
                        try:
                            obj, end_idx = decoder.raw_decode(content)
                            all_data.append(obj)
                            idx += end_idx
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decoding error at index {idx}: {e}")
                            break


    @staticmethod
    def json_to_csv_expanded(json_path: Union[str, Path], csv_path: Union[str, Path], array_columns: List[str], root_key: Optional[str]=None, max_rows: Optional[int]=None) -> None:
        """ Convert JSON to CSV with expanded arrays.
        Args:
            json_path (Union[str, Path]): Path to the input JSON file.
            csv_path (Union[str, Path]): Path to the output CSV file.
            array_columns (List[str]): List of column paths that contain arrays to expand.
            root_key (Optional[str]): Root key to extract data from if JSON is nested.
            max_rows (Optional[int]): Maximum number of rows to write to CSV.
        """
        json_path = Path(json_path)
        csv_path = Path(csv_path)

        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract data from root key if specified
            if root_key and isinstance(data, dict):
                data = data.get(root_key, [])
            
            # Ensure data is a list
            if isinstance(data, dict):
                data = [data]
            
            # Expand arrays to rows
            df = FileConverter.expand_arrays_to_rows(data, array_columns)

            if max_rows:
                df = df.head(max_rows)
            
            df.to_csv(csv_path, index=False)
            logger.info(f"Successfully converted {json_path} to {csv_path} with expanded arrays")
        except Exception as e:
            logger.error(f"Error converting JSON with array expansion: {e}")
            raise
    
    @staticmethod
    def json_to_excel_expanded(json_path: Union[str, Path], excel_path: Union[str, Path], array_columns: List[str], root_key: Optional[str]=None, sheet_name: str='Sheet1', max_rows: Optional[int]=None) -> None:
        """ Convert JSON to Excel with expanded arrays.
        Args:
            json_path (Union[str, Path]): Path to the input JSON file.
            excel_path (Union[str, Path]): Path to the output Excel file.
            array_columns (List[str]): List of column paths that contain arrays to expand.
            root_key (Optional[str]): Root key to extract data from if JSON is nested.
            sheet_name (str): Name of the Excel sheet to write data to.
            max_rows (Optional[int]): Maximum number of rows to write to Excel.
        """
        json_path = Path(json_path)
        excel_path = Path(excel_path)

        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if root_key and isinstance(data, dict):
                data = data.get(root_key, [])
            
            if isinstance(data, dict):
                data = [data]
            
            # Expand arrays to rows
            df = FileConverter.expand_arrays_to_rows(data, array_columns)

            if max_rows:
                df = df.head(max_rows)
            
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            logger.info(f"Successfully converted {json_path} to {excel_path} with expanded arrays.")
        except Exception as e:
            logger.error(f"Error converting JSON to Excel with array expansions: {e}")
            raise

    @staticmethod
    def json_to_csv(json_path: Union[str, Path], 
                   csv_path: Union[str, Path],
                   root_key: Optional[str] = None,
                   max_rows: Optional[int] = None,
                   flatten: bool = True,
                   sep: str = '_',
                   max_level: Optional[int] = None,
                   compression: Optional[str] = None,
                   column_map: Optional[Dict[str, str]] = None,
                   schema: Optional[Dict] = None,
                   num_workers: int = 4,
                   chunk_size: int = 1000,
                   show_progress: bool = True,
                   handle_multiple_roots: bool = False) -> None:
        """ Convert JSON to CSV with optional flattening and parallel processing.
        Args:
            json_path (Union[str, Path]): Path to the input JSON file.
            csv_path (Union[str, Path]): Path to the output CSV file.
            root_key (Optional[str]): Root key to extract data from if JSON is nested.
            max_rows (Optional[int]): Maximum number of rows to write to CSV.
            flatten (bool): Whether to flatten nested structures.
            sep (str): Separator for flattening keys.
            max_level (Optional[int]): Maximum depth for flattening.
            compression (Optional[str]): Compression type ('gzip', 'bz2', 'lzma', None).
            column_map (Optional[Dict[str, str]]): Mapping of old column names to new names.
            schema (Optional[Dict]): JSON schema for validation.
            num_workers (int): Number of parallel workers for processing chunks.
            chunk_size (int): Size of each chunk for parallel processing.
            show_progress (bool): Whether to show progress bar during processing.
        """
        
        json_path = Path(json_path)
        csv_path = Path(csv_path)

        # Check for multiple root keys
        if handle_multiple_roots and root_key:
            logger.info(f"Handling multiple root keys in {json_path} with root key '{root_key}'")
            return _stream_json_to_csv(
                json_path, csv_path, root_key, max_rows,
                flatten, sep, compression, column_map,
                schema, show_progress
            )

        try:
            with FileConverter._get_file_handle(json_path, 'rb', compression) as f:
                data = json.load(f)

            if root_key and isinstance(data, dict):
                data = data.get(root_key, [])

            if isinstance(data, dict):
                data = [data]

            # Split data into chunks for parallel processing
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            if max_rows:
                chunks = chunks[:max_rows // chunk_size + 1]

            # Process chunks in parallel
            dfs = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
                process_func = partial(
                    FileConverter._process_chunk,
                    flatten=flatten,
                    sep=sep,
                    column_map=column_map,
                    schema=schema
                )
                
                if show_progress:
                    futures = list(tqdm(
                        executor.map(process_func, chunks),
                        total=len(chunks),
                        desc="Processing chunks"
                    ))
                else:
                    futures = list(executor.map(process_func, chunks))

                dfs.extend(futures)

            # Combine results
            df = pd.concat(dfs, ignore_index=True)
            if max_rows:
                df = df.head(max_rows)

            # Save to CSV
            df.to_csv(csv_path, index=False)
            logger.info(f"Successfully converted {json_path} to {csv_path}")

        except MemoryError:
            logger.info("File too large, using streaming approach...")
            _stream_json_to_csv(
                json_path, csv_path, root_key, max_rows,
                flatten, sep, compression, column_map,
                schema, show_progress
            )

    @staticmethod
    def json_to_excel(json_path: Union[str, Path], excel_path: Union[str, Path],
                     root_key: Optional[str] = None,
                     sheet_name: str = 'Sheet1',
                     max_rows: Optional[int] = None,
                     flatten: bool = True,
                     sep: str = '_',
                     compression: Optional[str] = None,
                     column_map: Optional[Dict[str, str]] = None,
                     schema: Optional[Dict] = None,
                     show_progress: bool = True,
                     handle_multiple_roots: bool = False) -> None:
        """ Convert JSON to Excel with optional flattening and parallel processing.
        Args:
            json_path (Union[str, Path]): Path to the input JSON file.
            excel_path (Union[str, Path]): Path to the output Excel file.
            root_key (Optional[str]): Root key to extract data from if JSON is nested.
            sheet_name (str): Name of the Excel sheet to write data to.
            max_rows (Optional[int]): Maximum number of rows to write to Excel.
            flatten (bool): Whether to flatten nested structures.
            sep (str): Separator for flattening keys.
            compression (Optional[str]): Compression type ('gzip', 'bz2', 'lzma', None).
            column_map (Optional[Dict[str, str]]): Mapping of old column names to new names.
            schema (Optional[Dict]): JSON schema for validation.
            show_progress (bool): Whether to show progress bar during processing.
        """
        json_path = Path(json_path)
        excel_path = Path(excel_path)

        # Check for multiple root keys
        if handle_multiple_roots and root_key:
            logger.info(f"Using multiple root key handling for key: '{root_key}'")
            all_data = FileConverter.parse_json_multiple_roots(json_path, root_key, compression)
            
            if not all_data:
                logger.warning(f"No data found for root key '{root_key}'")
                pd.DataFrame().to_excel(excel_path, index=False)
                return
            
            # Validate if schema provided
            if schema:
                for record in all_data:
                    FileConverter._validate_data(record, schema)
            
            # Process the data
            if flatten:
                processed_data = []
                for record in all_data:
                    if isinstance(record, dict):
                        flattened = FileConverter.flatten_json(record, sep=sep)
                        processed_data.append(flattened)
                    else:
                        processed_data.append({'value': record})
                all_data = processed_data
            
            # Convert to DataFrame
            df = pd.DataFrame(all_data)
            
            # Apply column mapping
            if column_map:
                df = FileConverter._map_columns(df, column_map)
            
            if max_rows:
                df = df.head(max_rows)
            
            # Save to Excel
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            logger.info(f"Successfully saved {len(df)} rows to {excel_path}")
            return

        try:
            with FileConverter._get_file_handle(json_path, 'r', compression) as f:
                data = json.load(f)

            if root_key and isinstance(data, dict):
                data = data.get(root_key, [])

            if isinstance(data, dict):
                data = [data]

            # Process data
            if schema:
                for record in data:
                    FileConverter._validate_data(record, schema)

            if flatten:
                processed = [FileConverter.flatten_json(record, sep=sep) for record in data]
            else:
                processed = data

            df = pd.DataFrame(processed)
            
            if column_map:
                df = FileConverter._map_columns(df, column_map)

            if max_rows:
                df = df.head(max_rows)

            # Save to Excel
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

            logger.info(f"Successfully converted {json_path} to {excel_path}")

        except MemoryError:
            logger.info("File too large, using streaming approach...")
            _stream_json_to_excel(
                json_path, excel_path, root_key, sheet_name,
                max_rows, flatten, sep
            )


    # New method to diagnose JSON root keys
    @staticmethod
    def diagnose_json_root_keys(json_path: Union[str, Path], compression: Optional[str]=None) -> Dict[str, int]:
        """Diagnose the root keys in a JSON file.
        Args:
            json_path (Union[str, Path]): Path to the JSON file.
            compression (Optional[str]): Compression type ('gzip', 'bz2', 'lzma', None).
        Returns:
            Dict[str, int]: A dictionary with root keys as keys and their counts as values.
        """
        json_path = Path(json_path)
        key_counts = {}

        with FileConverter._get_file_handle(json_path, 'rb', compression) as f:
            parser = ijson.kvitems(f, '')

            for key, value in parser:
                key_counts[key] = key_counts.get(key, 0) + 1

        return key_counts


    @staticmethod
    def jsonlines_to_csv(jsonl_path: Union[str, Path], csv_path: Union[str, Path], max_rows: Optional[int]=None) -> None:
        """ Convert JSONL (JSON Lines) to CSV.
        Args:
            jsonl_path (Union[str, Path]): Path to the input JSONL file.
            csv_path (Union[str, Path]): Path to the output CSV file.
            max_rows (Optional[int]): Maximum number of rows to write to CSV.
        """
        jsonl_path = Path(jsonl_path)
        csv_path = Path(csv_path)
        records = []

        try:
            with open(jsonl_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if max_rows and i >= max_rows:
                        break
                    if line.strip():
                        records.append(json.loads(line))
            df = pd.DataFrame(records)
            df.to_csv(csv_path, index=False)
            logger.info(f"Converted {len(records)} records from {jsonl_path} to {csv_path}")
        except Exception as e:
            logger.error(f"Error converting JSONL to CSV: {e}")
            raise
    
    @staticmethod
    def xml_to_csv(xml_path: Union[str, Path], csv_path: Union[str, Path], record_tag: str='record', max_rows: Optional[int]=None) -> None:
        """ Convert XML to CSV.
        Args:
            xml_path (Union[str, Path]): Path to the input XML file.
            csv_path (Union[str, Path]): Path to the output CSV file.
            record_tag (str): XML tag for each record to extract.
            max_rows (Optional[int]): Maximum number of records to write to CSV.
        """
        xml_path = Path(xml_path)
        csv_path = Path(csv_path)

        try:
            # Parse XML and extract records
            tree = ET.parse(xml_path)
            root = tree.getroot()

            records = []
            for i, record in enumerate(root.findall(f'.//{record_tag}')):
                if max_rows and i >= max_rows:
                    break

                record_dict = {}
                # Extract text from alll child elements
                for child in record.iter():
                    if child.text and child.text.strip():
                        record_dict[child.tag] = child.text.strip()

                # Include attributes
                for attr_name, attr_value in record.attrib.items():
                    record_dict[f'@{attr_name}'] = attr_value
                
                records.append(record_dict)
            
            # Convert to DataFrame and save
            df = pd.DataFrame(records)
            df.to_csv(csv_path, index=False)
            logger.info(f"File successfully converted {xml_path} to {csv_path}")
        
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error converting XML to CSV: {e}")
            raise
    
    @staticmethod
    def xml_to_excel(xml_path: Union[str, Path], excel_path: Union[str, Path], record_tag: str='record', sheet_name: str='Sheet1', max_rows: Optional[int]=None) -> None:
        """ Convert XML to Excel.
        Args:
            xml_path (Union[str, Path]): Path to the input XML file.
            excel_path (Union[str, Path]): Path to the output Excel file.
            record_tag (str): XML tag for each record to extract.
            sheet_name (str): Name of the Excel sheet to write data to.
            max_rows (Optional[int]): Maximum number of records to write to Excel.
        """
        xml_path = Path(xml_path)
        excel_path = Path(excel_path)

        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()

            records = []

            for i, record in enumerate(root.findall(f'.//{record_tag}')):
                if max_rows and i >= max_rows:
                    break

                record_dict = {}
                for child in record.iter():
                    if child.text and child.text.strip():
                        record_dict[child.tag] = child.text.strip()

                for attr_name, attr_value in record.attrib.items():
                    record_dict[f'@{attr_name}'] = attr_value
                
                records.append(record_dict)
            
            df = pd.DataFrame(records)

            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            logger.info(f"File is successfully converted {xml_path} to {excel_path}")
        
        except Exception as e:
            logger.error(f"Error converting XML to Excel: {e}")
            raise
        
    @staticmethod
    def extract_fields_from_xml_element(element, ns, field_map):
        """ Extract fields from an XML element based on a field map.
        Args:
            element (ET.Element): The XML element to extract data from.
            ns (Dict[str, str]): Namespace mapping for XML parsing.
            field_map (Dict[str, Tuple[str, str, Any]]): Mapping of field names to (XPath, attribute, default value).
        Returns:
            Dict[str, Any]: A dictionary containing the extracted fields.
        """
        data = {}
        for key, (xpath, attr, default) in field_map.items():
            found = element.find(xpath, ns)
            if found is not None:
                if attr:
                    data[key] = found.get(attr, default)
                else:
                    data[key] = found.text if found.text is not None else default
            else:
                data[key] = default
        return data
    
    @staticmethod
    def extract_text_data_dynamic(section, ns, field_map):
        """ Extract text data from an XML section based on a field map.
        Args:
            section (ET.Element): The XML section to extract data from.
            ns (Dict[str, str]): Namespace mapping for XML parsing.
            field_map (Dict[str, Tuple[str, str, Any]]): Mapping of field names to (XPath, attribute, default value).
        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing the extracted text data.
        """
        entries = section.findall('.//hl7:entry', ns)
        text_data = []
        for entry in entries:
            act = entry.find('.//hl7:act', ns)
            if act is not None:
                data = FileConverter.extract_fields_from_xml_element(act, ns, field_map)
                text_data.append(data)
        return text_data
    
    @staticmethod
    def extract_table_data_dynamic(section, ns, file_name_without_extension):
        """ Extract table data from an XML section.
        Args:
            section (ET.Element): The XML section to extract table data from.
            ns (Dict[str, str]): Namespace mapping for XML parsing.
            file_name_without_extension (str): Name of the file without extension to add as a column.
        Returns:
            Tuple[List[str], List[List[str]]]: A tuple containing headers and table data.
        """
        tables = section.findall('.//hl7:table', ns)
        table_data = []
        headers = []
        if tables:
            for table in tables:
                table_headers = table.findall('.//hl7:thead/hl7:tr/hl7:th', ns)
                headers = [header.text for header in table_headers]
                headers.append('FileName')  # Add FileName column
                table_rows = table.findall('.//hl7:tbody/hl7:tr', ns)
                for row in table_rows:
                    if row.find('hl7:th', ns) is not None:
                        continue
                    row_data = []
                    for col in row.findall('hl7:td', ns):
                        content = col.find('hl7:content', ns)
                        if content is not None:
                            text = content.text.replace('\n', ' ').strip() if content.text else ''
                            row_data.append(text)
                        else:
                            text = col.text.replace('\n', ' ').strip() if col.text else ''
                            row_data.append(text)
                    row_data.append(file_name_without_extension)  # Add file name to each row
                    table_data.append(row_data)
        return headers, table_data
    
    @staticmethod
    def parse_json_all_root_keys(json_path: Path, 
                                root_keys: Optional[List[str]] = None,
                                compression: Optional[str] = None) -> Dict[str, List[Any]]:
        """
        Parse JSON file and extract data from multiple different root keys.
        Handles both top-level objects and arrays.
        """
        all_data = {}

        with FileConverter._get_file_handle(json_path, 'rb', compression) as f:
            # Peek at the first non-whitespace character
            first_char = f.read(1)
            while first_char in (b' ', b'\n', b'\r', b'\t'):
                first_char = f.read(1)
            f.seek(0)
            if first_char == b'[' or first_char == '[':
                # Top-level array: iterate each object, collect keys
                for obj in ijson.items(f, 'item'):
                    for key, value in obj.items():
                        if root_keys and key not in root_keys:
                            continue
                        if key not in all_data:
                            all_data[key] = []
                        if isinstance(value, list):
                            all_data[key].extend(value)
                        else:
                            all_data[key].append(value)
            else:
                # Top-level object: use kvitems as before
                parser = ijson.kvitems(f, '')
                for key, value in parser:
                    if root_keys and key not in root_keys:
                        continue
                    if key not in all_data:
                        all_data[key] = []
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
                    
        except MemoryError:
            logger.warning("Memory error encountered, switching to streaming mode")
            return FileConverter.stream_json_multi_keys_to_csv(
                json_path, csv_path, root_keys, combine_keys,
                add_source_column, max_rows, flatten, sep,
                compression, column_map
            )
        

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

# def _stream_json_to_csv(json_path: Path, csv_path: Path,
#                         root_key: Optional[str] = None,
#                         max_rows: Optional[int] = None,
#                         flatten: bool = True,
#                         sep: str = '_',
#                         compression: Optional[str] = None,
#                         column_map: Optional[Dict[str, str]] = None,
#                         schema: Optional[Dict] = None,
#                         show_progress: bool = True) -> None:
#     """
#     Stream large JSON files to CSV
#     Args:
#         json_path (Path): Path to the input JSON file.
#         csv_path (Path): Path to the output CSV file.
#         root_key (Optional[str]): Root key to extract data from if JSON is nested.
#         max_rows (Optional[int]): Maximum number of rows to write to CSV.
#         flatten (bool): Whether to flatten nested structures.
#         sep (str): Separator for flattening keys.
#         compression (Optional[str]): Compression type ('gzip', 'bz2', 'lzma', None).
#         column_map (Optional[Dict[str, str]]): Mapping of old column names to new names.
#         schema (Optional[Dict]): JSON schema for validation.
#         show_progress (bool): Whether to show progress bar during processing.
#     """
#     with FileConverter._get_file_handle(json_path, 'rb', compression) as json_file:
#         if root_key:
#             parser = ijson.items(json_file, f'{root_key}.item')
#         else:
#             parser = ijson.items(json_file, 'item')

#         first_row = True
#         rows_written = 0
#         all_headers = set()

#         with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
#             writer = None
#             batch = []
#             batch_size = 1000

#             for record in parser:
#                 if max_rows and rows_written >= max_rows:
#                     break

#                 # Flatten the record if needed
#                 if flatten and isinstance(record, dict):
#                     record = FileConverter.flatten_json(record, sep=sep)
#                 elif not isinstance(record, dict):
#                     record = {'value': record}

#                 batch.append(record)
#                 all_headers.update(record.keys())

#                 if len(batch) >= batch_size or (max_rows and rows_written + len(batch) >= max_rows):
#                     # Write batch
#                     if first_row:
#                         headers = sorted(list(all_headers))
#                         writer = csv.DictWriter(csv_file, fieldnames=headers, extrasaction='ignore')
#                         writer.writeheader()
#                         first_row = False

#                     if writer:
#                         for row in batch:
#                             writer.writerow(row)
#                             rows_written += 1

#                     batch = []

#             # Write remaining records
#             if batch and writer:
#                 for row in batch:
#                     writer.writerow(row)
#                     rows_written += 1

#         logger.info(f"Streamed {rows_written} rows to {csv_path}")


def _stream_json_to_csv(json_path: Path, csv_path: Path,
                        root_key: Optional[str] = None,
                        max_rows: Optional[int] = None,
                        flatten: bool = True,
                        sep: str = '_',
                        compression: Optional[str] = None,
                        column_map: Optional[Dict[str, str]] = None,
                        schema: Optional[Dict] = None,
                        show_progress: bool = True,
                        metadata: Optional[Dict[str, Any]] = None) -> None:
    """
    Stream large JSON files to CSV, handling both object and array top-level JSON.
    """
    with FileConverter._get_file_handle(json_path, 'rb', compression) as json_file:
        first_row = True
        rows_written = 0
        all_headers = set()
        batch = []
        batch_size = 1000

        with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = None

            # Try to detect if the file is an array or an object
            first_char = json_file.read(1)
            json_file.seek(0)
            if first_char == b'[' or first_char == '[':
                # Top-level array: stream each object, extract root_key from each
                for obj in ijson.items(json_file, 'item'):
                    parent_metadata = {k: v for k, v in obj.items() if k != root_key}
                    if metadata:
                        parent_metadata = {**metadata, **parent_metadata}
                    records = obj.get(root_key, [])
                    for record in records:
                        if max_rows and rows_written >= max_rows:
                            break
                        record = {**parent_metadata, **record}
                        if flatten and isinstance(record, dict):
                            record = FileConverter.flatten_json(record, sep=sep)
                        elif not isinstance(record, dict):
                            record = {'value': record}
                        batch.append(record)
                        all_headers.update(record.keys())
                        if len(batch) >= batch_size:
                            if first_row:
                                meta_cols = list(parent_metadata.keys()) if parent_metadata else []
                                other_cols = [col for col in sorted(all_headers) if col not in meta_cols]
                                headers = meta_cols + other_cols
                                writer = csv.DictWriter(csv_file, fieldnames=headers, extrasaction='ignore')
                                writer.writeheader()
                                first_row = False
                            for row in batch:
                                writer.writerow(row)
                                rows_written += 1
                            batch = []
            else:
                # Top-level object: use kvitems as before
                for parent_key, value in ijson.kvitems(json_file, ''):
                    if parent_key == root_key and isinstance(value, list):
                        for record in value:
                            if max_rows and rows_written >= max_rows:
                                break
                            if metadata:
                                record = {**metadata, **record}
                            if flatten and isinstance(record, dict):
                                record = FileConverter.flatten_json(record, sep=sep)
                            elif not isinstance(record, dict):
                                record = {'value': record}
                            batch.append(record)
                            all_headers.update(record.keys())
                            if len(batch) >= batch_size:
                                if first_row:
                                    meta_cols = list(metadata.keys()) if metadata else []
                                    other_cols = [col for col in sorted(all_headers) if col not in meta_cols]
                                    headers = meta_cols + other_cols
                                    writer = csv.DictWriter(csv_file, fieldnames=headers, extrasaction='ignore')
                                    writer.writeheader()
                                    first_row = False
                                for row in batch:
                                    writer.writerow(row)
                                    rows_written += 1
                                batch = []

            # Write any remaining records
            if batch and writer:
                for row in batch:
                    writer.writerow(row)
                    rows_written += 1

    logger.info(f"Streamed {rows_written} rows to {csv_path}")
    

def _stream_json_to_excel(json_path: Path, excel_path: Path,
                          root_key: Optional[str] = None,
                          sheet_name: str = 'Sheet1',
                          max_rows: Optional[int] = None,
                          flatten: bool = True,
                          sep: str = '_') -> None:
    """
    Stream large JSON files to Excel
    Args:
        json_path (Path): Path to the input JSON file.
        excel_path (Path): Path to the output Excel file.
        root_key (Optional[str]): Root key to extract data from if JSON is nested.
        sheet_name (str): Name of the Excel sheet to write data to.
        max_rows (Optional[int]): Maximum number of rows to write to Excel.
        flatten (bool): Whether to flatten nested structures.
        sep (str): Separator for flattening keys.
    """
    chunks = []
    chunk_size = 10000  # Process in chunks

    with open(json_path, 'rb') as json_file:
        if root_key:
            parser = ijson.items(json_file, f'{root_key}.item')
        else:
            parser = ijson.items(json_file, 'item')

        current_chunk = []
        total_rows = 0

        for record in parser:
            if max_rows and total_rows >= max_rows:
                break

            # Flatten the record if needed
            if flatten and isinstance(record, dict):
                record = FileConverter.flatten_json(record, sep=sep)
            elif not isinstance(record, dict):
                record = {'value': record}

            current_chunk.append(record)
            total_rows += 1

            if len(current_chunk) >= chunk_size:
                chunks.append(pd.DataFrame(current_chunk))
                current_chunk = []

        if current_chunk:
            chunks.append(pd.DataFrame(current_chunk))

    # Combine chunks and save to Excel
    if chunks:
        df = pd.concat(chunks, ignore_index=True)
        if max_rows:
            df = df.head(max_rows)

        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        logger.info(f"Streamed {len(df)} rows to {excel_path}")


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


def json_to_csv(json_path: Union[str, Path], csv_path: Union[str, Path], **kwargs) -> None:
    """Convert JSON to CSV. See FileConverter.json_to_csv for parameters."""
    converter = FileConverter()
    converter.json_to_csv(json_path, csv_path, **kwargs)

def json_to_excel(json_path: Union[str, Path], excel_path: Union[str, Path], **kwargs) -> None:
    """Convert JSON to Excel. See FileConverter.json_to_excel for parameters."""
    converter = FileConverter()
    converter.json_to_excel(json_path, excel_path, **kwargs)

def jsonlines_to_csv(jsonl_path: Union[str, Path], csv_path: Union[str, Path], **kwargs) -> None:
    converter = FileConverter()
    converter.jsonlines_to_csv(jsonl_path, csv_path, **kwargs)

def xml_to_csv(xml_path: Union[str, Path], csv_path: Union[str, Path], **kwargs) -> None:
    """Convert XML to CSV. See FileConverter.xml_to_csv for parameters."""
    converter = FileConverter()
    converter.xml_to_csv(xml_path, csv_path, **kwargs)

def xml_to_excel(xml_path: Union[str, Path], excel_path: Union[str, Path], **kwargs) -> None:
    """Convert XML to Excel. See FileConverter.xml_to_excel for parameters."""
    converter = FileConverter()
    converter.xml_to_excel(xml_path, excel_path, **kwargs)

def json_to_csv_expanded(json_path: Union[str, Path], csv_path: Union[str, Path], arrays_columns: List[str], **kwargs) -> None:
    """Convert JSON to CSV with expanded arrays. See FileConverter.json_to_csv_expanded for parameters."""
    converter = FileConverter()
    converter.json_to_csv_expanded(json_path, csv_path, arrays_columns, **kwargs)

def json_to_excel_expanded(json_path: Union[str, Path], excel_path: Union[str, Path], arrays_columns: List[str], **kwargs) -> None:
    """Convert JSON to Excel with expanded arrays. See FileConverter.json_to_excel_expanded for parameters."""
    converter = FileConverter()
    converter.json_to_excel_expanded(json_path, excel_path, arrays_columns, **kwargs)

# Extract text data from XML section using dynamic field mapping
def extract_text_data_dynamic(section, ns, field_map):
    """ Extract text data from an XML section using dynamic field mapping."""
    converter = FileConverter()
    return converter.extract_text_data_dynamic(section, ns, field_map)

def extract_table_data_dynamic(section, ns, file_name_without_extension):
    """ Extract table data from an XML section dynamically."""
    converter = FileConverter()
    return converter.extract_table_data_dynamic(section, ns, file_name_without_extension)

def stream_json_multi_keys_to_csv(json_path: Union[str, Path], csv_path: Union[str, Path], **kwargs) -> None:
    """Stream large JSON files with multiple keys to CSV"""
    converter = FileConverter()
    converter.stream_json_multi_keys_to_csv(json_path, csv_path, **kwargs)

def stream_json_multi_keys_to_excel(json_path: Union[str, Path], excel_path: Union[str, Path], **kwargs) -> None:
    """Stream large JSON files with multiple keys to Excel"""
    converter = FileConverter()
    converter.stream_json_multi_keys_to_excel(json_path, excel_path, **kwargs)


# Diagnose JSON root keys
def diagnose_json_root_keys(json_path: Union[str, Path], **kwargs) -> None:
    """Print the root keys in a JSON file."""
    counts = FileConverter.diagnose_json_root_keys(json_path, **kwargs)
    print(f"\nRoot keys in {json_path}:")
    for key, count in sorted(counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{key}: {count} occurrences")


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


if __name__ == "__main__":
    print("File converter module ready for use.!")
