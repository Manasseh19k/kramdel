"""
Selective JSON Data Extractor
Extract specific data from large JSON files based on filters and conditions
"""

import json
import csv
import pandas as pd
from pathlib import Path
from typing import Union, List, Dict, Any, Optional, Callable
import ijson
import logging
import re
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SelectiveJSONExtractor:
    """Extract specific data from JSON files based on various criteria"""
    
    @staticmethod
    def extract_by_field_value(json_path: Union[str, Path], 
                              csv_path: Union[str, Path],
                              root_key: Optional[str] = None,
                              filter_field: str = None,
                              filter_value: Any = None,
                              filter_operator: str = 'equals',
                              extract_fields: Optional[List[str]] = None,
                              flatten: bool = True,
                              sep: str = '_',
                              batch_size: int = 1000) -> None:
        """
        Extract records where a specific field matches a value
        
        Args:
            json_path: Path to JSON file
            csv_path: Output CSV path
            root_key: Root key to search within
            filter_field: Field name to filter on
            filter_value: Value to match
            filter_operator: 'equals', 'contains', 'greater', 'less', 'regex'
            extract_fields: List of fields to extract (None = all fields)
            flatten: Whether to flatten nested structures
            sep: Separator for flattening
            batch_size: Processing batch size
        """
        json_path = Path(json_path)
        csv_path = Path(csv_path)
        
        rows_written = 0
        batch = []
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = None
            
            with open(json_path, 'rb') as json_file:
                # Parse JSON stream
                if root_key:
                    parser = ijson.items(json_file, f'{root_key}.item')
                else:
                    parser = ijson.items(json_file, 'item')
                
                for record in parser:
                    # Check if record matches filter
                    if filter_field and not SelectiveJSONExtractor._matches_filter(
                        record, filter_field, filter_value, filter_operator
                    ):
                        continue
                    
                    # Extract only specified fields if requested
                    if extract_fields:
                        record = SelectiveJSONExtractor._extract_specific_fields(
                            record, extract_fields
                        )
                    
                    # Flatten if needed
                    if flatten and isinstance(record, dict):
                        from file_converter import FileConverter
                        record = FileConverter.flatten_json(record, sep=sep)
                    
                    batch.append(record)
                    
                    # Write batch
                    if len(batch) >= batch_size:
                        if writer is None:
                            headers = list(batch[0].keys()) if batch else []
                            writer = csv.DictWriter(csv_file, fieldnames=headers)
                            writer.writeheader()
                        
                        writer.writerows(batch)
                        rows_written += len(batch)
                        batch = []
                        
                        if rows_written % 10000 == 0:
                            logger.info(f"Processed {rows_written} matching records...")
                
                # Write remaining batch
                if batch:
                    if writer is None:
                        headers = list(batch[0].keys()) if batch else []
                        writer = csv.DictWriter(csv_file, fieldnames=headers)
                        writer.writeheader()
                    
                    writer.writerows(batch)
                    rows_written += len(batch)
        
        logger.info(f"Extracted {rows_written} matching records to {csv_path}")
    
    @staticmethod
    def extract_by_multiple_conditions(json_path: Union[str, Path], 
                                     csv_path: Union[str, Path],
                                     root_key: Optional[str] = None,
                                     conditions: List[Dict[str, Any]] = None,
                                     logic: str = 'AND',
                                     extract_fields: Optional[List[str]] = None,
                                     flatten: bool = True,
                                     sep: str = '_',
                                     batch_size: int = 1000) -> None:
        """
        Extract records matching multiple conditions
        
        Args:
            json_path: Path to JSON file
            csv_path: Output CSV path
            root_key: Root key to search within
            conditions: List of condition dicts with 'field', 'value', 'operator'
            logic: 'AND' or 'OR' for combining conditions
            extract_fields: Fields to extract
            flatten: Whether to flatten
            sep: Separator for flattening
            batch_size: Batch size
            
        Example conditions:
        [
            {'field': 'price', 'value': 100, 'operator': 'greater'},
            {'field': 'category', 'value': 'electronics', 'operator': 'equals'}
        ]
        """
        json_path = Path(json_path)
        csv_path = Path(csv_path)
        
        rows_written = 0
        batch = []
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = None
            
            with open(json_path, 'rb') as json_file:
                if root_key:
                    parser = ijson.items(json_file, f'{root_key}.item')
                else:
                    parser = ijson.items(json_file, 'item')
                
                for record in parser:
                    # Check multiple conditions
                    if conditions:
                        matches = []
                        for condition in conditions:
                            match = SelectiveJSONExtractor._matches_filter(
                                record,
                                condition.get('field'),
                                condition.get('value'),
                                condition.get('operator', 'equals')
                            )
                            matches.append(match)
                        
                        # Apply logic
                        if logic == 'AND' and not all(matches):
                            continue
                        elif logic == 'OR' and not any(matches):
                            continue
                    
                    # Extract specific fields
                    if extract_fields:
                        record = SelectiveJSONExtractor._extract_specific_fields(
                            record, extract_fields
                        )
                    
                    # Flatten if needed
                    if flatten and isinstance(record, dict):
                        from file_converter import FileConverter
                        record = FileConverter.flatten_json(record, sep=sep)
                    
                    batch.append(record)
                    
                    if len(batch) >= batch_size:
                        if writer is None:
                            headers = list(batch[0].keys()) if batch else []
                            writer = csv.DictWriter(csv_file, fieldnames=headers)
                            writer.writeheader()
                        
                        writer.writerows(batch)
                        rows_written += len(batch)
                        batch = []
                
                # Write remaining
                if batch:
                    if writer is None:
                        headers = list(batch[0].keys()) if batch else []
                        writer = csv.DictWriter(csv_file, fieldnames=headers)
                        writer.writeheader()
                    
                    writer.writerows(batch)
                    rows_written += len(batch)
        
        logger.info(f"Extracted {rows_written} records matching conditions to {csv_path}")
    
    @staticmethod
    def extract_by_custom_function(json_path: Union[str, Path], 
                                 csv_path: Union[str, Path],
                                 root_key: Optional[str] = None,
                                 filter_func: Callable[[Dict], bool] = None,
                                 transform_func: Callable[[Dict], Dict] = None,
                                 flatten: bool = True,
                                 sep: str = '_',
                                 batch_size: int = 1000) -> None:
        """
        Extract records using custom filter and transform functions
        
        Args:
            json_path: Path to JSON file
            csv_path: Output CSV path
            root_key: Root key to search within
            filter_func: Function that returns True for records to keep
            transform_func: Function to transform records before saving
            flatten: Whether to flatten
            sep: Separator
            batch_size: Batch size
            
        Example:
            filter_func = lambda record: record.get('price', 0) > 100
            transform_func = lambda record: {
                'id': record.get('id'),
                'price_with_tax': record.get('price', 0) * 1.1
            }
        """
        json_path = Path(json_path)
        csv_path = Path(csv_path)
        
        rows_written = 0
        batch = []
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = None
            
            with open(json_path, 'rb') as json_file:
                if root_key:
                    parser = ijson.items(json_file, f'{root_key}.item')
                else:
                    parser = ijson.items(json_file, 'item')
                
                for record in parser:
                    # Apply filter function
                    if filter_func and not filter_func(record):
                        continue
                    
                    # Apply transform function
                    if transform_func:
                        record = transform_func(record)
                    
                    # Flatten if needed
                    if flatten and isinstance(record, dict):
                        from file_converter import FileConverter
                        record = FileConverter.flatten_json(record, sep=sep)
                    
                    batch.append(record)
                    
                    if len(batch) >= batch_size:
                        if writer is None:
                            headers = list(batch[0].keys()) if batch else []
                            writer = csv.DictWriter(csv_file, fieldnames=headers)
                            writer.writeheader()
                        
                        writer.writerows(batch)
                        rows_written += len(batch)
                        batch = []
                
                if batch:
                    if writer is None:
                        headers = list(batch[0].keys()) if batch else []
                        writer = csv.DictWriter(csv_file, fieldnames=headers)
                        writer.writeheader()
                    
                    writer.writerows(batch)
                    rows_written += len(batch)
        
        logger.info(f"Extracted {rows_written} records using custom functions to {csv_path}")
    
    @staticmethod
    def extract_nested_array_items(json_path: Union[str, Path], 
                                 csv_path: Union[str, Path],
                                 root_key: Optional[str] = None,
                                 array_path: str = None,
                                 item_filter: Optional[Dict[str, Any]] = None,
                                 parent_fields: Optional[List[str]] = None,
                                 flatten: bool = True,
                                 sep: str = '_') -> None:
        """
        Extract items from nested arrays with optional parent context
        
        Args:
            json_path: Path to JSON file
            csv_path: Output CSV path
            root_key: Root key
            array_path: Path to array (e.g., 'orders.items')
            item_filter: Filter for array items
            parent_fields: Parent fields to include with each item
            flatten: Whether to flatten
            sep: Separator
            
        Example:
            # Extract all items from orders with order_id
            extract_nested_array_items(
                'data.json', 'items.csv',
                root_key='data',
                array_path='orders.items',
                parent_fields=['order_id', 'customer_name']
            )
        """
        json_path = Path(json_path)
        csv_path = Path(csv_path)
        
        rows_written = 0
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = None
            
            with open(json_path, 'rb') as json_file:
                if root_key:
                    parser = ijson.items(json_file, f'{root_key}.item')
                else:
                    parser = ijson.items(json_file, 'item')
                
                for record in parser:
                    # Extract parent fields
                    parent_data = {}
                    if parent_fields:
                        for field in parent_fields:
                            parent_data[field] = SelectiveJSONExtractor._get_nested_value(
                                record, field
                            )
                    
                    # Navigate to nested array
                    array_items = SelectiveJSONExtractor._get_nested_value(
                        record, array_path
                    )
                    
                    if isinstance(array_items, list):
                        for item in array_items:
                            # Apply item filter if specified
                            if item_filter:
                                if not SelectiveJSONExtractor._matches_filter(
                                    item,
                                    item_filter.get('field'),
                                    item_filter.get('value'),
                                    item_filter.get('operator', 'equals')
                                ):
                                    continue
                            
                            # Combine parent data with item
                            combined = {**parent_data, **item} if isinstance(item, dict) else {
                                **parent_data, 'value': item
                            }
                            
                            # Flatten if needed
                            if flatten:
                                from file_converter import FileConverter
                                combined = FileConverter.flatten_json(combined, sep=sep)
                            
                            # Write immediately for memory efficiency
                            if writer is None:
                                headers = list(combined.keys())
                                writer = csv.DictWriter(csv_file, fieldnames=headers)
                                writer.writeheader()
                            
                            writer.writerow(combined)
                            rows_written += 1
        
        logger.info(f"Extracted {rows_written} nested array items to {csv_path}")
    
    @staticmethod
    def extract_sample(json_path: Union[str, Path], 
                      csv_path: Union[str, Path],
                      root_key: Optional[str] = None,
                      sample_size: int = 1000,
                      sample_type: str = 'first',
                      extract_fields: Optional[List[str]] = None,
                      flatten: bool = True,
                      sep: str = '_') -> None:
        """
        Extract a sample of records for analysis
        
        Args:
            json_path: Path to JSON file
            csv_path: Output CSV path
            root_key: Root key
            sample_size: Number of records to sample
            sample_type: 'first', 'random', or 'stratified'
            extract_fields: Fields to extract
            flatten: Whether to flatten
            sep: Separator
        """
        import random
        
        json_path = Path(json_path)
        csv_path = Path(csv_path)
        
        if sample_type == 'first':
            # Simple first N records
            SelectiveJSONExtractor.extract_by_field_value(
                json_path, csv_path, root_key,
                filter_field=None,  # No filter, just limit
                extract_fields=extract_fields,
                flatten=flatten,
                sep=sep,
                batch_size=sample_size
            )
        elif sample_type == 'random':
            # Random sampling
            records = []
            total_records = 0
            
            # First pass: count total records
            with open(json_path, 'rb') as json_file:
                if root_key:
                    parser = ijson.items(json_file, f'{root_key}.item')
                else:
                    parser = ijson.items(json_file, 'item')
                
                for _ in parser:
                    total_records += 1
            
            # Calculate sampling probability
            sample_prob = min(1.0, sample_size / total_records)
            
            # Second pass: random sampling
            with open(json_path, 'rb') as json_file:
                if root_key:
                    parser = ijson.items(json_file, f'{root_key}.item')
                else:
                    parser = ijson.items(json_file, 'item')
                
                for record in parser:
                    if random.random() < sample_prob:
                        if extract_fields:
                            record = SelectiveJSONExtractor._extract_specific_fields(
                                record, extract_fields
                            )
                        
                        if flatten and isinstance(record, dict):
                            from file_converter import FileConverter
                            record = FileConverter.flatten_json(record, sep=sep)
                        
                        records.append(record)
                        
                        if len(records) >= sample_size:
                            break
            
            # Write sample
            if records:
                df = pd.DataFrame(records)
                df.to_csv(csv_path, index=False)
                logger.info(f"Sampled {len(df)} records to {csv_path}")
    
    @staticmethod
    def _matches_filter(record: Dict, field: str, value: Any, operator: str) -> bool:
        """Check if record matches filter criteria"""
        if not field:
            return True
        
        # Get nested field value
        field_value = SelectiveJSONExtractor._get_nested_value(record, field)
        
        if operator == 'equals':
            return field_value == value
        elif operator == 'contains':
            return str(value).lower() in str(field_value).lower()
        elif operator == 'greater':
            try:
                return float(field_value) > float(value)
            except:
                return False
        elif operator == 'less':
            try:
                return float(field_value) < float(value)
            except:
                return False
        elif operator == 'regex':
            try:
                return bool(re.match(value, str(field_value)))
            except:
                return False
        elif operator == 'exists':
            return field_value is not None
        elif operator == 'not_exists':
            return field_value is None
        elif operator == 'in':
            return field_value in value if isinstance(value, list) else False
        elif operator == 'not_in':
            return field_value not in value if isinstance(value, list) else True
        
        return False
    
    @staticmethod
    def _get_nested_value(record: Dict, path: str) -> Any:
        """Get value from nested path like 'user.address.city'"""
        if not path or not isinstance(record, dict):
            return None
        
        parts = path.split('.')
        value = record
        
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        
        return value
    
    @staticmethod
    def _extract_specific_fields(record: Dict, fields: List[str]) -> Dict:
        """Extract only specific fields from record"""
        if not isinstance(record, dict):
            return record
        
        result = {}
        for field in fields:
            value = SelectiveJSONExtractor._get_nested_value(record, field)
            if value is not None:
                # Handle nested field names in result
                if '.' in field:
                    # Use last part as key
                    key = field.split('.')[-1]
                    result[key] = value
                else:
                    result[field] = value
        
        return result


# Convenience functions
def extract_by_value(json_path: str, csv_path: str, **kwargs):
    """Extract records where a field matches a value"""
    extractor = SelectiveJSONExtractor()
    extractor.extract_by_field_value(json_path, csv_path, **kwargs)

def extract_by_conditions(json_path: str, csv_path: str, **kwargs):
    """Extract records matching multiple conditions"""
    extractor = SelectiveJSONExtractor()
    extractor.extract_by_multiple_conditions(json_path, csv_path, **kwargs)

def extract_custom(json_path: str, csv_path: str, **kwargs):
    """Extract using custom functions"""
    extractor = SelectiveJSONExtractor()
    extractor.extract_by_custom_function(json_path, csv_path, **kwargs)

def extract_nested_items(json_path: str, csv_path: str, **kwargs):
    """Extract items from nested arrays"""
    extractor = SelectiveJSONExtractor()
    extractor.extract_nested_array_items(json_path, csv_path, **kwargs)

def extract_sample(json_path: str, csv_path: str, **kwargs):
    """Extract a sample of records"""
    extractor = SelectiveJSONExtractor()
    extractor.extract_sample(json_path, csv_path, **kwargs)


# Example usage
if __name__ == "__main__":
    print("Selective JSON Data Extractor")
    print("\nExample usage:")
    
    examples = """
    # 1. Extract records where price > 100
    extract_by_value(
        'data.json', 'expensive_items.csv',
        filter_field='price',
        filter_value=100,
        filter_operator='greater'
    )
    
    # 2. Extract specific fields only
    extract_by_value(
        'data.json', 'product_summary.csv',
        extract_fields=['id', 'name', 'price', 'category']
    )
    
    # 3. Multiple conditions
    extract_by_conditions(
        'data.json', 'filtered.csv',
        conditions=[
            {'field': 'price', 'value': 50, 'operator': 'greater'},
            {'field': 'category', 'value': 'electronics', 'operator': 'equals'},
            {'field': 'in_stock', 'value': True, 'operator': 'equals'}
        ],
        logic='AND'
    )
    
    # 4. Custom filtering
    extract_custom(
        'data.json', 'custom.csv',
        filter_func=lambda r: r.get('price', 0) > 100 and 'premium' in r.get('tags', []),
        transform_func=lambda r: {
            'id': r.get('id'),
            'name': r.get('name'),
            'discounted_price': r.get('price', 0) * 0.9
        }
    )
    
    # 5. Extract nested array items
    extract_nested_items(
        'orders.json', 'order_items.csv',
        root_key='orders',
        array_path='items',
        parent_fields=['order_id', 'customer_name', 'order_date']
    )
    
    # 6. Get a sample for analysis
    extract_sample(
        'large_data.json', 'sample.csv',
        sample_size=1000,
        sample_type='random'
    )
    """
    print(examples)
