# 1. First, peek at the structure
from file_converter import peek_json_file
peek_json_file(r"G:/MIS/DOWN/Frank/Castlight MRF/MRF 20250610/professional/5233/5233_src_flatfile_split_splitfiles_rcd00_innprof.json")

# 2. Extract specific billing codes
extract_by_value(
    r"G:/MIS/DOWN/Frank/Castlight MRF/MRF 20250610/professional/5233/5233_src_flatfile_split_splitfiles_rcd00_innprof.json",
    'specific_codes.csv',
    root_key='in_network',  # Common in MRF files
    filter_field='billing_code',
    filter_value='99213',  # Example CPT code
    filter_operator='equals',
    extract_fields=['billing_code', 'negotiated_rate', 'provider_references']
)

# 3. Extract all rates above a certain threshold
extract_by_value(
    r"G:/MIS/DOWN/Frank/Castlight MRF/MRF 20250610/professional/5233/5233_src_flatfile_split_splitfiles_rcd00_innprof.json",
    'high_cost_items.csv',
    filter_field='negotiated_rate',
    filter_value=1000,
    filter_operator='greater'
)

# 4. Extract specific provider group
extract_by_conditions(
    r"G:/MIS/DOWN/Frank/Castlight MRF/MRF 20250610/professional/5233/5233_src_flatfile_split_splitfiles_rcd00_innprof.json",
    'provider_group.csv',
    conditions=[
        {'field': 'provider_group_id', 'value': '5233', 'operator': 'equals'},
        {'field': 'billing_code_type', 'value': 'CPT', 'operator': 'equals'}
    ],
    logic='AND'
)

from selective_json_extractor import extract_nested_items

# Extract items from nested arrays with parent context
extract_nested_items(
    'mrf_file.json',
    'all_rates.csv',
    root_key='in_network',
    array_path='negotiated_rates',
    parent_fields=['billing_code', 'billing_code_type', 'name'],
    item_filter={'field': 'negotiated_rate', 'value': 0, 'operator': 'greater'}
)

# Extract data for specific providers
extract_by_value(
    'mrf_file.json',
    'provider_data.csv',
    filter_field='provider_name',
    filter_value='ACME Hospital',
    filter_operator='contains',
    extract_fields=['provider_name', 'billing_code', 'negotiated_rate', 'service_code']
)


# New for the large file

import file_converter

# 1. First, analyze the file structure
file_converter.analyze_json_structure(
    r"G:/MIS/DOWN/Frank/Castlight MRF/MRF 20250610/professional/5233/5233_src_flatfile_split_splitfiles_rcd00_innprof.json"
)

# 2. Peek at the file to understand its structure
file_converter.peek_json_file(
    r"G:/MIS/DOWN/Frank/Castlight MRF/MRF 20250610/professional/5233/5233_src_flatfile_split_splitfiles_rcd00_innprof.json",
    lines=20
)

# 3. Use ultra-low memory streaming
file_converter.stream_json_ultra_low_memory(
    r"G:/MIS/DOWN/Frank/Castlight MRF/MRF 20250610/professional/5233/5233_src_flatfile_split_splitfiles_rcd00_innprof.json",
    '5233_output.csv',
    sep='__',
    combine_keys=True,
    add_source_column=True,
    batch_size=50  # Very small batch size for low memory
)

# 4. If it still fails, try with even smaller batch
file_converter.stream_json_ultra_low_memory(
    r"G:/MIS/DOWN/Frank/Castlight MRF/MRF 20250610/professional/5233/5233_src_flatfile_split_splitfiles_rcd00_innprof.json",
    '5233_output.csv',
    sep='__',
    batch_size=10  # Extremely small batch
)
