import file_converter

# 1. First, diagnose your JSON file to see root key occurrences
file_converter.diagnose_json_root_keys('your_file.json')

# 2. Convert with multiple root key support
file_converter.json_to_csv(
    'your_file.json', 
    'output.csv', 
    root_key='data',
    handle_multiple_roots=True  # This is the new parameter!
)

# 3. Same for Excel
file_converter.json_to_excel(
    'your_file.json',
    'output.xlsx',
    root_key='data',
    handle_multiple_roots=True
)
