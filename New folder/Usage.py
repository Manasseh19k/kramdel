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
