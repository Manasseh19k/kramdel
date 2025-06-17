Traceback (most recent call last):
  File "c:\Users\rsa21305\Desktop\file_converter\example_usage.py", line 75, in <module>
    file_converter.stream_json_multi_keys_to_csv(
  File "c:\Users\rsa21305\Desktop\file_converter\file_converter.py", line 1880, in stream_json_multi_keys_to_csv
    converter.stream_json_multi_keys_to_csv(json_path, csv_path, **kwargs)
  File "c:\Users\rsa21305\Desktop\file_converter\file_converter.py", line 1209, in stream_json_multi_keys_to_csv
    _stream_combined_keys_to_csv(
  File "c:\Users\rsa21305\Desktop\file_converter\file_converter.py", line 1530, in _stream_combined_keys_to_csv
    for key, value in parser:
  File "C:\Python39\lib\site-packages\ijson\utils.py", line 55, in coros2gen
    f.send(value)
  File "C:\Python39\lib\site-packages\ijson\backends\python.py", line 44, in utf8_encoder
    send(sdata)
  File "C:\Python39\lib\site-packages\ijson\backends\python.py", line 44, in utf8_encoder
  File "C:\Python39\lib\site-packages\ijson\backends\python.py", line 44, in utf8_encoder
    send(sdata)
  File "C:\Python39\lib\site-packages\ijson\backends\python.py", line 100, in Lexer
    buf += data
MemoryError



Traceback (most recent call last):
  File "c:\Users\rsa21305\Desktop\file_converter\example_usage.py", line 75, in <module>
    file_converter.json_to_csv_multi_keys(
  File "c:\Users\rsa21305\Desktop\file_converter\file_converter.py", line 1900, in json_to_csv_multi_keys
    converter.json_to_csv_multi_keys(json_path, csv_path, **kwargs)
  File "c:\Users\rsa21305\Desktop\file_converter\file_converter.py", line 857, in json_to_csv_multi_keys
    if use_streaming is None:
UnboundLocalError: local variable 'use_streaming' referenced before assignment



Traceback (most recent call last):
  File "c:\Users\rsa21305\Desktop\file_converter\example_usage.py", line 75, in <module>
    file_converter.stream_json_multi_keys_to_csv(
  File "c:\Users\rsa21305\Desktop\file_converter\file_converter.py", line 1880, in stream_json_multi_keys_to_csv
    converter.stream_json_multi_keys_to_csv(json_path, csv_path, **kwargs)
  File "c:\Users\rsa21305\Desktop\file_converter\file_converter.py", line 1209, in stream_json_multi_keys_to_csv
    _stream_combined_keys_to_csv(
  File "c:\Users\rsa21305\Desktop\file_converter\file_converter.py", line 1530, in _stream_combined_keys_to_csv
    for key, value in parser:
  File "C:\Python39\lib\site-packages\ijson\utils.py", line 53, in coros2gen
    for value in source:
  File "C:\Python39\lib\site-packages\ijson\common.py", line 220, in file_source
    data = f.read(buf_size)
OSError: [Errno 22] Invalid argument
Exception ignored in: <generator object utf8_encoder at 0x0000020F7FA094A0>
Traceback (most recent call last):
  File "C:\Python39\lib\site-packages\ijson\backends\python.py", line 46, in utf8_encoder
    target.close()
  File "C:\Python39\lib\site-packages\ijson\backends\python.py", line 88, in Lexer
    raise common.IncompleteJSONError('Incomplete string lexeme')
ijson.common.IncompleteJSONError: Incomplete string lexeme



if __name__ == "__main__":
    file_converter.stream_json_multi_keys_to_csv(
         r"G:/MIS/DOWN/Frank/Castlight MRF/MRF 20250610/professional/5233/5233_src_flatfile_split_splitfiles_rcd00_innprof.json",
        '5233_5233_src_flatfile_split_splitfiles_rcd00_innprof.csv',
         sep='__',
         combine_keys=True,
         add_source_column=True,
         batch_size=1000  # Adjust batch size as needed
     )
