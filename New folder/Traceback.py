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
