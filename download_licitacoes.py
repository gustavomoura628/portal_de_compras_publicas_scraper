import sys

json_file_path = sys.argv[1]
print("Loading file",json_file_path)

import json

with open(json_file_path) as json_file:
    data = json.load(json_file)

print("Table Name:",data["table"])
table_size = len(data["rows"])
print("Table Size:",table_size)
print(data["rows"][0])
