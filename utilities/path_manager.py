import os

dirname = os.path.dirname(__file__)
mockdata_path = str(os.path.abspath(os.path.join(dirname, os.pardir)))

print(f'project home={mockdata_path}')

DATE_FORMATS_JSON = str(mockdata_path) +'/resources/dateFormats.json'
AEGIS_PYTHON_LOGS = str(mockdata_path) + "/Aegis_log"

