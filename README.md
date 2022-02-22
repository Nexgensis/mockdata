# mockdata
The repo is used for test data creation

Steps for data generation using mockdata

1. Create rule json like sample.json given inside test folder
2. Run datageneration command as follows

command : python3 <path to synthetic_data_generator.py> <path to sample.json> <log_number>

ex: python synthetic_data_generator.py /home/pgnosi/Desktop/Dec2021/Aegis/Aegis_testing/test2/sample.json 3

=> After successful execution of above command , data would be generated and would be saved inside csv file at
location mentioned inside input json file.
