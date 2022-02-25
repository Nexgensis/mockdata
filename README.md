# mockdata
The repo is used for test data creation

Steps for generating data:

1. Create a rule json file. ex: sample.json given inside test folder.
2. Execute synthetic_data_generator.py as follows

command : python3 <path to synthetic_data_generator.py> <path to json file> <log_number>

ex: python synthetic_data_generator.py /home/pgnosi/Desktop/Dec2021/Aegis/Aegis_testing/test2/sample.json 3

=> After successful execution of above data generator csv file would be generated inside location which was mentioned inside sample.json file.
