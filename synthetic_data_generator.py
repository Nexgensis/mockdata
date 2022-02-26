"""
This python script is the begining of synthetic data generation
"""

import io
import sys
import json
import logging
import os
import random
import time
import pandas as pd
import numpy as np
import copy
import warnings
from traceback import print_exc
from datetime import datetime
from zipfile import ZipFile
# import to make zip
from utilities.generate_time_series import generate_time_series
from aegis_logging import setup_logger

from syntheticData.float_data_generator import generate_float_data
from syntheticData.alpha_numeric_data_generator import generate_alpha_num_data
from syntheticData.boolean_data_generator import generate_boolean_data
from syntheticData.faker_generation import generate_entity_data
from syntheticData.date_data_generator import generate_date_data
from syntheticData.string_data_generator import generate_string_data
from syntheticData.integer_data_generator import generate_integer_data
from data_consistency_handler import add_generation_consistency
from utilities.constants_declearation import CREATE_SCHEMA, FAILED, FINISHED, RUNNING
from utilities.path_manager import DATE_FORMATS_JSON,AEGIS_PYTHON_LOGS
warnings.filterwarnings("ignore", category=FutureWarning)

print(f'AEGIS_PYTHON_LOGS={AEGIS_PYTHON_LOGS}')
loggstr = 'Inside SyntheticDataGenerator.py:'

logger_name = "synthetic_data_log"

def get_time_stamp():
    ts = time.time()
    return int(ts)

def update_distributions(distributions_dict, num_of_records, data_type, higher_unique_values_count):
    try:
        logger.info(loggstr+'inside update_distributions function')
        date_format_dict = {}
        with open(DATE_FORMATS_JSON) as f:
            date_format_dict = json.load(f)
        property_str = ""
        if "minChars" in distributions_dict.keys():
            property_str = ""+str(distributions_dict["minChars"])
        if len(property_str) == 0:
            distributions_dict["minChars"] = 3
            logger.info(f"minChars in update=3")
        property_str = ""
        if "maxChars" in distributions_dict.keys():
            property_str = ""+str(distributions_dict["maxChars"])
        if len(property_str) == 0:
            distributions_dict["maxChars"] = 8
            logger.info(f"maxChars in update=8")
        if data_type.lower() == 'integer' or data_type.lower() == 'long' or data_type.lower() == 'float' or data_type.lower() == 'double':
            property_str = ""
            if "minimum" in distributions_dict.keys():
                property_str = ""+str(distributions_dict["minimum"])
                logger.info(f"minimum in updates={property_str}")
            if len(property_str) == 0:
                distributions_dict["minimum"] = 1
        elif data_type.lower() == 'date':
            property_str = ""
            if "minimum" in distributions_dict.keys():
                property_str = ""+str(distributions_dict["minimum"])
                logger.info(f"minimum date in updates={property_str}")
            if len(property_str) == 0:
                distributions_dict["minimum"] = "2020-01-01"
            property_str = ""
            if "maximum" in distributions_dict.keys():
                property_str = ""+str(distributions_dict["maximum"])
                logger.info(f"maximum date in updates={property_str}")
            if len(property_str) == 0:
                distributions_dict["maximum"] = "2020-12-31"
            property_str = ""
            if "dateFormat" in distributions_dict.keys():
                property_str = ""+str(distributions_dict["dateFormat"])
            if len(property_str) == 0:
                distributions_dict["dateFormat"] = "yyyy-mm-dd"
                logger.info("date format in updates= %Y-%m-%d")
        elif data_type.lower() == 'timestamp':
            property_str = ""
            if "minimum" in distributions_dict.keys():
                property_str = ""+str(distributions_dict["minimum"])
                logger.info(f"minimum in updates={property_str}")
            if len(property_str) == 0:
                distributions_dict["minimum"] = "2020-01-01 00:00:00"
            property_str = ""
            if "maximum" in distributions_dict.keys():
                property_str = ""+str(distributions_dict["maximum"])
                logger.info(f"maximum in updates={property_str}")
            if len(property_str) == 0:
                distributions_dict["maximum"] = "2020-12-31 00:00:00"
            property_str = ""
            if "dateFormat" in distributions_dict.keys():
                property_str = ""+str(distributions_dict["dateFormat"])
                dateFormat = property_str
                logger.info(f"timestamp before updates={property_str}")
                distributions_dict["dateFormat"] = dateFormat
                logger.info(f"timestamp after update={distributions_dict['dateFormat']}")
            if len(property_str) == 0:
                distributions_dict["dateFormat"] = "yyyy-MM-dd hh:mm:ss"
        property_str = ""
        if "uniqueValuesCount" in distributions_dict.keys():
            property_str = ""+str(distributions_dict["uniqueValuesCount"])
        if len(property_str) == 0:
            if higher_unique_values_count > 0:
                distributions_dict["uniqueValuesCount"] = higher_unique_values_count
            else:
                if len(str(num_of_records)) <= 0:
                    distributions_dict["uniqueValuesCount"] = 100
                else:
                    distributions_dict["uniqueValuesCount"] = num_of_records
        property_str = ""
        if "recordCount" in distributions_dict.keys():
            property_str = ""+str(distributions_dict["recordCount"])
        if len(property_str) == 0:
            distributions_dict["recordCount"] = num_of_records
        property_str = ""
        if "stepSize" in distributions_dict.keys():
            property_str = ""+str(distributions_dict["stepSize"])
        if len(property_str) == 0 and data_type == "integer":
            distributions_dict["stepSize"] = 1

        elif len(property_str) == 0 and data_type == "float":
            distributions_dict["stepSize"] = 0.234
        return distributions_dict
    except Exception as exception:
        logger.error(exception)
        print_exc()
        exit()


def make_default_distribution(data_type, num_of_records, higher_unique_values_count):
    try:
        logger.info(loggstr+'inside make_default_distribution function')
        distributions_dict = {}
        recordCount = num_of_records
        minChars = 3
        maxChars = 8
        minimum = 1
        maximum = 1000
        uniqueValuesCount = 0
        if higher_unique_values_count <= 0:
            if num_of_records > 100:
                uniqueValuesCount = 50
            elif num_of_records > 10:
                uniqueValuesCount = 10
            else:
                uniqueValuesCount = 1
        else:
            uniqueValuesCount = higher_unique_values_count
        distributions_dict["recordCount"] = recordCount
        distributions_dict["uniqueValuesCount"] = uniqueValuesCount
        if data_type.lower() == 'integer' or data_type.lower() == 'long':
            distributions_dict["minimum"] = minimum
            distributions_dict["maximum"] = maximum

        if data_type.lower() == data_type.lower() == 'float' or data_type.lower() == 'double':
            distributions_dict["minimum"] = 1.25
            distributions_dict["maximum"] = 423.22

        elif data_type.lower() == 'string' or data_type.lower() == 'alphanumeric':
            distributions_dict["minChars"] = minChars
            distributions_dict["maxChars"] = maxChars
        elif data_type.lower() == 'date':
            distributions_dict["minimum"] = "2020-01-01"
            distributions_dict["maximum"] = "2020-12-31"
            distributions_dict["dateFormat"] = "yyyy-MM-dd"
            distributions_dict["stepSize"] = 1

        elif data_type.lower() == 'timestamp':
            distributions_dict["minimum"] = "2020-01-01 00:00:00"
            distributions_dict["maximum"] = "2020-12-31 00:00:00"
            distributions_dict["dateFormat"] = "yyyy-MM-dd hh:mm:ss"
            distributions_dict["stepSize"] = 1

        return distributions_dict
    except Exception as exception:
        print_exc()
        logger.error("Exception occurred", exc_info=True)
        exit()


def init_generation(schemaDict):
    try:
        data_list_dict = {}
        num_of_records = 100
        numeric_id_dict = {}
        try:
            num_of_records = int(schemaDict["numberOfRecords"])
            logger.info(f"num of records from schema:{num_of_records}")
        except Exception as e:
            num_of_records = int(schemaDict["number_of_records"])
        logger.info(f"num_of_records={num_of_records}")
        columnDetails = schemaDict["columndetails"]
        totalColumns = len(columnDetails)
        logger.info(f"total columns={totalColumns}")
        columnList = []
        dataDict = {}
        unique_count_dict = {}
        for i in range(totalColumns):
            colDetailDict = columnDetails[i]
            columnName = colDetailDict["columnName"]
            data_type = ""
            entity = ""
            if "data_type" in colDetailDict:
                data_type = colDetailDict["data_type"]
            if "dataType" in colDetailDict:
                data_type = colDetailDict["dataType"]

            if "entity" in colDetailDict:
                entity = colDetailDict["entity"]
               

            generation = colDetailDict["generation"]
            isDistribution = colDetailDict["isDistribution"]
            nullPer = colDetailDict["nullPer"]

            logger.info(f"columnName={columnName}")
            logger.info(f"data_type={data_type}")
            logger.info(f"entity={entity}")
            logger.info(f"generation={generation}")
            uniqueValuesCount = 0
            if "uniqueValuesCount" in colDetailDict.keys():
                uniqueStr = str(colDetailDict["uniqueValuesCount"])
                if len(uniqueStr) > 0:
                    uniqueValuesCount = int(colDetailDict["uniqueValuesCount"])

            # if data_type.lower() == "integer" or data_type.lower() == "string" or data_type.lower() == "date":
            distributions = colDetailDict["distributions"]
            if not isDistribution:
                distributions = []
                newDistr = make_default_distribution(
                    data_type, num_of_records, uniqueValuesCount)
                distributions.append(newDistr)
            totalDistributions = len(distributions)
            logger.info(f"total distributions={totalDistributions}")
            data_list = []
            logger.info("distrubution completed")
            for j in range(totalDistributions):
                col_dict = distributions[j]
                col_dict = update_distributions(
                    col_dict, num_of_records, data_type, uniqueValuesCount)
                if "default" in colDetailDict.keys():
                    col_dict["default"] = colDetailDict["default"]
                if (entity.lower() == "string" or entity.lower() == "stringlower" or entity.lower() == "stringupper")  and data_type.lower() == "string":
                    col_dict = generate_string_data(
                        col_dict, columnName, generation,entity)
                elif entity.lower() == "float" and data_type.lower() == "float":
                    col_dict = generate_float_data(
                        col_dict, columnName, generation, "float")
                elif entity.lower() == "double" and data_type.lower() == "double":
                    col_dict = generate_float_data(
                        col_dict, columnName, generation, "float")
                elif entity.lower() == "integer" and data_type.lower() == "integer":
                    logger.info('call generate_integer_data() this method')
                    col_dict = generate_integer_data(
                        col_dict, columnName, generation, "integer")
                elif entity.lower() == "long" and data_type.lower() == "long":
                    col_dict = generate_integer_data(
                        col_dict, columnName, generation, "integer")
                elif entity.lower() == "date" and data_type.lower() == "date":
                    col_dict = generate_date_data(
                        col_dict, columnName, generation)

                elif entity.lower() == "timestamp" and data_type.lower() == "timestamp":
                    col_dict = generate_date_data(
                        col_dict, columnName, generation)
                elif entity.lower() == "boolean" and data_type.lower() == "boolean":
                    col_dict = generate_boolean_data(
                        col_dict, columnName, generation)
                elif entity.lower() == "alphanumeric" and data_type.lower() == "alphanumeric":
                    col_dict = generate_alpha_num_data(
                        col_dict, columnName, generation)
                elif entity.lower() == "alphanumeric" and data_type.lower() == "string":
                    col_dict = generate_alpha_num_data(
                        col_dict, columnName, generation)
                elif entity.lower() == "timeseries" and data_type.lower() == "datetime":
                    timeseries_json_dict = add_time_series_info(schemaDict)
                    col_dict = generate_time_series(col_dict,timeseries_json_dict)
                    logger.info("timeseries")
                    
                else:
                    logger.info("faker_call")
                    col_dict = generate_entity_data(
                        col_dict, entity, generation)

                dataListDist = col_dict["generated"]
                data_list.extend(dataListDist)
            columnList.append(columnName)
            if data_type.lower() == "string" or data_type.lower() == "alphanumeric" or data_type.lower() == "date":
                data_list = add_null_values(data_list, nullPer)
            # dataDict[columnName] = data_list
            data_list_dict[columnName] = data_list

            unique_count_dict[columnName] = len(data_list)
            if entity == "numericId":
                numeric_id_dict[columnName] = data_list
        # df = pd.DataFrame(dataDict)
        return data_list_dict, unique_count_dict, num_of_records, columnList, numeric_id_dict
    except Exception as exception:
        print_exc()
        logger.error("Exception occurred", exc_info=True)
        exit()


def add_null_values(data_list, nullPer):
    try:
        total = len(data_list)
        if len(nullPer) == 0:
            return data_list
        nullCount = int(total * int(nullPer) / 100)
        for i in range(nullCount):
            randomNum = random.randint(0, total-1)
            data_list[randomNum] = ""
        return data_list
    except Exception as exception:
        logger.error(exception)
        print_exc()


def extract_schema_and_connection_dict(schemaJsonPath):
    try:
        logger.info(loggstr+'inside extract_schema_and_connection_dict function')
        wholeSchemaDict = {}
        with open(schemaJsonPath) as f:
            wholeSchemaDict = json.load(f)
        schemaDict = wholeSchemaDict["schema"]
        # print("*******************************",schemaDict)
        connectionDict = wholeSchemaDict["connectioninfo"]
        return schemaDict, connectionDict
    except Exception as exception:
        logger.error(exception)
        print_exc()
        exit()


def add_time_series_info(schemaDict):
    timeseries_json_dict = {}
    columndetails = schemaDict["columndetails"]
    number_of_records = schemaDict["numberOfRecords"]
    for i in range(len(columndetails)):
        column_dict = columndetails[i]
        entity = column_dict['entity']
        if entity.lower() == 'timeseries':
            timeseries_json_dict = column_dict
            timeseries_json_dict["number_of_records"] = number_of_records
            break
    return timeseries_json_dict


def handle_data_generation(schemaJsonPath):
    try:
        global logger
        zipFileName = ""
        schemaDictArr, connectionDict = extract_schema_and_connection_dict(
            schemaJsonPath)
        schemaSize = len(schemaDictArr)
        filePathList = []
        for i in range(schemaSize):
           
            schemaDict = schemaDictArr[i]
            timeseries_json_dict = add_time_series_info(schemaDict)
            df = pd.DataFrame()
            unique_count_dict = 0
            numeric_id_dict = {}
            try:
                data_list_dict, unique_count_dict, num_of_records, columns, numeric_id_dict = init_generation(
                    schemaDict)
                if data_list_dict is None:
                    continue
                else:
                    logger.info("Ready to go Inside add_generation_cosistanct")
                    df = add_generation_consistency(
                        data_list_dict, unique_count_dict, num_of_records, columns)
                    for col, data_list in numeric_id_dict.items():
                        df[col] = data_list
            except Exception as e:
                continue
            filename = schemaDict["schemaName"]
            print("filename=", filename,i)
            folderPath = connectionDict["folderPath"]
            filePath = folderPath+"/"+filename+".csv"

            if df.empty:
                continue

            df.to_csv(filePath, index=False)
            filePathList.append(filename+".csv")
            if zipFileName == "":
                zipFileName = folderPath+"/"+filename+".zip"
            targetType = connectionDict["targetType"]
            if targetType.lower() == "sql":
                print("Sql connection not supported")
        if len(filePathList) > 0:
            for root, dirs, files in os.walk(folderPath):
                for file in filePathList:
                    with ZipFile(zipFileName, 'w') as zip:
                        zip.write(os.path.join(root, file),
                                  os.path.relpath(os.path.join(root, file),
                                                  os.path.join(folderPath, '..')))
    except Exception as exception:
        logger.error(exception)
        print_exc()
        exit()


def handle_file_paths(col_dict):
    try:
        string_data_list = []
        defaultDict = col_dict["default"]
        filePath = defaultDict["filePath"]
        refColumnName = defaultDict["refColumnName"]
        recordCount = int(col_dict["recordCount"])
        # df = pd.read_csv(filePath)
        no_of_col = len(list(pd.read_csv(filePath, nrows=0).columns))
        df = pd.read_csv(filePath, usecols=range(no_of_col))
        data_list = df[refColumnName]
        for i in range(recordCount):
            string_data_list.append(data_list[i])
        col_dict["generated"] = string_data_list
        return col_dict
    except Exception as exception:
        print_exc()


if __name__ == "__main__":

    try:
        global logger
        schemaJsonPath = sys.argv[1]
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # ts = get_time_stamp()
        log_file = "synthetic_data"
        log_file = AEGIS_PYTHON_LOGS + "/" + log_file +"_"+str(ts)+".log"
        setup_logger(logger_name, log_file)
        os.environ['LOGGER_NAME'] = logger_name
        logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info(loggstr+'process started')
        handle_data_generation(schemaJsonPath)
        logger.info("process completed")

    except Exception as exception:
        logger.error("Exception occurred", exc_info=True)
