
from utility_codes.constants_declearation import CREATE_SCHEMA, FAILED
import pandas as pd
import numpy as np

import io
import sys
import json
import logging
import os
from traceback import print_exc
import random

import copy
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
from .common_functionality import updateDataByAdditives
from .common_functionality import remove_extra_list_elements
logger = None
loggstr = 'Inside IntegerDataGenerator.py:'

# from .commom_functionality import make_duplicates_of_unique


def get_listof_integers(colDict):
    try:
        uniqueValuesCount= int(colDict["uniqueValuesCount"])
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")
        minimum = int(colDict["minimum"])
        stepSize= int(colDict["stepSize"])
        logger.info(f"step size={stepSize}")
        maxVal=100
        property_str = ""
        if "maximum" in colDict.keys():
            property_str = ""+str(colDict["maximum"])
        if len(property_str) == 0:
            maxVal = minimum + uniqueValuesCount * stepSize
        else:
            maxVal = int(colDict["maximum"])
        logger.info("inside getListofIntegers")
        logger.info(f"minimum ={minimum}")
        logger.info(f"maxVal ={maxVal}")     
        intList= list(np.arange(minimum,maxVal,stepSize))
        logger.info(f"list size inside get_listof_integers={str(len(intList))}")
        # random.shuffle(intList)
        return intList
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def generate_integer_data(colDict, colName, generation, dataType):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info("Inside generate Integer data")
        numOfRecords= int(colDict["recordCount"])
        intDataList=[]
        uniqueValuesCount= int(colDict["uniqueValuesCount"])
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")
        if (numOfRecords == uniqueValuesCount) or generation.lower() == "sequential":
            intDataList = get_listof_integers(colDict)
            random.shuffle(intDataList)
            colDict["generated"] = intDataList[0:numOfRecords]
            logger.info(loggstr+" process completed inside generateIntegerData function:")
            return colDict
        if uniqueValuesCount > 0:
            intDataList = get_listof_integers(colDict)
            random.shuffle(intDataList)
            intDataListSize = len(intDataList)
            logger.info(f"list size= {intDataListSize}")
            if intDataListSize > uniqueValuesCount:
                logger.info("dateListSize > uniqueValuesCount")
                intDataList= remove_extra_list_elements(intDataList,uniqueValuesCount)
                intDataListSize= len(intDataList)
            stringDataList= updateDataByAdditives(colDict, intDataList)
            logger.info(f"final list size after additives={str(len(stringDataList))}")
            colDict["generated"]=stringDataList[0:numOfRecords]
        logger.info(loggstr+" process completed inside generateIntegerData function:")
        return colDict
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)
        exit()