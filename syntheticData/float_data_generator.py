from utilities.constants_declearation import CREATE_SCHEMA, FAILED
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

logger = None
loggstr='Inside FloatDataGenerator.py:'

from .common_functionality import remove_extra_list_elements
# from .commom_functionality import make_duplicates_of_unique


def generateIntegerData(colDict,colName, generation,dataType):
    try:
        numOfRecords= int(colDict["recordCount"])
        intDataList=[]
        uniqueValuesCount= int(colDict["uniqueValuesCount"])
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")

        if (numOfRecords == uniqueValuesCount) or generation.lower() == "sequential":
            intDataList= getListofFloat(colDict)
            # random.shuffle(intDataList)
            colDict["generated"]=intDataList[0:numOfRecords]
            logger.info(loggstr+" process completed inside generateIntegerData function:")
            return colDict
        if uniqueValuesCount > 0:
            intDataList= getListofFloat(colDict)
            # random.shuffle(intDataList)
            intDataListSize= len(intDataList)
            logger.info(f"list size= {intDataListSize}")
            if intDataListSize > uniqueValuesCount:
                logger.info("dateListSize > uniqueValuesCount")
                intDataList= remove_extra_list_elements(intDataList,uniqueValuesCount)
            colDict["generated"]=intDataList[0:numOfRecords]
        logger.info(loggstr+" process completed inside generateIntegerData function:")
        return colDict
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def getListofFloat(colDict):
    try:
        logger.info("inside getListofIntegers")
        uniqueValuesCount= int(colDict["uniqueValuesCount"])
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")
       
       
        
        maximum=100.0
        minimum=2.0
        stepSize=0.16
        
        if "minimum" in colDict:
            minimum= float(colDict["minimum"])

        if "maximum" in colDict:
            maximum= float(colDict["maximum"])

        if "stepSize" in colDict:
            stepSize= float(colDict["stepSize"])

        a= np.arange(minimum,maximum,stepSize)
        floatList= list(a.round(decimals=3))
        random.shuffle(floatList)
        logger.info("process completed inside getListofIntegers")
        return floatList[0:uniqueValuesCount]
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)

def generate_float_data(colDict,colName, generation,dataType):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info(loggstr+'inside generate_float_data function')
        numOfRecords= int(colDict["recordCount"])
        intDataList=[]
        uniqueValuesCount= int(colDict["uniqueValuesCount"])
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")
        intDataList= getListofFloat(colDict)
        if uniqueValuesCount > 0:
            intDataListSize= len(intDataList)
            logger.info(f"list size= {intDataListSize}")
            if intDataListSize > numOfRecords:
                logger.info("dateListSize > uniqueValuesCount")
                intDataList= remove_extra_list_elements(intDataList,uniqueValuesCount)
            elif intDataListSize < numOfRecords:               
                duplicateCount= numOfRecords - intDataListSize
                # intDataList= make_duplicates_of_unique(intDataList, duplicateCount)
            colDict["generated"]=intDataList
        logger.info(loggstr+" process completed inside generate_float_data function:")
        logger.info(f"final list size={str(len(intDataList))}")
        return colDict
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)
        exit()