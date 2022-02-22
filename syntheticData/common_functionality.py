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
loggstr='Inside CommonFunctionality.py:'

def remove_extra_list_elements(dataList,uniqueValuesCount ):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info(loggstr+'inside remove_extra_list_elements function')
        random.shuffle(dataList)
        intListSize= len(dataList)
        delCount= intListSize - uniqueValuesCount
        if delCount == 0:
            random.shuffle(dataList)
            return dataList
            
        logger.info(f"list size={intListSize}")
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")
        logger.info(f"delCount={delCount}")
        return dataList[0:uniqueValuesCount]
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)

def make_duplicates_of_unique(dataList, duplicateCount):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        if duplicateCount == 0:
            return dataList
        listSize= len(dataList)
        for i in range(duplicateCount):
            randomNum= random.randint(0,listSize-1)
            dataList.append(dataList[randomNum])
        # random.shuffle(dataList)
        logger.info(f"list size inside duplicates={str(len(dataList))}")
        return dataList
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)

def updateDataByAdditives(colDict, dataList):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info(loggstr+'inside updateDataByAdditives function')
        stringDataList=[]
        beginWithList=[]
        endsWithList=[]
        beginWith=""
        endsWith=""
        if "endsWith" in colDict.keys():
            endsWith= colDict["endsWith"]
            endsWithList= endsWith.split(",")            
        if "beginWith" in colDict.keys():
            beginWith= colDict["beginWith"]
            beginWithList= beginWith.split(",")
        beginListSize= len(beginWithList)
        endListSize= len(endsWithList)
        for d in dataList:
            begWith=""
            enWith=""
            if beginListSize > 0:
                randNum= random.randint(1,beginListSize)-1
                begWith= beginWithList[randNum]
            if endListSize > 0:
                randNum= random.randint(1,endListSize)-1
                enWith= endsWithList[randNum]
            genStr= str(begWith)+str(d)+ str(enWith)
            stringDataList.append(genStr)
        return stringDataList
    except Exception as exception:
        logger.error(exception)
        print_exc()