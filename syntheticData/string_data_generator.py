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
import string

import copy 
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

logger = None
loggstr='Inside StringDataGenerator.py:'

from .common_functionality import remove_extra_list_elements

def get_listof_strings(stringDataList,colDict,entity):
    try:
        logger.info(loggstr+'inside get_listof_strings function')
        totalCount= int(colDict["recordCount"])
        minChars= int(colDict["minChars"])
        maxChars= int(colDict["maxChars"])
        logger.info(f"minChars={minChars}")
        logger.info(f"maxChars={maxChars}")
        beginWith=""
        endsWith=""
        if "endsWith" in colDict.keys():
            endsWith= colDict["endsWith"]
        if "beginWith" in colDict.keys():
            beginWith= colDict["beginWith"]
        logger.info("inside getListofIntegers")
        logger.info(f"totalCount={totalCount}")
        for i in range(totalCount):
            strLen= random.randint(minChars,maxChars)
            # genStr= ''.join(random.choices(string.ascii_uppercase + string.digits, k=strLen)) 
            genStr= ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=strLen))
            
            #check for entity type for uppercase or lowercase conversion
            if entity.lower()=="stringupper":
                genStr=genStr.upper()
            elif entity.lower()=="stringlower":
                genStr=genStr.lower()
                
            genStr= str(beginWith)+str(genStr)+ str(endsWith)
            stringDataList.append(genStr)
        return stringDataList
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)

def generate_string_data(colDict,colName, generation,entity):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info(loggstr+'inside generate_string_data function')
        numOfRecords= int(colDict["recordCount"])
        stringDataList=[]
        uniqueValuesCount= int(colDict["uniqueValuesCount"])
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")
        minChars= int(colDict["minChars"])
        maxChars= int(colDict["maxChars"])
        logger.info(f"minChars={minChars}")
        logger.info(f"maxChars={maxChars}")
        beginWith=""
        endsWith=""
        if "endsWith" in colDict.keys():
            endsWith= colDict["endsWith"]
        if "beginWith" in colDict.keys():
            beginWith= colDict["beginWith"]
        
        if uniqueValuesCount > 0:
            stringDataList= get_listof_strings(stringDataList, colDict,entity)
            
            stringDataList= list(set(stringDataList))
            # random.shuffle(stringDataList)
            stringListSize= len(stringDataList)
            logger.info(f"list size= {stringListSize}")
            if stringListSize > uniqueValuesCount:
                logger.info("stringListSize > uniqueValuesCount")
                stringDataList= remove_extra_list_elements(stringDataList,uniqueValuesCount)
                stringListSize= len(stringDataList)
            if stringListSize < uniqueValuesCount:
                logger.info("stringListSize < uniqueValuesCount")
                countNotMatch= True
                index=0
                while countNotMatch:
                    if(index >=25):
                        break
                    index+=1
                    logger.info("Inside loop")
                    stringDataList= get_listof_strings(stringDataList,colDict,entity)
                    stringDataList= list(set(stringDataList))
                    # random.shuffle(stringDataList)
                    stringListSize= len(stringDataList)
                    logger.info(f"list size= {stringListSize}")
                    if stringListSize > uniqueValuesCount:
                        stringDataList= remove_extra_list_elements(stringDataList,uniqueValuesCount)
                        countNotMatch= False
                        break
                    elif stringListSize == uniqueValuesCount:
                        countNotMatch= False
                        break
                    else:
                        continue
            duplicateCount= numOfRecords - uniqueValuesCount
            # stringDataList= make_duplicates_of_unique(stringDataList, duplicateCount)
            colDict["generated"]=stringDataList[0:numOfRecords]
        logger.info(loggstr+" process completed inside generateIntegerData function:")
        return colDict
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)
        exit()