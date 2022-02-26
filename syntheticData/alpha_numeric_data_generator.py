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
import string

import copy 
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

logger = None
loggstr='Inside AlphaNumDataGenerator.py:'

from .common_functionality import remove_extra_list_elements

def generate_with_lower_case(totalCount,minChars, maxChars):
    try:
        stringDataList=[]
        i=0
        while i<totalCount:
            strLen= random.randint(minChars,maxChars)
            # genStr= ''.join(random.choices(string.ascii_uppercase + string.digits, k=strLen)) 
            genStr= ''.join(random.choices(string.ascii_lowercase + string.digits, k=strLen))
            if genStr.isalpha()==False:
                i+=1
                stringDataList.append(genStr)
        return stringDataList
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)

def generate_with_upper_case(totalCount,minChars, maxChars):
    try:
        stringDataList=[]
        i=0
        while i<totalCount:
            strLen= random.randint(minChars,maxChars)
            # genStr= ''.join(random.choices(string.ascii_uppercase + string.digits, k=strLen)) 
            genStr= ''.join(random.choices(string.ascii_uppercase + string.digits, k=strLen)) 
            if genStr.isalpha()==False:
                i+=1
                stringDataList.append(genStr)
        return stringDataList
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)

def generate_with_ignore_case(totalCount,minChars, maxChars):
    try:
        stringDataList=[]
        i=0
        while i<totalCount:
            strlen= random.randint(minChars,maxChars)
            # genStr= ''.join(random.choices(string.ascii_uppercase + string.digits, k=strLen)) 
            gen_str = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=strlen)) 
            if gen_str.isalpha()==False:
                i+=1
                stringDataList.append(gen_str)
        return stringDataList
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def get_listof_strings(stringDataList, totalCount, minChars, maxChars):
    try:
        defaultCase='ignoreCase'
        stringDataList=[]
        logger.info("inside getListofString")
        logger.info(f"totalCount={totalCount}")
        if defaultCase.lower() == 'upper':
            stringDataList= generate_with_upper_case(totalCount,minChars, maxChars)
        elif defaultCase.lower() == 'lower':
            stringDataList= generate_with_lower_case(totalCount,minChars, maxChars)
        else:
            stringDataList= generate_with_ignore_case(totalCount,minChars, maxChars)
        
        logger.info("process completed inside getListofString")
        return stringDataList
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)

def generate_alpha_num_data(colDict,colName, generation):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info(loggstr+'inside generate_alpha_num_data function')
        numOfRecords= int(colDict["recordCount"])
        stringDataList=[]
        uniqueValuesCount= int(colDict["uniqueValuesCount"])
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")
        minChars= int(colDict["minChars"])
        maxChars= int(colDict["maxChars"])
        logger.info(f"minChars={minChars}")
        logger.info(f"maxChars={maxChars}")
        if uniqueValuesCount > 0:
            stringDataList= get_listof_strings(stringDataList, numOfRecords, minChars, maxChars)
            
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
                    if(index >=10):
                        break
                    index+=1
                    logger.info("Inside loop")
                    stringDataList= get_listof_strings(stringDataList, numOfRecords, minChars, maxChars)
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
        logger.info(loggstr+" process completed inside generate_alpha_num_data function:")
        return colDict
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)
        exit()