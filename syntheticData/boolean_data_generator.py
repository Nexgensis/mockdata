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
loggstr='Inside boolean_data_generator.py:'

def generate_boolean_data(colDict,colName, generation):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info(loggstr+'inside generate_boolean_data function')
        numOfRecords= int(colDict["recordCount"])
        uniqueValuesCount= int(colDict["uniqueValuesCount"])
        stringDataList=[]
        if uniqueValuesCount == 1:
            defaultValue= colDict["defaultValue"]
            for i in range(numOfRecords):
                stringDataList.append(defaultValue)
            colDict["generated"]=stringDataList
            return colDict

        for i in range(numOfRecords):
            num=random.randint(0,1)
            if num == 0:
                stringDataList.append('false')
            else:
                stringDataList.append('true')
        colDict["generated"]=stringDataList[0:numOfRecords]
        return colDict
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)
        exit()

        # pip install -U pip setuptools wheel
