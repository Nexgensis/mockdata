
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
from datetime import datetime
from datetime import timedelta
from .common_functionality import remove_extra_list_elements
from utilities.path_manager import DATE_FORMATS_JSON
import copy
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

logger = None
loggstr = 'Inside DateDataGenerator.py:'

def getFormattedDate(dateVal, x, dateFormat):
    try:
        dateVal = dateVal + timedelta(days=int(x))
        dateVal = datetime.strptime(
            str(dateVal), '%Y-%m-%d %H:%M:%S').strftime(dateFormat)
        return dateVal
    except Exception as e:
        print_exc()


def getListofDates(colDict):
    try:
        uniqueValuesCount = int(colDict["uniqueValuesCount"])
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")
        minimum = colDict["minimum"]
        maximum = colDict["maximum"]
        stepSize = int(colDict["stepSize"])
        dateFormat = colDict["dateFormat"]
        dateFormatDict = {}
        logger.info(dateFormat)
        with open(DATE_FORMATS_JSON) as f:
            dateFormatDict = json.load(f)
        logger.info(f"date step size={stepSize}")
        logger.info("inside getListofDates")
        dateFormat = dateFormatDict[dateFormat]
        logger.info(f"Date format={dateFormat}")

        minimum = datetime.strptime(minimum, dateFormat)
        maximum = datetime.strptime(maximum, dateFormat)
        logger.info(f"minimum date={minimum}")
        logger.info(f"maximum date={maximum}")
        # dateList= (minimum + datetime.timedelta(days=x) for x in range(0, (maximum-minimum).days))
        daysCount = int((maximum-minimum).days)
        daysList = np.arange(0, daysCount+stepSize, stepSize)
        dayslistCount = len(daysList)
        if dayslistCount > uniqueValuesCount:
            dateList = [str(getFormattedDate(minimum, x, dateFormat))
                        for x in daysList]
            # dateList= [str(minimum + datetime.timedelta(days=int(x))) for x in daysList]
            dateDataList = dateList[0:uniqueValuesCount]
        elif dayslistCount == uniqueValuesCount:
            dateDataList = [str(getFormattedDate(minimum, x, dateFormat))
                            for x in daysList]
            # dateDataList = [str(minimum + datetime.timedelta(days=int(x))) for x in daysList]
        else:
            daysCount = uniqueValuesCount * stepSize
            daysList = np.arange(0, daysCount+stepSize, stepSize)
            dateDataList = [str(getFormattedDate(minimum, x, dateFormat))
                            for x in daysList]
            # dateDataList = [str(minimum + datetime.timedelta(days=int(x))) for x in daysList]

        logger.info("date list generated")
        return dateDataList[0:uniqueValuesCount]
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def generate_date_data(colDict, colName, generation):

    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info(loggstr+'inside generate_date_data function')
        numOfRecords = int(colDict["recordCount"])
        dateDataList = []
        uniqueValuesCount = int(colDict["uniqueValuesCount"])
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")
        minimum = colDict["minimum"]
        maximum = colDict["maximum"]
        stepSize = colDict["stepSize"]
        dateFormat = colDict["dateFormat"]
        logger.info(f"minimum date={minimum}")
        logger.info(f"maximum date={maximum}")
        logger.info(f"date step size={stepSize}")
        if (numOfRecords == uniqueValuesCount) or generation.lower() == "sequential":
            dateDataList = getListofDates(colDict)
            colDict["generated"] = dateDataList[0:numOfRecords]
            logger.info(
                loggstr+" process completed inside generateIntegerData function:")
            return colDict
        if uniqueValuesCount > 0:
            dateDataList = getListofDates(colDict)
            # random.shuffle(dateDataList)
            dateListSize = len(dateDataList)
            logger.info(f"list size= {dateListSize}")
            if dateListSize > uniqueValuesCount:
                logger.info("dateListSize > uniqueValuesCount")
                dateDataList = remove_extra_list_elements(
                    dateDataList, uniqueValuesCount)
            colDict["generated"] = dateDataList[0:numOfRecords]
        logger.info(
            loggstr+" process completed inside generateIntegerData function:")
        return colDict
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True)
        exit()
