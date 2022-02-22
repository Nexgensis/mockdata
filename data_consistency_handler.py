import io
import sys
import json
import logging
import os
import random
import pandas as pd

import copy 
import warnings
from traceback import print_exc
from sklearn.utils import shuffle

warnings.filterwarnings("ignore", category=FutureWarning)

logger = None
loggstr='Inside data_consistency_handler.py:'

def add_generation_consistency(data_list_dict, unique_count_dict, num_of_records , columns):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info(loggstr+'inside add_generation_consistency function')
        df_mod = pd.DataFrame()
        done_list = []
        i=0
        for col in columns:
            if col in done_list:
                continue
            done_list.append(col)
            data_list = data_list_dict[col]
            # unique_count = int(unique_count_dict[col])
            unique_count = len(data_list)
            duplicate_count = num_of_records - unique_count
            groupDF= pd.DataFrame()
            groupDF[col] = data_list
            for column_name , ucount in unique_count_dict.items():
                ucount = int(ucount)
                if column_name in done_list or column_name == col:
                    continue
                if unique_count == ucount:
                    done_list.append(column_name)
                    groupDF[column_name] = data_list_dict[column_name]
            groupDF = shuffle(groupDF)
            groupDF = make_duplicates_of_unique(groupDF, duplicate_count, unique_count)
            df_mod = pd.concat([df_mod,groupDF], axis=1)  
        df_mod = df_mod[columns]
        return df_mod
    except Exception as exception:
        logger.error(exception)
        print_exc()


def make_duplicates_of_unique(df, duplicate_count, unique_values_count):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        if duplicate_count == 0:
            return df
        df_mod = pd.DataFrame()       
        columns = list(df.columns)
        randomNumList = []
        for i in range(duplicate_count):
            randomNumList.append(random.randint(0,unique_values_count-1))
        for col in columns:
            orgList = list(df[col].values)
            dataList = list(df[col].values)
            for i in range(duplicate_count):
                randomNum= randomNumList[i]
                dataList.append(str(orgList[randomNum]))
            # random.shuffle(dataList)
            df_mod[col] = dataList
        return df_mod
    except Exception as e:
        print_exc()
        logger.error("Exception occurred", exc_info=True) 

