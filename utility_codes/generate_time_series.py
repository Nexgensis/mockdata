"""
This utility is basically used to generate time series column for the synthetic data
Here we use pandas date_range function for generating time series data.
Here we use following format for date ::Date f ::D:Day,Y:Year,M:Month,H:Hours,T:Minute,S:Seconds
"""

from datetime import datetime
import random
from traceback import print_exc
from utility_codes.constants_declearation import FAILED
import pandas as pd

# This function will accept the data frame and it will append the timeseries column in generated data_column


def generate_time_series(col_dict,timeseries_json_dict):

    try:
        
        # collect the date and time details from timeseries_json_dict
        
        date1, date2 = timeseries_json_dict["transactionDate"][0],\
            timeseries_json_dict["transactionDate"][1]

        start_date_with_time, end_date_with_time = date1.split(
            "T"), date2.split("T")

        start_date = start_date_with_time[0]
        end_date = end_date_with_time[0]

        # format the date for convserion to mm,dd,yyy
        start_date = start_date.replace("/", "-")
        end_date = end_date.replace("/", "-")

        formatted_start_date, formatted_end_date = datetime.strptime\
            (start_date, '%Y-%m-%d').strftime('%m/%d/%Y'),\
            datetime.strptime(end_date, '%Y-%m-%d').strftime('%m/%d/%Y')

        start_date = formatted_start_date.replace("/", "-")
        end_date = formatted_end_date.replace("/", "-")

        start = start_date+" "+start_date_with_time[1]
        end = end_date+" "+end_date_with_time[1]
        freq = timeseries_json_dict["frequency"]

        print("start@@@@@@@@@@@@@@@@@@@",start,end)

        # generate timeseries data
        time_series_data = pd.date_range(start=start, end=end, freq=freq)

        # generate random data from generated time series as per number of records
        final_time_series_data = random.choices(
            time_series_data, k=timeseries_json_dict["number_of_records"])

        # sort the data by date and time
        final_time_series_data.sort()

        return {'recordCount': col_dict['recordCount'], 'uniqueValuesCount':  col_dict['uniqueValuesCount'],'generated':final_time_series_data}


      
    except:
        print_exc()
        exit()

    
    # return df
