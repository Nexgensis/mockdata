"""
This utility is basically used to generate time series column for the synthetic data
Here we use pandas date_range function for generating time series data.
Here we use following format for date ::Date f ::D:Day,Y:Year,M:Month,H:Hours,T:Minute,S:Seconds
"""

from datetime import datetime
import random
from traceback import print_exc
from utilities.constants_declearation import FAILED
import pandas as pd

# This function will accept the data frame and it will append the timeseries column in generated data_column


def getTimeBySeconds(sec):
    try:
        from datetime import timedelta
        return str(timedelta(seconds=sec))
    except:
        print_exc()

def getSeconds(timestr):
    timearr= timestr.split(":")
    hh = int(timearr[0])
    mm = int(timearr[1])
    ss = int(timearr[2])
    totalSec= (hh*60*60)+(mm*60)+ss
    return totalSec

def addRandomSeconds(number_of_records,start_seconds_str, date_time_list):
    try:
        import random
        end_seconds= getSeconds("23:59:59")
        start_seconds = getSeconds(start_seconds_str)
        for i in range(0,number_of_records):
            date_str_arr= str(date_time_list[i]).split(" ")
            date_part,time_part= date_str_arr[0],date_str_arr[1]
            random_sec= random.randint(start_seconds,end_seconds)
            time_part_new = getTimeBySeconds(random_sec)
            date_new_str= str(date_part)+" "+str(time_part_new)
            date_obj= datetime.strptime(date_new_str, "%Y-%m-%d %H:%M:%S")
            date_time_list[i] = date_obj
        return date_time_list
    except:
        print_exc()


def generate_time_series(col_dict,timeseries_json_dict):

    try:
        
        # collect the date and time details from timeseries_json_dict
        
        date1, date2 = timeseries_json_dict["transactionDate"][0],\
            timeseries_json_dict["transactionDate"][1]

        print(f'date1:{date1}')
        print(f'date2:{date2}')
        start_date_with_time, end_date_with_time = date1.split(
            " "), date2.split(" ")

        start_date = start_date_with_time[0]
        end_date = end_date_with_time[0]

        print(f'start_date:{start_date}')
        print(f'end_date:{end_date}')
        # format the date for convserion to mm,dd,yyy
        start_date = start_date.replace("/", "-")
        end_date = end_date.replace("/", "-")

        formatted_start_date, formatted_end_date = datetime.strptime\
            (start_date, '%Y-%m-%d').strftime('%m/%d/%Y'),\
            datetime.strptime(end_date, '%Y-%m-%d').strftime('%m/%d/%Y')

        start_date = formatted_start_date.replace("/", "-")
        end_date = formatted_end_date.replace("/", "-")

        print(f'formatted_start_date:{formatted_start_date}')
        print(f'formatted_end_date:{formatted_end_date}')

        start = start_date+" "+start_date_with_time[1]
        end = end_date+" "+end_date_with_time[1]
        # freq = timeseries_json_dict["frequency"]
        freq = "D"
        print(f'start:{start}')
        print(f'end:{end}')


        print("start@@@@@@@@@@@@@@@@@@@",start,end)

        # generate timeseries data
        time_series_data = pd.date_range(start=start, end=end, freq=freq)

        # generate random data from generated time series as per number of records
        final_time_series_data = random.choices(
            time_series_data, k=timeseries_json_dict["number_of_records"])

        fixedTime = timeseries_json_dict["fixedTime"]
        if not fixedTime:
            final_time_series_data = addRandomSeconds(timeseries_json_dict["number_of_records"],start_date_with_time[1], final_time_series_data)
        # sort the data by date and time
        final_time_series_data.sort()
        

        return {'recordCount': col_dict['recordCount'], 'uniqueValuesCount':  col_dict['uniqueValuesCount'],'generated':final_time_series_data}


      
    except:
        print_exc()
        exit()

    
    # return df
