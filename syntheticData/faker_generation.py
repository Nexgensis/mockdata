# basic imports
from utility_codes.constants_declearation import CREATE_SCHEMA, FAILED
from utility_codes.regex_handler import generate_number_by_regex
from .common_functionality import remove_extra_list_elements
import sys
import json
import logging
import os
import json
import re
import copy
import time
import random

import numpy as np
import uuid
import string
import warnings
from datetime import timedelta, datetime

import pandas as pd
from os import path
from traceback import print_exc
from faker import Faker
from faker.providers import BaseProvider
fake = Faker()

warnings.filterwarnings("ignore", category=FutureWarning)

logger = None
loggstr = 'Inside FakeGeneration.py:'

aegis_home = os.environ.get('AEGIS_HOME')
sys.path.append(aegis_home+'/python')
sys.path.append(aegis_home+'/python/utility_codes')


def get_product_list():
    try:
        default_val = [
            "Air conditioning",
            "Fryer Air",
            "Ioniser Blower Blender",
            "Immersion",
            "Clothes Blender",
            "Dryer",
            "Coffee iron",
            "Dish washer",
            "Domestic Cabinet",
            "Electric Fryer",
            "Electric Blanket",
            "Electric Drill",
            "Electric Knife",
            "Electric Boiler",
            "Electric Heater",
            "Electric Shaver",
            "Kitchen Processor",
            "Garbage Hood",
            "Fan Disposer",
            "Atticceiling Fan",
            "Microwave Oven",
            "Oven Pie",
            "Refrigerator Iron",
            "Crisper",
            "cleaner"
        ]
        return default_val
    except:
        print_exc()


def fake_x_product_name(colDict):
    try:
        default = get_product_list()
        product_total = len(default)
        rand_num = random.randint(0, product_total-1)
        return default[rand_num]
    except:
        print_exc()


# create new provider class
class MyProvider(BaseProvider):
    def phonenumber_countrycode(self):

        status = True
        while status:
            self.phoneNumber = fake.phone_number()
            data = self.phoneNumber

            if "+" in data:
                if "x" in data:
                    data = data.split("x")
                    return data[0]
                else:
                    return data
            else:
                continue


    def phonenumber(self):
        status = True
        while status:
            self.phoneNumber = fake.phone_number()
            data = self.phoneNumber

            if "+" not in data:
                if "." not in data:
                    if "(" not in data:
                        if "x" in data:
                            data = data.split("x")
                            return data[0]
                        else:
                            return data


fake.add_provider(MyProvider)


# create new provider class
class Credit(BaseProvider):
    def credit_card(self):

        status = True
        while status:
            self.creditCard = fake.credit_card_number()
            data = self.creditCard
            count = 0
            creditcard_no = ""
            for i_var in data:
                i_var = str(i_var)
                count += 1
                if count%4 == 0:
                    d_var = i_var + "-"
                    creditcard_no = creditcard_no + str(d_var)
                else:
                    creditcard_no = creditcard_no + i_var
            if creditcard_no.endswith("-"):
                continue
            else:
                return creditcard_no

fake.add_provider(Credit)   
    
def fake_timestamp():
    try:
        boolean_list = ["True", "False"]
        random_num = random.randint(0, 1)
        return boolean_list[random_num]
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_timestamp():
    try:
        minimum = "2020-01-01 05:10:15"
        maximum = "2020-12-31 20:25:30"
        date_format = "%Y-%m-%d %H:%M:%S"
        minimum = datetime.strptime(minimum, date_format)
        maximum = datetime.strptime(maximum, date_format)
        days_count = int((maximum-minimum).days)
        random_num = random.randint(1, days_count)
        gen_timestamp = str(minimum + timedelta(days=random_num, hours=random.randrange(23), 
                                minutes=random.randrange(59), seconds=random.randrange(60)))
        return gen_timestamp
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_date():
    try:
        minimum = "2020-01-01"
        maximum = "2020-12-31"
        date_format = "%Y-%m-%d"
        minimum = datetime.strptime(minimum, date_format)
        maximum = datetime.strptime(maximum, date_format)
        days_count = int((maximum-minimum).days)
        random_num = random.randint(1, days_count)
        gen_date = str(minimum + timedelta(days=random_num))
        return gen_date
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_float():
    try:
        minimum = 1.0
        maximum = 1000.0
        random_num = random.uniform(minimum, maximum)
        return random_num
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_integer():
    try:
        minimum = 1
        maximum = 1000
        random_num = random.randint(minimum, maximum)
        return random_num
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_string(colDict):
    try:
        min_chars = int(colDict["minChars"])
        max_chars = int(colDict["maxChars"])
        strLen = random.randint(min_chars, max_chars)
        gen_str = ''.join(random.choices(
            string.ascii_lowercase + string.ascii_uppercase, k=strLen))
        return gen_str
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_alphanumeric_id():
    try:
        gen_UUID = uuid.uuid4()
        alphanum_Id_str = str(gen_UUID)
        alphanum_Id_list = alphanum_Id_str.split("-")
        alphanum_Id = str(alphanum_Id_list[0])+str(alphanum_Id_list[1])
        return alphanum_Id
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_string_upper(colDict):
    try:
        min_chars = int(colDict["minChars"])
        max_chars = int(colDict["maxChars"])

        

        strLen = random.randint(min_chars, max_chars)
        gen_str = ''.join(random.choices(string.ascii_uppercase, k=strLen))

     
        return gen_str
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_string_lower(colDict):
    try:
        min_chars = int(colDict["minChars"])
        max_chars = int(colDict["maxChars"])
        strLen = random.randint(min_chars, max_chars)
        gen_str = ''.join(random.choices(string.ascii_lowercase, k=strLen))
        return gen_str
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_date_default():
    try:
        minDate = "2021-01-01"
        day = random.randint(1, 365)
        date_format = "%Y-%m-%d"
        minimum = datetime.strptime(minDate, date_format)
        genDate = minimum + timedelta(days=day)
        return genDate
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_mobile():
    try:
        startsWithList = [70, 71, 72, 73, 74, 77, 78, 79, 90, 91,
                          92, 93, 95, 96, 97, 98, 99, 62, 63, 80, 82, 84, 86, 87]
        random_num = random.randint(10000000, 99999999)
        size = len(startsWithList)-1
        indexNum = random.randint(0, size)
        mobNum = str(startsWithList[indexNum])+str(random_num)
        return mobNum
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_gender():
    try:
        genderList = ["Male", "Female"]
        random_num = random.randint(0, 1)
        return genderList[random_num]
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def generate_entity_data(colDict, entity, generation):
    try:
        global logger
        if logger == None:
            logger = logging.getLogger(os.environ.get('LOGGER_NAME'))
        logger.info(loggstr+'inside generate_entity_data function')
        numOfRecords = int(colDict["recordCount"])
        if entity == "numericId":
            minimum = 1
            stepSize = 1
            if "stepSize" in colDict.keys():
                stepSizeStr = ""+str(colDict["stepSize"])
                if len(stepSizeStr) > 0:
                    stepSize = int(stepSizeStr)
            if "minimum" in colDict.keys():
                minimum = int(colDict["minimum"])
                logger.info(f"minimum={minimum}")
                if minimum == 0:
                    minimum = 1
            maxVal = (minimum * numOfRecords * stepSize) + stepSize
            numIdList = np.arange(minimum, maxVal, stepSize)
            colDict["generated"] = numIdList[0:numOfRecords]
            colDict["uniqueValuesCount"] = numOfRecords
            return colDict
        stringDataList = []
        uniqueValuesCount = int(colDict["uniqueValuesCount"])
        if entity.lower() == "gender":
            uniqueValuesCount = 2
        logger.info(f"uniqueValuesCount={uniqueValuesCount}")
        funcNameDict = get_fun_name_dict()
        if uniqueValuesCount > 0:
            func = funcNameDict[entity]
            stringDataList = generate_fake_data(
                func, numOfRecords, stringDataList, entity, colDict)

            stringDataList = list(set(stringDataList))
            # random.shuffle(stringDataList)
            stringListSize = len(stringDataList)
            logger.info(f"list size= {stringListSize}")
            if stringListSize > uniqueValuesCount:
                logger.info("stringListSize > uniqueValuesCount")
                stringDataList = remove_extra_list_elements(
                    stringDataList, uniqueValuesCount)
            elif stringListSize < uniqueValuesCount:
                logger.info("stringListSize < uniqueValuesCount")
                countNotMatch = True
                index = 0
                while countNotMatch:
                    if(index >= 20):
                        break
                    index += 1
                    logger.info("Inside loop")
                    stringDataList = generate_fake_data(
                        func, numOfRecords, stringDataList, entity, colDict)
                    stringDataList = list(set(stringDataList))
                    # random.shuffle(stringDataList)
                    stringListSize = len(stringDataList)
                    logger.info(f"list size= {stringListSize}")
                    if stringListSize > uniqueValuesCount:
                        stringDataList = remove_extra_list_elements(
                            stringDataList, uniqueValuesCount)
                        countNotMatch = False
                        break
                    elif stringListSize == uniqueValuesCount:
                        countNotMatch = False
                        break
                    else:
                        continue
            duplicateCount = numOfRecords - uniqueValuesCount
            # stringDataList= make_duplicates_of_unique(stringDataList, duplicateCount)
            colDict["generated"] = stringDataList[0:numOfRecords]
        logger.info(loggstr+" process completed inside generate_entity_data function:")
        return colDict
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)
        exit()


def generate_fake_data(func, numOfRecords, stringDataList, entity, colDict):
    logger.info("Inside fake generate_fake_data")
    logger.info(f"numOfRecords={str(numOfRecords)} functionName={str(func)}")
    try:
        for i in range(numOfRecords):
            data = eval(func)
            if entity.lower == 'address':
                data = data.replace("\n", " ")
            stringDataList.append(data)
        return stringDataList
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_profiles(numOfRecords):
    logger.info("Inside fake firstnames")
    try:
        dataRecords = []
        for i in range(numOfRecords):
            data = fake.profile()
            dataRecords.append(data)
        return dataRecords
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_account_number(numOfRecords):
    logger.info("Inside fake fake_account_number")
    try:
        dataRecords = []
        for i in range(numOfRecords):
            data = fake.bban()
            dataRecords.append(data)
        return dataRecords
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_phone_number():
    try:
        status = True
        while status:
            data = fake.phone_number()
            data_str = str(data)
            if "." in data_str:
                continue
            elif "x" in data_str:
                data_list = data_str.split("x")
                data = data_list[0]
            break
        return data
    except:
        print_exc()


def get_fun_name_dict():
    try:
        funcNameDict = {
            "address": "fake.address()",
            "firstName": "fake.first_name()",
            "lastName": "fake.last_name()",
            "firstname": "fake.first_name()",
            "lastname": "fake.last_name()",
            "name": "fake.name()",
                    "phoneNumber": "fake_phone_number()",
                    "phoneNumberCountryCode": "fake.phonenumber_countrycode()",
                    "phonenumber": "fake.phonenumber()",
                    "country": "fake.country()",
                    "city": "fake.city()",
                    "company": "fake.company()",
                    "zipCode": "fake.zipcode()",
                    "streetName": "fake.street_name()",
                    "streetname": "fake.street_name()",
                    "email": "fake.email()",
                    "emailId": "fake.email()",
                    "ssn": "fake.ssn()",
                    "ipv4": "fake.ipv4_public()",
                    "ipv6": "fake.ipv6()",
                    "license": "fake.license_plate()",
                    "swiftCode": "fake.swift()",
                    "swiftcode": "fake.swift()",
                    "creditCard": "fake.credit_card_number()",
                    "companyEmail": "fake.company_email()",
                    "CreditCard": "fake.credit_card()",
                    "creditcard": "fake.credit_card_number()",
                    "companyemail": "fake.company_email()",
                    "domainName": "fake.domain_name()",
                    "countryCode": "fake.country_code()",
                    "gender": "fake_gender()",
                    "stringUpper": "fake_string_upper(colDict)",
                    "stringLower": "fake_string_lower(colDict)",
                    "numericId": "fake_numericId()",
                    "alphaNumericId": "fake_alphanumeric_id()",
                    "dateDefault": "fake_date_default()",
                    "mobile": "fake_mobile()",
                    "string": "fake_string(colDict)",
                    "integer": "fake_integer()",
                    "float": "fake_float()",
                    "date": "fake_date()",
                    "timestamp": "fake_timestamp()",
                    "boolean": "fake.pybool()",
                    "alphanumeric": "fake_alphanumeric()",
                    "productName": "fake_x_product_name(colDict)",
                    "productname": "fake_x_product_name(colDict)",
                    "passport": "fake_x_passport()"
        }
        return funcNameDict
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)


def fake_alphanumeric():
    try:
        strlen = random.randint(7, 10)
        gen_str = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=strlen))
        return gen_str
    except:
        print_exc()


def fake_x_passport():
    try:
        regex = "^[0-9]{9}$"
        random_num = generate_number_by_regex(regex)
        return random_num
    except:
        print_exc()


if __name__ == "__main__":

    try:
        logger.info(loggstr+'process started')
        schemaJsonPath = sys.argv[1]
        schemaDict = {}
        with open(schemaJsonPath) as f:
            schemaDict = json.load(f)
        fakeMain(schemaDict)
        logger.info("process completed")
    except:
        print_exc()
        logger.error("Exception occurred", exc_info=True)
