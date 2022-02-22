# import os
# import sys
import re
# import random
# import logging
from traceback import print_exc
from random import randint


def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)


def recognize_digits(expr):
    try:
        exclusive_digits = []
        inclusive_digits = []
        if "-" in expr:
            expr_arr = expr.split("-")
            digit1 = int(expr_arr[0])
            digit2 = int(expr_arr[1])
            for i in range(10):
                if i >= digit1 and i <= digit2:
                    inclusive_digits.append(i)
                    continue
                exclusive_digits.append(i)
        elif "," in expr:
            for i in range(10):
                if str(i) in expr:
                    inclusive_digits.append(i)
                else:
                    exclusive_digits.append(i)

        return exclusive_digits, inclusive_digits
    except:
        print_exc()


def get_regex_symantics(regex):
    try:
        # print(f"received regex ={regex}")
        expr = ""
        num_of_digits = 0
        index = 0
        while index < len(regex):
            char = regex[index]
            if char == "^" or char == "$":
                index += 1
                continue
            if char == "[":
                index += 1
                char = regex[index]
                while char != "]":
                    expr = expr + char
                    index += 1
                    char = regex[index]
            if char == "{":
                index += 1
                char = regex[index]
                num_of_digits = int(char)
            index += 1
        # print(f"extracted expression:{expr}")
        # print(f"extrcated num of digits :{num_of_digits}")
        return expr, num_of_digits
    except:
        print_exc()


def generate_number_by_regex(regex):
    try:
        expr, num_of_digits = get_regex_symantics(regex)
        exclusive_digits, inclusive_digits = recognize_digits(expr)
        # print(f"inclusive digits:{inclusive_digits}")
        # print(f"exclusive digits :{exclusive_digits}")
        randon_num = random_with_N_digits(num_of_digits)
        return randon_num
    except:
        print_exc()


# # regex = "^[3-8]{5}$"
# regex = "^[1,2,3,4]{5}$"

# randon_num = generate_number_by_regex(regex)
# print(randon_num)
