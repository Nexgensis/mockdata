import logging
import os
import warnings
from traceback import print_exc

warnings.filterwarnings("ignore", category=FutureWarning)

def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    try:
        fileHandler = logging.FileHandler(log_file, mode='w')
        fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

        l.setLevel(level)
        l.addHandler(fileHandler)
        l.addHandler(streamHandler)
    except :
        print_exc()


# setup_logger('log1', txtName+"txt")
# setup_logger('log2', txtName+"small.txt")
# logger_1 = logging.getLogger('log1')
# logger_2 = logging.getLogger('log2')