import logging
import logging.config
import time

def logSetup(*args,**kwargs):
    LOG_PATH = 'log/'
    fileName = LOG_PATH + kwargs.get('filename') + '.' + str(int(time.time())) + '.out'
    logging.config.fileConfig('config/logging.conf',defaults={'logfilename': fileName})
