import time
import sys
import threading
import asyncio

# fly
from .ModelBootstrap import ModelBootstrap
from . import ModelManager

def bootstrap(_filename,):
#Model Bootstrap
    runForEver = threading.Event()
    mb = ModelBootstrap(filename=_filename,)
    runForEver.wait()

#runForEver = threading.Event()

# Expects a .conf for the model. It should be availble in config folder
#modelConf='calculator.conf' #sys.argv[1]
#bootstrap(modelConf)
# This will wait forever.
# 
#runForEver.wait()
