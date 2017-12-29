import queue
import random
import time
import unittest
import asyncio
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor

class calculator(object):
    def __init__(self,*args,**kwargs):
        self._a = None
        self._b = None
        self._result = None

    async def arange(self,n):
        for i in range(n):
            yield i
            asyncio.sleep(0)

    async def input(self,resultQ,loop,*args,**kwargs):
        def qPut(q):
            for i in range(10000):
                q.put({'n': i})
        with ThreadPoolExecutor(max_workers=10000) as executor:
            await loop.run_in_executor(executor, qPut, resultQ)


    async def print(self,resultQ,loop,*args,**kwargs):
        def qGet(q):
            if kwargs.get('n') % 1000 == 0:
                print('1k records processed')
        if kwargs.get('n') % 1000 == 0:
            print('1k records processed')
        await asyncio.sleep(0)
        #await loop.run_in_executor(None, qGet, resultQ)

