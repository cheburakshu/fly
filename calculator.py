import queue
import random
import time
import unittest
import asyncio

class calculator(object):
    def __init__(self,*args,**kwargs):
        self._a = None
        self._b = None
        self._result = None

    async def add(self,resultQ,*args,**kwargs):
        with await asyncio.Lock():
            sleep=random.random()
            #await asyncio.sleep(sleep/1000)
            self._result = kwargs.get('value1') + kwargs.get('value2')
            await resultQ.put({'v1_add':kwargs.get('value1'),'v2_add':kwargs.get('value2'),'add':self._result})

    async def arange(self,n):
        for i in range(n):
            yield i
            asyncio.sleep(0)

    async def input(self,resultQ,*args,**kwargs):
        async for i in self.arange(100000):
            self._a = i #random.randint(0,9999)
            self._b = i #random.randint(0,9999)
            await resultQ.put({'value1':self._a,'value2':self._b})

    async def print(self,resultQ,*args,**kwargs):
        if kwargs.get('v1_add') % 5000 == 0:
            print('processed 1k records')
        await asyncio.sleep(0)
        #print({'value1':kwargs.get('v1_add'),'value2':kwargs.get('v1_add'),'add':kwargs.get('add')})
        #print({'value1':kwargs.get('v1_sub'),'value2':kwargs.get('v2_sub'),'sub':kwargs.get('sub')})

    async def subtract(self,resultQ,*args,**kwargs):
        with await asyncio.Lock():
            self._result = kwargs.get('value1') - kwargs.get('value2')
            await resultQ.put({'v1_sub':kwargs.get('value1'),'v2_sub':kwargs.get('value2'),'sub':self._result})

class mUnitTest(unittest.TestCase):
    def setUp(self):
        self.calculator = calculator()
        self.resultQ = queue.Queue()

    def testAdd(self):
        self.calculator.add(self.resultQ,value1=1,value2=2)
        result = self.resultQ.get_nowait()
        self.assertEqual({'op1':3},result)

    def testSubtract(self):
        self.calculator.subtract(self.resultQ,value1=1,value2=2)
        result = self.resultQ.get_nowait()
        self.assertEqual({'op2':-1},result)
 
    def testInput(self):
        self.calculator.input(self.resultQ)
        result = self.resultQ.get_nowait()
        self.assertIsNotNone(result)

#    def tearDown(self):
#        self.calculator.dispose()
#        self.resultQ.dispose()

if __name__ == '__main__':
    unittest.main()
