import asyncio
from concurrent.futures import ThreadPoolExecutor
import time

#

class ModelConnections(object):
    def __init__(self,*args,**kwargs):
        pass

    async def connection(self,resultQ,loop,*args,**kwargs):
        loop=asyncio.get_event_loop()

        def qPut(q,kwargs):
            q.put(kwargs)

        with ThreadPoolExecutor(max_workers=100) as executor:
            await loop.run_in_executor(None, qPut, resultQ, kwargs)
