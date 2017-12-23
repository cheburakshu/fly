class ModelConnections(object):
    def __init__(self,*args,**kwargs):
        pass

    async def connection(self,resultQ,*args,**kwargs):
#    def connection(self,*args,**kwargs):
        await resultQ.put(kwargs)
        return kwargs
