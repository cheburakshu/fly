import copy
import logging
import asyncio
import threading
import sys

#fly
from .ModelIO import ModelIO
from .ModelConfig import ModelConfig
from pprint import pprint
from .ModelManager import ModelManager
from . import ModelCreate
from . import logSetup

class ModelBootstrap(object):
    def __init__(self,*args,**kwargs):
# Setup log configurations
        logSetup.logSetup(*args,**kwargs)
        self.logger = logging.getLogger(__name__)
        self._loop = asyncio.get_event_loop()
        self._ModelConfig = ModelConfig(kwargs.get('filename'))
        self._modelNames = self._ModelConfig.getModels()
        self._ModelCreate = ModelCreate.ModelCreate(*args,**kwargs)
        self._modelInit = threading.Event()
        self.createModels(*args,**kwargs)
        self._ModelManager = ModelManager()
        self.createConnections(*args,**kwargs)
        self.startModels(*args,**kwargs)
        #self.startThreads()

    def createTasks(self,model=None,executors=1,params=None,*args,**kwargs):
        tasks=[]
        for i in range(executors):
            for func in [self.producer, self.consumer]:
                tasks.append(asyncio.ensure_future(func(*params)))
            if model.getModelType() == 'generator':
                break
        return tasks

    def startModels(self,*args,**kwargs):
        tasks=[]
        for modelName,model in self._ModelManager.getModels().items():
            baseParams = [model, model.getQOut(), model.getQErr(),
                      model.getQOnSuccess(), model.getQOnFailure()]
            if model.getInputPorts() == 'any':
                for q in model.getQIn():
                    resultQ = asyncio.Queue()
                    params = baseParams + [[q],resultQ]
                    tasks = tasks + self.createTasks(model=model,executors=int(model.getThreadCount()),params=params)
            else:
                resultQ = asyncio.Queue()
                params = baseParams + [model.getQIn(),resultQ]
                tasks = tasks + self.createTasks(model=model, executors=int(model.getThreadCount()), params=params)
            self.logger.info('Created Model - %s.', str(modelName))
        self.logger.info('All models created. Launching event loop.')
        loop=asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*tasks))

    async def producer(self,model,qOut,qErr,qOnSuccess,qOnFailure,qIn,resultQ):
        with await asyncio.Lock():
            modelType = model.getModelType()
            while True:
                try:
                    taskInput = {}
                    for q in qIn:
                        taskInput.update(await q.get())
                    await model.getCallable()(resultQ, **taskInput)
                    if modelType == 'generator':
                        break
                except:
                    self.logger.error('%s in Model - %s, Program - %s.%s.%s',str(sys.exc_info()),str(model.getModelName()), model.getModuleName(),model.getClassName(),model.getMethodName())

    async def consumer(self,model,qOut,qErr,qOnSuccess,qOnFailure,qIn,resultQ):
        with await asyncio.Lock():
            modelType = model.getModelType()
            while True:
                try:
                    if modelType == 'sink':
                        break
                    task_result = await resultQ.get()
                    if modelType == 'connection':
                        if task_result.get('onFailure'):
                            del [task_result['onFailure']]
                            for q in qOnFailure:
                                await q.put(task_result)
                        else:
                            for q in qOnSuccess:
                                await q.put(task_result)
                    else:
                        for q in qOut:
                            await q.put(task_result)
                except:
                    self.logger.error('%s in Model - %s, Program - %s.%s.%s',str(sys.exc_info()),str(model.getModelName()), model.getModuleName(),model.getClassName(),model.getMethodName())

    def createModels(self,*args,**kwargs):
        for modelName in self._modelNames:
            self._ModelCreate.create(modelName=modelName,model_init_event=self._modelInit,*args,**kwargs)
 
    def createConnections(self,*args,**kwargs):
        for _connection in self._ModelManager.getModelConnections():
            self._ModelCreate.create(modelName='connection',connectionObject=_connection,model_init_event=self._modelInit,*args,**kwargs)

