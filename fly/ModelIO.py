import queue
import threading
import uuid
#import ThreadIO
#import SemaphoreIO
#import EventIO
#import SubscribeIO
import copy
from pprint import pprint
#Setup log
import logging


#fly
from . import QueueIO
from . import ModelImport
from .ModelManager import ModelManager
from .ModelConfig import ModelConfig
from . import ModelConnections

class ModelInit(object):
    def __init__(self,*args,**kwargs):
#Setup lg
        self.logger = logging.getLogger(__name__)
        self._id =  uuid.uuid1().int
        self._modelId = kwargs.get('modelId')
        self._type = 'MODEL'
        self._modelName = kwargs.get('modelName')
        self._qIO = QueueIO.QueueIO(*args,**kwargs)
        self._className = kwargs.get('class')
        self._methodName = kwargs.get('method')
        if kwargs.get('type') == 'connection':
            self._module = ModelConnections
        else:
            self._module = ModelImport.ModelImport().importModule(kwargs.get('module'))
        self._object = getattr(self._module, self._className)()
        self._callable = getattr(self._object,self._methodName)
        self._moduleName = kwargs.get('module')
        self._modelType = kwargs.get('type')
        self._input_ports = kwargs.get('input_ports')
        self._threadCount = kwargs.get('thread_count')
        #self._ModelTarget = ModelTarget.ModelTarget(q=self.getQ(),*args,**kwargs)
        #self._semaphoreIO = SemaphoreIO.SemaphoreIO(*args,**kwargs)
        #self._lowLoadSemaIO = SemaphoreIO.SemaphoreIO(lowLoadSema=True,*args,**kwargs)
        #self._threadIO = ThreadIO.ThreadIO(target=self.getModelTarget(),semaphore=self.getSemaphore().getSemaphore(),*args,**kwargs)
        #self._threadIO = ThreadIO.ThreadIO(moduleObj=self._module,q=self.getQ(),semaphore=self.getSemaphore().getSemaphore().getObj(),lowLoadSema=self.getLowLoadSema().getSemaphore().getObj(),*args,**kwargs)
        #self._eventIO = EventIO.EventIO(*args,**kwargs)
        #self._subscribeIO = SubscribeIO.SubscribeIO(*args,**kwargs)

    def getModuleName(self):
        return self._moduleName

    def getClassName(self):
        return self._className

    def getMethodName(self):
        return self._methodName

    def getThreadCount(self):
        return self._threadCount

    def getQIn(self):
        return self._qIO.getQIn().getObj()

    def getQOut(self):
        return self._qIO.getQOut().getObj()

    def getQErr(self):
        return self._qIO.getQErr().getObj()

    def getQOnSuccess(self):
        return self._qIO.getQOnSuccess().getObj()

    def getQOnFailure(self):
        return self._qIO.getQOnFailure().getObj()

    def getModelType(self):
        return self._modelType

    def getInputPorts(self):
        return self._input_ports

    def getCallable(self):
        return self._callable

    def getId(self):
        return self._id

    def getType(self):
        return self._type

    def getModelName(self):
        return self._modelName

    def getQ(self):
        return self._qIO

    def getThread(self):
        return self._threadIO

    def getLowLoadSema(self):
        return self._lowLoadSemaIO

    def getSemaphore(self):
        return self._semaphoreIO

    def getEvent(self):
        return self._eventIO

    def getSubscribe(self):
        return self._subscribeIO

    def getModelId(self):
        return self._modelId

    def getQIn(self):
        result = []
        for model,ports in self._qIO.getQIn().items():
            for port,queues in ports.items():
                if queues:
                    result = result + list(map(lambda x:x.getObj(),queues))
        return result

    def getQOut(self):
        result = []
        for model,ports in self._qIO.getQOut().items():
            for port,queues in ports.items():
                if queues:
                    result = result + list(map(lambda x:x.getObj(),queues))
        return result

    def getQErr(self):
        result = []
        for model,ports in self._qIO.getQErr().items():
            for port,queues in ports.items():
                if queues:
                    result = result + list(map(lambda x:x.getObj(),queues))
        return result

    def getQOnSuccess(self):
        result = []
        for model,ports in self._qIO.getQOnSuccess().items():
            for port,queues in ports.items():
                if queues:
                    result = result + list(map(lambda x:x.getObj(),queues))
        return result

    def getQOnFailure(self):
        result = []
        for model,ports in self._qIO.getQOnFailure().items():
            for port,queues in ports.items():
                if queues:
                    result = result + list(map(lambda x:x.getObj(),queues))
        return result

class ModelIO(object):
    def __init__(self,*args,**kwargs):
#Setup log
        self.logger = logging.getLogger(__name__)
        self._subRefs = {}
        self._objRefs = {}
        self._modelRefs = {}

        self._modelName = kwargs.get('modelName')
        self._objectList = ['queue','thread','semaphore','event','subscribe']
        self._ModelConfig = ModelConfig(kwargs.get('filename')) #*args,**kwargs)
        self._modelManager = ModelManager()

        self._id =  uuid.uuid1().int
        self._modelAttributes = self._ModelConfig.getModelAttributes(self.getModelName())
        self._OnSuccess = self.getModelAttributes().get('OnSuccess')
        self._OnFailure = self.getModelAttributes().get('OnFailure')
        self._model_type = self.getModelAttributes().get('type')
        self._model = ModelInit(modelId=self.getId(),*args,**kwargs,**self._modelAttributes,ports=self._ModelConfig.getModelInputPorts(self.getModelName()))
        self.setModelRefs()

        if (self.getModelType() == 'connection'):
            self._modelManager.setConnectionRefs(modelRefs=self.getModelRefs(),connectionObject=kwargs.get('connectionObject'))
            self._modelManager.setConnectionModels(model=self.getModel(),connectionObject=kwargs.get('connectionObject'))
        else:
            self._modelManager.setModelRefs(objRefs=self.getObjRefs(),modelName=self.getModelName()) 
            self._modelManager.setModels(model=self.getModel(),modelName=self.getModelName()) 

    def getModelType(self):
        return self._model_type

    def getModelAttributes(self):
        return self._modelAttributes

    def getId(self):
        return self._id

    def getModelName(self):
        return self._modelName

    def getModel(self):
        return self._model

    def getOnSuccess(self):
        return self._OnSuccess

    def getOnFailure(self):
        return self._OnFailure

    #def formDict(self,key,value):
    #    return dict(zip([key],[value]))

    def setModelRefsForObjects(self,object):
        if (object == 'queue'):
            self._subRefs ['subref'] = self.getModel().getQ().getSubRefs() 
            self._subRefs ['objectid'] = self.getModel().getQ().getId()
        # elif (object == 'thread'):
        #     self._subRefs ['subref'] = self.getModel().getThread().getSubRefs()
        #     self._subRefs ['objectid'] = self.getModel().getThread().getId()
        # elif (object == 'semaphore'):
        #     self._subRefs ['subref'] = self.getModel().getSemaphore().getSubRefs()
        #     self._subRefs ['objectid'] = self.getModel().getSemaphore().getId()
        # elif (object == 'event'):
        #     self._subRefs ['subref'] = self.getModel().getEvent().getSubRefs()
        #     self._subRefs ['objectid'] = self.getModel().getEvent().getId()
        # elif (object == 'subscribe'):
        #     self._subRefs ['subref'] = self.getModel().getSubscribe().getSubRefs()
        #     self._subRefs ['objectid'] = self.getModel().getSubscribe().getId()
        else:
            return

        self._objRefs ['modelId'] = self.getId()

        if (self.getOnSuccess()):
            self._objRefs ['OnSuccess'] = list(self.getOnSuccess().split(sep=',')) #kwargs['next'].split(sep=',')
        else:
            self._objRefs ['OnSuccess'] = []

        if (self.getOnFailure()):
            self._objRefs ['OnFailure'] = list(self.getOnFailure().split(sep=',')) #kwargs['next'].split(sep=',')
        else:
            self._objRefs ['OnFailure'] = []

        self._objRefs ['model_type'] = self.getModelType()
        self._objRefs [object] = copy.deepcopy(self._subRefs)
        self._modelRefs [self.getModelName()] = copy.deepcopy(self._objRefs)

    def getObjectList(self):
        return self._objectList

    def setModelRefs(self):
        return list(map(self.setModelRefsForObjects,self.getObjectList()))

    def getObjRefs(self):
        return self._objRefs

    def getModelRefs(self):
        return self._modelRefs
