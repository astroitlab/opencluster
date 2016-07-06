import datetime
import random
import logging

from opencluster.configuration import setLogger
from opencluster.workerparallel import WorkerParallel
from opencluster.factory import FactoryContext
from opencluster.util import port_opened
from opencluster.factorypatternexector import FactoryPatternExector

logger = logging.getLogger(__file__)
class Worker(WorkerParallel):
    def __init__(self,workerType, level=logging.DEBUG):
        super(Worker,self).__init__()
        self.host = None
        self.port = None
        self.workerType = workerType
        self._interrupted = False
        self.level = level
        j = str(random.randint(0, 9000000)).ljust(7,'0')
        workerId = "%s%s" % (datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"),j)
        setLogger(self.workerType,workerId,self.level)

    def doTask(self, task):
        pass
    def stopTask(self):
        pass
    
    def waitWorkingByService(self, host, port=None) :
        self.host = host
        self.port = port

        if self.host is None:
            self.host = "127.0.0.1"

        if self.port is None:
            self.port = random.randint(30000, 40000)

        while port_opened(self.host, self.port) :
            self.port = random.randint(30000, 40000)

        FactoryContext.startWorker(self, self.workerType, self.host, self.port)

    def getWorkerIndex(self, index, workerType=None):
        if not workerType :
            return self.getWorkerIndex(index, self.workerType)
            
        wsList = self.getWorkerList(workerType)
        if index >= 0 and index < len(wsList) :
            wsInfo = wsList[index]
            return FactoryContext.getWorker(wsInfo[0], int(wsInfo[1]), wsInfo[2])

    def getWorkerAll(self, workerType=None):
        if not workerType : 
            return self.getWorkers(None, 0, self.workerType)
        return self.getWorkers(None, 0, workerType)
    
    def getWorkers(self, host, port, workerType):
        wkList = []
        wsList = self.getWorkerList(workerType)
        i = 0
        for wsInfo in wsList :
            if self.host != wsInfo[0] and self.port != int(wsInfo[1]) :
                wkList.append(FactoryContext.getWorker(wsInfo[0], int(wsInfo[1]), wsInfo[2]))
            else :
                self.selfIndex = i
            i = i + 1
        return wkList
    def getSelfIndex(self):
        if self.selfIndex == -1 :
            wsList = self.getWorkerList(self.workerType)
            i = 0
            for wsInfo in wsList :
                if self.host == wsInfo[0] and self.port == int(wsInfo[1]) :
                    return i
                i = i + 1
                
        return self.selfIndex
    
    def getWorkerElse(self, workerType=None, host=None, port = None):
        if not workerType :
            workerType = self.workerType
            
        if host is None or port is None :
            return self.getWorkers(self.host, self.port, workerType)
        
        
        if self.host == host and self.port == port :
            return None
        wsList = self.getWorkerList(workerType)
        for wsInfo in wsList :
            if self.host == wsInfo[0] and self.port == int(wsInfo[1]) :
                return FactoryContext.getWorker(wsInfo[0], int(wsInfo[1]), wsInfo[2])
        return None
        
    def receive(self, task):
        return True

    def setSelfIndex(self, i):
        self.selfIndex = i
        
    def isInterrupted(self):
        return self._interrupted
    
    def interrupted(self,  interrupted) :
        self._interrupted = interrupted

    def getServices(self,serviceName, asynchronous = False, worker = None):
        wslist = self.getServiceList(serviceName)
        wklist = []
        for wsinfo in wslist :
            wklist.append(FactoryContext.getWorker(wsinfo[0],int(wsinfo[1]),wsinfo[2],asynchronous))

        return  wklist

    def getWorkerList(self, workerType, host=None, port=None):
        obList = FactoryPatternExector.getWorkerTypeList(workerType);
        wsList = []
        if not obList is None :
            for obj in obList :
                hostPort = str(obj.obj).split(":")
                if hostPort[0] != host or int(hostPort[1]) != port:
                    wsList.append([hostPort[0], hostPort[1], workerType])
        return wsList

    def getServiceList(self, serviceName, host=None, port=None):
        obList = FactoryPatternExector.getServiceNameList(serviceName);
        wsList = []
        if not obList is None :
            for obj in obList :
                hostPort = str(obj.obj).split(":")
                if hostPort[0] != host or int(hostPort[1]) != port:
                    wsList.append([hostPort[0], hostPort[1], serviceName])
        return wsList