import sys


from configuration import Conf
from workerparallel import WorkerParallel
from beancontext import BeanContext
from parkpatternexector import ParkPatternExector

class Worker(WorkerParallel):
    
    software = ['numpy','scipy']
    hardware = {'cpu':0, 'mem':0}
    
    def __init__(self):
        super(Worker,self).__init__()
        self.host = None
        self.port = None
        self.workerType = None
        self._interrupted = False
    
    def setMigrantWorker(self, mw):
        pass
    def doTask(self, inhouse):
        pass
    def stopTask(self):
        pass
    
    def waitWorkingByService(self, host, port, workerType) :
        self.host = host
        self.port = port
        self.workerType = workerType        
        BeanContext.startWorker(host, port, workerType, self)
        # ParkPatternExector.createWorkerTypeNode(workerType, host+":"+str(port))

    def waitWorkingByPark(self, workerType):
        ob = ParkPatternExector.createWorkerTypeNode(workerType, "wk_pk")
        while True :
            lastestOb = ParkPatternExector.getLastestObjectBean(ob)
            whouse = self.doTask(lastestOb)
            ob = ParkPatternExector.updateObjectBean(lastestOb, whouse)
        
    def getWorkerIndex(self, index, workerType=None):
        if not workerType :
            return self.getWorkerIndex(index, self.workerType)
            
        wsList = self.getWorkersService(workerType)
        if index >=0 and index < len(wsList) :
            wsInfo = wsList[index]
            return BeanContext.getWorkman(wsInfo[0], int(wsInfo[1]), wsInfo[2])
        
    
    
    def getWorkerAll(self, workerType=None):
        if not workerType : 
            return self.getWorkers(None, 0, self.workerType)
        return self.getWorkers(None, 0, workerType)
    
    def getWorkers(self, host, port, workerType):
        wkList = []
        if WorkerParallel.computeModeFlag == 0 :
            wsList = self.getWorkersService(workerType)
            i = 0
            for wsInfo in wsList :
                if self.host != wsInfo[0] and self.port != int(wsInfo[1]) :
                    wkList.append(BeanContext.getWorkman(wsInfo[0], int(wsInfo[1]), wsInfo[2]))
                else :
                    self.selfIndex = i
                i = i + 1    
        return wkList
    def getSelfIndex(self):
        if self.selfIndex == -1 :
            wsList = self.getWorkersService(self.workerType)
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
        wsList = self.getWorkersService(workerType)
        for wsInfo in wsList :
            if self.host == wsInfo[0] and self.port == int(wsInfo[1]) :
                return BeanContext.getWorkman(wsInfo[0], int(wsInfo[1]), wsInfo[2])
        return None
        
    def receive(self, inhouse):
        return True
        
    def receiveMaterials(self, inhouse):
        return self.receive(inhouse)
        
    def setSelfIndex(self, i):
        self.selfIndex = i
        
    def isInterrupted(self):
        return self._interrupted
    
    def interrupted(self,  interrupted) :
        self._interrupted = interrupted
        
if __name__ == "__main__" :
    serverStr = Conf.getWorkerServers()
    server = serverStr.split(":")

    if len(sys.argv) != 2:
        print "no worker type specified\n usage:python worker.py [WorkerType]"
        exit(0)
    wk = Worker()
    wk.waitWorking(sys.argv[1],server[0],int(server[1]))