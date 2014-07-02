import logging
from workerparallel import WorkerParallel
from parallelservice import ParallelService
from beancontext import BeanContext
from parkpatternexector import ParkPatternExector
from contractorservice import ContractorService

from item import *

logger = logging.getLogger(__name__)
class Contractor(WorkerParallel) :
    def __init__(self):
        self.ctor = None
        workers = []

    def toNext(self,ctor):
        self.ctor = ctor
        return ctor

    def giveTask(self,inHouse):
        pass

    def giveChainTask(self,inHouse):
        outHouse = self.giveTask(inHouse)
        if self.ctor :
            return self.ctor.giveChainTask(outHouse)
        return  outHouse

    def doProject(self,inHouse):
        self.giveTask(inHouse)

    def getWaitingWorkers(self,workerType,worker = None):
        if ParallelService.computeModeFlag == 0 :
            return self.__getWaitingWorkersFromService(workerType,worker)
        else:
            return self.__getWaitingWorkersFromPark(workerType)

    def __getWaitingWorkersFromService(self,workerType,worker = None):
        logger.info("getWaitingWorkersFromService:%s"%workerType)

        wslist = self.getWorkersService(workerType)
        wklist = []

        for wsinfo in wslist :
            wklist.append(BeanContext.getWorkman(wsinfo[0],int(wsinfo[1]),wsinfo[2]))

        return  wklist

    def __getWaitingWorkersFromPark(self,workerType):
        logger.info("getWaitingWorkersFromPark:%s"%workerType)

        oblist = ParkPatternExector.getWorkerTypeList(workerType)
        wklist = []
        for ob in oblist :
            wklist.append(BeanContext.getWorkman(ob.getName()))

        return  wklist

    def waitWorking(self, workerType, host=None, port=None):
        ctors = ContractorService(self)
        ctors.waitWorking(workerType,host,port)

    # def map(self,whHouses,workers=[]):

    def doTaskBatch(self,whHouse,workers = []):
        whHouseArray = []
        i = 0
        while i < workers.__len__() :
            whHouseArray.append(None)
            i += 1
        i = 0
        j = 0
        while j < workers.__len__() :
            if whHouseArray[i] is None :
                whHouseArray[i] = workers[i].doTask(whHouse)
            elif whHouseArray[i].ready and whHouseArray[i].mark:
                whHouseArray[i].mark = False
                j += 1
            i += 1
            if i == whHouseArray.__len__() :
                i = 0
        return whHouseArray