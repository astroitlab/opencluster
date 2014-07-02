import logging
import sys
import item
from configuration import Conf
from servicecontext import ServiceContext
from errors import RecallError
logger = logging.getLogger(__name__)

class Workman(object) :
    inetFlag = True

    def __init__(self,host,port,workerType) :
        self.host = host
        self.port = port
        self.workerType = workerType
        self.workerService = ServiceContext.getService(host, port, workerType)
        self.rx = RecallError()

    def setWorker(self, worker):
        #todo InetStart()
        self.__inetStart__()
        self.workerService.setWorker(worker)


    def receive(self, inHouse):
        received = False
        try :
            received = self.workerService.receiveMaterials(inHouse)
        except Exception, e:
            logger.error(e)
        return received

    def getHost(self):
        return self.host

    def getPort(self):
        return self.port

    def doTask(self, inHouse, t = None):
        outHouse = item.WareHouse(False)

        if Conf.getWorkerServiceFlag()==0 and self.rx.tryRecall(inHouse)==-1 :
            return None

        try :
            wh = self.workerService.doTask(inHouse)
            if wh :
                if Conf.getWorkerServiceFlag()==0 :
                    self.rx.setRecall(False)
                outHouse.putAll(wh)
            outHouse.setReady(item.ITEM_READY)

        except Exception, e:
            logger.error(e)
            outHouse.setReady(item.ITEM_EXCEPTION)

        return outHouse

    def doTaskProxy(self,inHouse,outHouse):
        try :
            wh = self.workerService.doTask(inHouse)
            if wh :
                if Conf.getWorkerServiceFlag()==0 :
                    self.rx.setRecall(False)
                outHouse.putAll(wh)
            outHouse.setReady(item.ITEM_READY)

        except Exception, e:
            logger.error(e)
            outHouse.setReady(item.ITEM_EXCEPTION)

        return outHouse

    def interrupt(self):
        try :
            self.worker.stopTask()
        except Exception, e:
            logger.error(e)

    def __inetStart__(self) :
        if self.__class__.inetFlag :
            # BeanContext.startInetServer()
            self.__class__.inetFlag = False
#end
