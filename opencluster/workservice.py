import logging
import threading

from configuration import Conf


logger = logging.getLogger(__name__)

class WorkService(object):
    
    def __init__(self, worker):
        
        self.software = ['numpy','scipy', 'matplotlib']
        self.hardware = {'cpu':0, 'mem':0}
        self.worker = worker
        self.condition = threading.Condition()

    def setWorker(self, worker):
        worker.host = self.worker.host
        worker.port = self.worker.port
        worker.workerType = self.worker.workerType
        
        self.worker = worker
    def doTask(self, inHouse):
        wh = None
        try :
            if Conf.getWorkerServiceFlag() == "0" :
                if self.condition.acquire() :
                    self.worker.interrupted(False)
            wh = self.worker.doTask(inHouse)
        finally :
            if Conf.getWorkerServiceFlag() == "0" :
                self.condition.release()
        return wh
    
    def stopTask(self):
        if Conf.getWorkerServiceFlag() == "0":
            self.worker.interrupted(True)
        else :
            raise Exception("Worker public service status cant be interrupted!")
    
    def receiveMaterials(self, inHouse):
        return self.worker.receiveMaterials(inHouse)
        
    
        
