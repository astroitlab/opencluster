import logging

from configuration import Conf


logger = logging.getLogger(__name__)

class WorkService(object):
    
    def __init__(self, worker):
        
        self.software = ['numpy','scipy', 'matplotlib']
        self.hardware = {'cpu':0, 'mem':0}
        self.worker = worker

    def setWorker(self, worker):
        self.worker.host = worker.host
        self.worker.port = worker.port
        self.worker.workerType = worker.workerType
        
        self.worker = worker
    def doTask(self, inHouse):
        wh = None
        try :
            if Conf.getWorkerServiceFlag() == "0" :
                self.worker.interrupted(False)
            wh = self.worker.doTask(inHouse)
        finally :
            if Conf.getWorkerServiceFlag() == "0" :
                #unlock
                pass
        return wh
    
    def stopTask(self):
        if Conf.getWorkerServiceFlag() == "0":
            self.worker.interrupted(True)
        else :
            raise Exception("Worker public service status cant be interrupted!")
    
    def receiveMaterials(self, inHouse):
        return self.worker.receiveMaterials(inHouse)
        
    
        
