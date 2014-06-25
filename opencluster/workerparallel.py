
from parallelservice import ParallelService
class WorkerParallel(ParallelService) :

    def waitWorking(self, workerType, host=None, port=None):
        if ParallelService.computeModeFlag == 1 :
            self.waitWorkingByPark(workerType)
        else :
            self.waitWorkingByService(host, port, workerType)

    def waitWorkingByService(self, host, port, workerType) :
        pass
    def waitWorkingByPark(self,workerType):
        pass
    def getWorkerElse(self, workerType=None, host=None, port = None):
        pass
    def getWorkerIndex(self, index, workerType=None) :
        pass
    def getWorkerAll(self,workerType=None):
        pass
    def getSelfIndex(self):
        pass
    def receive(self,inhouse):
        pass
