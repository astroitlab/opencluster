
class WorkerParallel(object) :
    def waitWorking(self,host=None, port=None):
        self.waitWorkingByService(host, port)

    def waitWorkingByService(self, host, port) :
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
