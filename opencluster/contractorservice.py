
from worker import Worker
class ContractorService(Worker) :
    def __init__(self,ctor):
        self.ctor = ctor

    def doTask(self, inhouse):
        return self.giveTask(inhouse)
