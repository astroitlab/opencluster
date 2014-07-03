from errors import RecallError
from item import *
from parkpatternexector import ParkPatternExector
class WorkerLocal(object) :

    def __init__(self,domainNodeKey):
        self.domainNodeKey = domainNodeKey
        self.rx = RecallError()

    def doTask(self,inHouse):
        if self.rx.tryRecall(inHouse) == -1 :
            return None

        outHouse = WareHouse(False)

        keys = ParkObjValue.getDomainNodekey(self.domainNodeKey)
        ppb = ParkPatternBean(keys[0],keys[1],inHouse,outHouse,self.rx)
        ParkPatternExector.append(ppb)
        return outHouse

