from item import *
from beancontext import BeanContext

class ParkPatternExector(object):
    park = None
    
    @classmethod
    def getPark(cls):
        if not cls.park : 
            cls.park = BeanContext.getDefaultPark()
        return cls.park
        
    @classmethod
    def getWorkerTypeList(cls, workType):
        return cls.getPark().getNodes("_worker_" + workType)
        
    @classmethod
    def createWorkerTypeNode(cls, workerType, nodeValue):
        return cls.getPark().createDomainNode("_worker_" + workerType, str(time.time()).replace(".", ""), nodeValue)
    
    @classmethod
    def getLastestObjectBean(cls, ob):
        keys= ParkObjValue.getDomainNode(ob.getName())
        while True :
            curOb = cls.getPark().getLatest(keys[0], int(keys[2]), ob)
            if ob :
                return ob
                
    @classmethod
    def updateObjectBean(cls,ob, wh):
        keys= ParkObjValue.getDomainNode(ob.getName())
        return cls.getPark().update(keys[0], keys[1], wh)
    
    @classmethod
    def append(cls, obj):
        pass

#end
