import logging
import Queue

from item import *
from beancontext import BeanContext
from asyncexector import AsyncExector

logger = logging.getLogger(__name__)

class ParkPatternExector(object):
    park = None
    queue = Queue.Queue()
    rpl = None

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
        return cls.getPark().createDomainNode("_worker_" + workerType, str(time.time()).replace(".", ""), nodeValue,True)
    
    @classmethod
    def getLastestObjectBean(cls, ob):
        keys= ParkObjValue.getDomainNode(ob.getName())
        while True :
            curOb = cls.getPark().getLatest(keys[0], int(keys[2]), ob)
            if curOb :
                return curOb
                
    @classmethod
    def updateObjectBean(cls,ob, wh):
        keys= ParkObjValue.getDomainNode(ob.getName())
        return cls.getPark().update(keys[0], keys[1], wh)
    
    @classmethod
    def append(cls, ppb):
        try:
            ob = cls.getPark().update(ppb.domain, ppb.node, ppb.inHouse)
            ppb.thisVersion = ob
            cls.queue.put(ppb)


            def task():
                try:
                    curPpb = cls.queue.get()
                    curVersion = cls.getPark().getLatest()
                    if not curVersion :
                        curPpb.thisVersion = curVersion
                        curPpb.rx.setRecall(False)
                        curPpb.outhouse.putAll(curVersion)
                        curPpb.outhouse.setReady(ITEM_READY)
                    else:
                        cls.queue.put(curPpb)
                except Exception,e1 :
                    logger.error(e1)

            if cls.rpl is None :
                cls.rpl = AsyncExector(task , ())
                cls.rpl.run()


        except Exception,e :
            logger.error(e)


#end
