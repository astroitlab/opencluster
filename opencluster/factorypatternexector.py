import logging
import Queue

from item import *
from factory import FactoryContext
from asyncexector import AsyncExector

logger = logging.getLogger(__name__)

class FactoryPatternExector(object):
    factory = None
    queue = Queue.Queue()
    rpl = None

    @classmethod
    def getFactory(cls):
        if not cls.factory :
            cls.factory = FactoryContext.getDefaultFactory()
        return cls.factory
        
    @classmethod
    def getWorkerTypeList(cls, workType=None):
        if workType is None:
            return cls.getFactory().getNodes()
        return cls.getFactory().getNodes("_worker_" + workType)

    @classmethod
    def getServiceNameList(cls, serviceName):
        return cls.getFactory().getNodes("_service_" + serviceName)

    @classmethod
    def getNodeList(cls):
        return cls.getFactory().getNodes("_node_")

    @classmethod
    def getManagerList(cls):
        return cls.getFactory().getNodes("_manager_")

    @classmethod
    def getServiceList(cls,host=None):
        return cls.getFactory().getNodes("_service_")

    @classmethod
    def createWorkerTypeNode(cls, workerType, nodeValue):
        return cls.getFactory().createDomainNode("_worker_" + workerType, str(time.time()).replace(".", ""), nodeValue,True)

    @classmethod
    def createPhysicalNode(cls, node):
        return cls.getFactory().createDomainNode("_node_", "".join(node.host.split(".")), node,True)

    @classmethod
    def updatePhysicalNode(cls, node):
        return cls.getFactory().createDomainNode("_node_", "".join(node.host.split(".")), node,False)

    @classmethod
    def createServiceNode(cls, host, port, serviceName):
        return cls.getFactory().createDomainNode("_service_" + serviceName, "".join(host.split("."))+str(port), host+":"+str(port),False)

    @classmethod
    def createManager(cls, manager):
        return cls.getFactory().createDomainNode("_manager_", manager.name, manager,False,manager.timeout)

    @classmethod
    def updateManager(cls, manager):
        return cls.getFactory().update("_manager_", manager.name, manager,False)

    @classmethod
    def getLastestObjectBean(cls, ob):
        keys= FactoryObjValue.getDomainNode(ob.getName())
        while True :
            curOb = cls.getFactory().getLatest(keys[0], int(keys[2]), ob)
            if curOb :
                return curOb
                
    @classmethod
    def updateObjectBean(cls,ob, wh):
        keys= FactoryObjValue.getDomainNode(ob.getName())
        return cls.getFactory().update(keys[0], keys[1], wh)
    
    @classmethod
    def append(cls, ppb):
        try:
            ob = cls.getFactory().update(ppb.domain, ppb.node, ppb.inHouse)
            ppb.thisVersion = ob
            cls.queue.put(ppb)

            def task():
                try:
                    curPpb = cls.queue.get()
                    curVersion = cls.getFactory().getLatest()
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
