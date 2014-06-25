import time
import meta
import Pyro4
from parkleader import ParkLeader
from parkservice import ParkService
from item import *
from errors import *
from hbdaemon import HbDaemon

class ParkLocal(object):
    
    sid = None
    
    def __init__(self, host, port, serviceName, servers):
        self.parkLeader = ParkLeader(host, port, serviceName, servers)
        self.park = self.parkLeader.getLeaderPark()
        self.getSessionId(0)

    def getSessionId(self,count=0):
        try :
            if not ParkLocal.sid :
                ParkLocal.sid = self.park.getSessionId()
        except Exception,e :
            if isinstance(e,Pyro4.errors.CommunicationError) :
                logger.error("%s maybe is shutdown,can't connected. Try getNextLeader" % self.parkLeader.thisServer)
                self.park = self.parkLeader.getNextLeader()
                if self.park :
                    self.getSessionId(count+1)

    def createDomain(self, domain, ob):
        self.__put(domain,str(time.time()).replace(".",""),ob,False,0)

    def createDomainNode(self, domain, node, ob, isHeartBeat=False):
        self.__put(domain,node,ob,isHeartBeat,0)

    def __put(self,domain,node,obj,isHearBeat,count):
        ob = None
        if obj is None :
            return None
        if ParkObjValue.checkGrammer(domain) and ParkObjValue.checkGrammer(node) :
            try:
                ov = self.park.create(domain,node,obj,ParkLocal.sid,isHearBeat)
                ob = self.ovToBean(ov,domain,node)
                if ob and isHearBeat :
                    HbDaemon.runPutTask(self.park,self.parkLeader,domain,node,ParkLocal.sid)
            except Exception,e :
                logger.error(e)
                if isinstance(e,Pyro4.errors.CommunicationError):
                    self.park = self.parkLeader.getNextLeader()
                    if self.park :
                        ob = self.__put(domain,node,obj,isHearBeat,count+1)
                if isinstance(e,ClosetoOverError) :
                    logger.error("put error:%s" % ClosetoOverError(e).errorPrint())
        return ob

    def update(self, domain, node, obj,count=0):
        ob = None
        if obj is None :
            return None
        if ParkObjValue.checkGrammer(domain) and ParkObjValue.checkGrammer(node) :
            try:
                ov = self.park.update(domain,node,obj,ParkLocal.sid)
                ob = self.ovToBean(ov,domain,node)

            except Exception,e :
                logger.error(e)
                if isinstance(e,Pyro4.errors.CommunicationError):
                    self.park = self.parkLeader.getNextLeader()
                    if self.park :
                        ob = self.update(domain,node,obj,count+1)
                if isinstance(e,ClosetoOverError) :
                    logger.error("update error:%s" % ClosetoOverError(e).errorPrint())

        return ob
    
    def getNodes(self, domain,count=0):
        objList = None
        if ParkObjValue.checkGrammer(domain) :
            try:
                ov = self.park.get(domain,None,ParkLocal.sid)
                objList = self.ovToBeanList(ov,domain)

            except Exception,e :
                logger.error(e)
                if isinstance(e,Pyro4.errors.CommunicationError):
                    self.park = self.parkLeader.getNextLeader()
                    if self.park :
                        ob = self.getNodes(domain,count+1)
                if isinstance(e,ClosetoOverError) :
                    logger.error("get Nodes error:%s" % ClosetoOverError(e).errorPrint())

        return objList


    def getDomainNode(self, domain, node,count=0):
        ob = None
        if ParkObjValue.checkGrammer(domain) and ParkObjValue.checkGrammer(node):
            try:
                ov = self.park.get(domain,node,ParkLocal.sid)
                ob = self.ovToBean(ov,domain,node)

            except Exception,e :
                logger.error(e)
                if isinstance(e,Pyro4.errors.CommunicationError):
                    self.park = self.parkLeader.getNextLeader()
                    if self.park :
                        ob = self.get(domain,node,count+1)
        return ob

    def getLastest(self, domain, node, oldObj,count=0):
        ob = None
        if ParkObjValue.checkGrammer(domain) and ParkObjValue.checkGrammer(node):
            try:
                vid = 0l
                if oldObj :
                    vid = oldObj.vid

                ov = self.park.getLastest(domain,node,ParkLocal.sid,vid)
                ob = self.ovToBean(ov,domain,node)

            except Exception,e :
                logger.error(e)
                if isinstance(e,Pyro4.errors.CommunicationError):
                    self.park = self.parkLeader.getNextLeader()
                    if self.park :
                        ob = self.getLastest(domain,node,oldObj,count+1)
        return ob
    def getNodesLastest(self, domain, oldObjList,count=0):
        obList = None
        if ParkObjValue.checkGrammer(domain):
            try:
                vid = 0l
                if oldObjList :
                    vid = oldObjList.vid

                ov = self.park.getLastest(domain,None,ParkLocal.sid,vid)
                obList = self.ovToBeanList(ov,domain)

            except Exception,e :
                logger.error(e)
                if isinstance(e,Pyro4.errors.CommunicationError):
                    self.park = self.parkLeader.getNextLeader()
                    if self.park :
                        ob = self.getNodesLastest(domain,oldObjList,count+1)
                if isinstance(e,ClosetoOverError) :
                    logger.error("getNodesLastest error:%s" % ClosetoOverError(e).errorPrint())
        return obList

    def delete(self, domain, node,count=0):
        ob = None
        if ParkObjValue.checkGrammer(domain) and ParkObjValue.checkGrammer(node):
            try:
                ov = self.park.delete(domain,node,ParkLocal.sid)
                ob = self.ovToBean(ov,domain,node)

            except Exception,e :
                logger.error(e)
                if isinstance(e,Pyro4.errors.CommunicationError):
                    self.park = self.parkLeader.getNextLeader()
                    if self.park :
                        ob = self.delete(domain,node,count+1)
        return ob
    def deleteDomain(self, domain, count=0):
        obList = None
        if ParkObjValue.checkGrammer(domain):
            try:
                ov = self.park.delete(domain,None,ParkLocal.sid)
                obList = self.ovToBeanList(ov,domain)

            except Exception,e :
                logger.error(e)
                if isinstance(e,Pyro4.errors.CommunicationError):
                    self.park = self.parkLeader.getNextLeader()
                    if self.park :
                        ob = self.deleteDomain(domain,count+1)
                if isinstance(e,ClosetoOverError) :
                    logger.error("deleteDomain Nodes error:%s" % ClosetoOverError(e).errorPrint())
        return obList

    def setDeletable(self, domain,obj=None):
        pass

    def addLastestListener(self, domain, obj):
        pass


    def ovToBean(self,ov,domain,node):
        if ov and ov.isEmpty() :
            domainNodeKey = ParkObjValue.getDomainNodekey(domain,node)
            bean = ObjectBean()
            bean.name = domainNodeKey
            bean.obj = ov.getObj(domainNodeKey)
            return bean
        else :
            return None

    def ovToBeanList(self,ov,domain) :
        if ov and not ov.isEmpty() :
            objList = ObjectBeanList(ov.getObj(meta.getMetaVersion(domain)))
            nodeVersion = ov.getWidely(meta.getMetaVersion(domain + "..*"))

            for key in nodeVersion.keys() :
                obp = ObjectBean()
                obp.vid = long(nodeVersion.getObj(key))
                obp.name = key[0:key.find(meta.METAVERSION)]
                obp.obj = ov.getObj(obp.name)
                objList.append(obp)
            return objList



