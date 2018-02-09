import random
import traceback

from Pyro4.errors import CommunicationError
from opencluster.factoryleader import FactoryLeader
from opencluster.item import *
from opencluster.errors import *
from opencluster.hbdaemon import HbDaemon

class FactoryLocal(object):
    

    def __init__(self, host, port, serviceName, servers):
        self.factoryLeader = FactoryLeader(host, port, serviceName, servers)
        self.serverCount = len(servers)
        self.factory = self.factoryLeader.getLeaderFactory()
        self.sid = None
        self.getSessionId(0)

    def getSessionId(self,count=0):
        try :
            sid = self.factory.getSessionId()
            if not self.sid :
                self.sid = sid
        except Exception,e :
            logger.error("%s maybe is shutdown,can't connected. Try getNextLeader" % self.factoryLeader.thisServer)

            if count < self.serverCount :
                self.factory = self.factoryLeader.getNextLeader()
                if self.factory :
                    self.getSessionId(count+1)

    def createDomain(self, domain, ob):
        self.__put(domain,str(time.time()).replace(".",""),ob,False,0,None)

    def createDomainNode(self, domain, node, ob, isHeartBeat=False,timeout=None):
        self.__put(domain,node,ob,isHeartBeat,0, timeout)

    def __put(self,domain,node,obj,isHearBeat,count,timeout=None):
        ob = None
        if obj is None :
            return None
        if FactoryObjValue.checkGrammer(domain) and FactoryObjValue.checkGrammer(node) :
            try:
                ov = self.factory.create(domain,node,obj,self.sid,isHearBeat,timeout)
                ob = self.ovToBean(ov,domain,node)
                if ob and isHearBeat :
                    HbDaemon.runPutTask(self.factory,self.factoryLeader,domain,node,obj,self.sid)
            except Exception,e :
                logger.error("factorylocal.__put:"+str(e))

                if isinstance(e,CommunicationError):
                    self.factory = self.factoryLeader.getNextLeader()
                    if self.factory :
                        ob = self.__put(domain,node,obj,isHearBeat,count+1,timeout)
                if isinstance(e,ClosetoOverError) :
                    logger.error("put error:%s" % e.errorPrint())
        else:
            logger.error("domain:%s,node:%s is not valid string"%(domain,node))
        return ob

    def update(self, domain, node, obj,count=0):
        ob = None
        if obj is None :
            return None
        if FactoryObjValue.checkGrammer(domain) and FactoryObjValue.checkGrammer(node) :
            try:
                ov = self.factory.update(domain,node,obj,self.sid)
                ob = self.ovToBean(ov,domain,node)

            except Exception,e :
                logger.error("factorylocal.update:"+str(e))
                if isinstance(e,CommunicationError):
                    self.factory = self.factoryLeader.getNextLeader()
                    if self.factory :
                        ob = self.update(domain,node,obj,count+1)
                if isinstance(e,ClosetoOverError) :
                    logger.error("update error:%s" % e.errorPrint())

        return ob
    
    def getNodes(self, domain,count=0):

        objList = None

        if domain is None :
            ov = self.factory.getNodes()
            return objList

        if FactoryObjValue.checkGrammer(domain) :
            try:
                ov = self.factory.get(domain,None,self.sid)
                objList = self.ovToBeanList(ov,domain)

            except Exception,e :
                logger.error(e)
                if isinstance(e,CommunicationError):
                    self.factory = self.factoryLeader.getNextLeader()
                    if self.factory :
                        ob = self.getNodes(domain,count+1)
                if isinstance(e,ClosetoOverError) :
                    logger.error("get Nodes error:%s" % e.errorPrint())

        return objList

    def getNodeByPrefix(self, prefix, isDomain=False, count = 0):
        objList = None

        try:
            ov = self.factory.getNodesByPrefix(prefix)
            objList = self.ovToBeanList2(ov,isDomain)

        except Exception,e :
            logger.error(e)
            if isinstance(e,CommunicationError) and count > 3:
                self.factory = self.factoryLeader.getNextLeader()
                if self.factory :
                    ob = self.getNodeByPrefix(prefix,isDomain,count+1)
            if isinstance(e,ClosetoOverError) :
                logger.error("get Nodes by prefix error:%s" % e.errorPrint())

        return objList

    def getDomainNode(self, domain, node,count=0):
        ob = None
        if FactoryObjValue.checkGrammer(domain) and FactoryObjValue.checkGrammer(node):
            try:
                ov = self.factory.get(domain,node,self.sid)
                ob = self.ovToBean(ov,domain,node)

            except Exception,e :
                logger.error(e)
                if isinstance(e,CommunicationError):
                    self.factory = self.factoryLeader.getNextLeader()
                    if self.factory :
                        ob = self.getDomainNode(domain,node,count+1)
        return ob

    def getLastest(self, domain, node, oldObj,count=0):
        ob = None
        if FactoryObjValue.checkGrammer(domain) and FactoryObjValue.checkGrammer(node):
            try:
                vid = 0l
                if oldObj :
                    vid = oldObj.vid

                ov = self.factory.getLastest(domain,node,self.sid,vid)
                ob = self.ovToBean(ov,domain,node)

            except Exception,e :
                logger.error(e)
                if isinstance(e,CommunicationError):
                    self.factory = self.factoryLeader.getNextLeader()
                    if self.factory :
                        ob = self.getLastest(domain,node,oldObj,count+1)
        return ob
    def getNodesLastest(self, domain, oldObjList,count=0):
        obList = None
        if FactoryObjValue.checkGrammer(domain):
            try:
                vid = 0l
                if oldObjList :
                    vid = oldObjList.vid

                ov = self.factory.getLastest(domain,None,self.sid,vid)
                obList = self.ovToBeanList(ov,domain)

            except Exception,e :
                logger.error(e)
                if isinstance(e,CommunicationError):
                    self.factory = self.factoryLeader.getNextLeader()
                    if self.factory :
                        ob = self.getNodesLastest(domain,oldObjList,count+1)
                if isinstance(e,ClosetoOverError) :
                    logger.error("getNodesLastest error:%s" % e.errorPrint())
        return obList

    def delete(self, domain, node,count=0):
        ob = None
        if FactoryObjValue.checkGrammer(domain) and FactoryObjValue.checkGrammer(node):
            try:
                ov = self.factory.delete(domain,node,self.sid)
                ob = self.ovToBean(ov,domain,node)

            except Exception,e :
                logger.error(e)
                if isinstance(e,CommunicationError):
                    self.factory = self.factoryLeader.getNextLeader()
                    if self.factory :
                        ob = self.delete(domain,node,count+1)
        return ob
    def deleteDomain(self, domain, count=0):
        obList = None
        if FactoryObjValue.checkGrammer(domain):
            try:
                ov = self.factory.delete(domain,None,self.sid)
                obList = self.ovToBeanList(ov,domain)

            except Exception,e :
                logger.error(e)
                if isinstance(e,CommunicationError):
                    self.factory = self.factoryLeader.getNextLeader()
                    if self.factory :
                        ob = self.deleteDomain(domain,count+1)
                if isinstance(e,ClosetoOverError) :
                    logger.error("deleteDomain Nodes error:%s" % e.errorPrint())
        return obList

    def setDeletable(self, domain,obj=None):
        pass

    def addLastestListener(self, domain, obj):
        pass


    def ovToBean(self,ov,domain,node):
        if ov and not ov.isEmpty() :
            domainNodeKey = FactoryObjValue.getDomainNodekey(domain,node)
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

    def ovToBeanList2(self,ov,isDomain) :
        objList = ObjectBeanList(random.random())
        keyLen = 2
        if ov and not ov.isEmpty() :
            if isDomain:
                keyLen = 1
            for key in ov.keys() :
                if key.find(meta.METACREATETIME) > -1 :
                    domainNodeKey = key[0:key.find(meta.METACREATETIME)]
                    keyArr = FactoryObjValue.getDomainNode(domainNodeKey)
                    if keyArr and len(keyArr) == keyLen :
                        obp = ObjectBean()
                        obp.vid = long(ov.getObj(meta.getMetaVersion(domainNodeKey)))
                        obp.name = domainNodeKey
                        obp.obj = ov.getObj(domainNodeKey)
                        obp.createTime = ov.getObj(meta.getMetaCreateTime(domainNodeKey))
                        objList.append(obp)
        return objList



