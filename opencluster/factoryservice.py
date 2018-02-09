import binascii
import node
from errors import *
from hbdaemon import *
from item import *
from factoryleader import FactoryLeader

logger = logging.getLogger(__name__)

class FactoryService(object):
    
    def __init__(self, host, port, servers, serviceName):
        self.factoryLeader = FactoryLeader(host, port, serviceName, servers)
        self.factoryInfo = FactoryObjValue()
        self.hbInfo = ObjValue()

        self.condition = threading.Condition()
    
    def wantBeMaster(self):
        self.factoryLeader.wantBeMaster(self)
        
    def checkSessionId(self, sessionId):
        if sessionId :
            return sessionId
        else :
            return "se%s%s" % (str(time.time()).replace(".", ""),id(self))
    
    
    def updateDomainVersion(self, domain=None):
        if not domain :
            return self.getObjectVersion(str(time.time()).replace(".", ""))
        domainversion = self.factoryInfo[meta.getMetaVersion(domain)]
        nodeversions = self.factoryInfo.getWidely(meta.getMetaVersion(domain+"\\..+"));
        crcstr = "".join(v for k, v in nodeversions.items())
        
        crcversion = self.getObjectVersion(crcstr)
        
        if crcversion ==  domainversion :
            return domainversion
        else :
            return crcversion
   
        
    def getObjectVersion(self, crcstr):
        return binascii.crc32(crcstr)
        
    def getSessionId(self):
        return self.checkSessionId(None)
        
    def create(self, domain, node, obj, sessionId, isHeartBeat, timeout=None):
        ClosetoOverError.checkMemCapacity()
        objv = None
        if domain and node :
            #lock
            if self.condition.acquire() :
                try :
                    domainNodeKey = FactoryObjValue.getDomainNodekey(domain, node)
                    if not self.factoryInfo.has_key(domainNodeKey) :
                        if not self.factoryInfo.has_key(domain) :
                            self.factoryInfo.setObj(domain, 0l)
                            self.factoryInfo.setObj(meta.getMetaVersion(domain), 0l)
                            self.factoryInfo.setObj(meta.getMetaCreater(domain), sessionId)
                            self.factoryInfo.setObj(meta.getMetaCreaterIP(domain), "")
                            self.factoryInfo.setObj(meta.getMetaCreateTime(domain), time.time())
                            self.factoryInfo.setObj(meta.getMetaUpdateTime(domain), time.time())

                        self.factoryInfo.setObj(meta.getMetaVersion(domain), self.updateDomainVersion())

                        self.factoryInfo.setObj(domainNodeKey, obj)
                        self.factoryInfo.setObj(meta.getMetaVersion(domainNodeKey), self.getObjectVersion(str(obj)))
                        self.factoryInfo.setObj(meta.getMetaCreater(domainNodeKey), node)
                        self.factoryInfo.setObj(meta.getMetaCreaterIP(domainNodeKey), "")
                        self.factoryInfo.setObj(meta.getMetaCreateTime(domainNodeKey), time.time())

                        #self-defined timeout
                        if timeout is not None:
                            self.factoryInfo.setObj(meta.getMetaTimeout(domainNodeKey), timeout)

                        nodeNum = self.factoryInfo.getObj(domain)
                        self.factoryInfo.setObj(domain, nodeNum+1);

                        if isHeartBeat :
                            self.factoryInfo.setObj(meta.getMeta(domainNodeKey), meta.HEARTBEAT)
                        self.factoryLeader.runCopyTask(domainNodeKey, self)
                        # print self.factoryInfo
                        objv = self.get(domain, node, sessionId)

                    else :
                        logger.info("%s is exist,updating!" %(domainNodeKey))
                        self.update(domain,node,obj,sessionId)

                except Exception,e:
                    logger.error("factoryservice.create:"+e)
                finally :
                    #unlock
                    self.condition.release()
        return objv
    def update(self, domain, node, obj, sessionId):
        ClosetoOverError.checkMemCapacity()
        objv = None
        if domain and node :
            if self.condition.acquire() :
                try :
                    domainNodeKey = FactoryObjValue.getDomainNodekey(domain, node)
                    #if canWrite
                    if self.factoryInfo.has_key(domainNodeKey) :
                        self.factoryInfo.setObj(domainNodeKey, obj)
                        theversion = self.getObjectVersion(str(obj));
                        if theversion != self.factoryInfo[meta.getMetaVersion(domainNodeKey)] :
                            self.factoryInfo.setObj(meta.getMetaVersion(domainNodeKey), theversion)
                            self.factoryInfo.setObj(meta.getMetaVersion(domain), self.updateDomainVersion())
                        self.factoryInfo.setObj(meta.getMetaUpdater(domainNodeKey), sessionId)
                        self.factoryInfo.setObj(meta.getMetaUpdaterIP(domainNodeKey), "")
                        self.factoryInfo.setObj(meta.getMetaUpdateTime(domainNodeKey), time.time())

                        #logger.info(self.factoryInfo)
                        self.factoryLeader.runCopyTask(domainNodeKey, self)
                        objv = self.get(domain, node, sessionId)
                    else :
                        logger.info("%s is not exist!" %(domainNodeKey))
                except Exception,e:
                    logger.error("factoryservice.update:"+e)
                finally :
                    #unlock
                    self.condition.release()
        return objv
    
    def delete(self, domain, node, sessionId=None):
        objv = None
        if domain :
            if not node :
                ClosetoOverError.checkMemCapacity()
            if self.condition.acquire() :
                try :
                    domainNodeKey = FactoryObjValue.getDomainNodekey(domain, node)
                    objv = self.factoryInfo.removeNode(domain, node)

                    if len(objv.items()) > 0 :
                        nodeNum = self.factoryInfo.getObj(domain)
                        if nodeNum :
                            if nodeNum==1 :
                                self.factoryInfo.removeDomain(domain)
                            else :
                                self.factoryInfo.setObj(domain,nodeNum-1)
                        self.factoryLeader.runCopyTask(domainNodeKey,self)
                    else :
                        objv = None
                        logger.info(domainNodeKey + " cant be deleted or not exist!")
                except Exception,e:
                    logger.error(e)
                finally :
                    #unlock
                    self.condition.release()
        return objv        
        
    def get(self, domain, node, sessionId) :
        ov = None
        if domain :
            if node is None :
                ClosetoOverError.checkMemCapacity()
            if self.condition.acquire() :
                try :
                    # logger.info("get:%s"%FactoryObjValue.getDomainNodekey(domain,node))
                    ov = self.factoryInfo.getNode(domain, node)
                    if ov.isEmpty() :
                        ov = None
                finally :
                    self.condition.release()
            #unlock
        return ov

    def getNodesByPrefix(self,prefix):
        ov = None
        ClosetoOverError.checkMemCapacity()
        if self.condition.acquire() :
            try :
                # logger.info("get:%s"%FactoryObjValue.getDomainNodekey(domain,node))
                ov = self.factoryInfo.getNodeByPrefix(prefix)
                if ov.isEmpty() :
                    ov = None
            finally :
                self.condition.release()
        #unlock
        return ov

    def getLatest(self, domain, node, sessionId,version) :
        ov = None
        if self.condition.acquire() :
            try :
                nodeVersion = self.factoryInfo.getObj(meta.getMetaVersion(FactoryObjValue.getDomainNodekey(domain,node)))

                if nodeVersion and nodeVersion!=version :
                    self.get(domain,node,sessionId)
                    logger.info("getLastest:%s,%s" % (nodeVersion,version))
            finally :
                self.condition.release()

        return ov

    def getTheFactoryInfo(self):
        if self.condition.acquire() :
            try :
                ov = self.factoryInfo.getFactoryInfo()
            finally :
                self.condition.release()
        return ov
        
    def getFactoryInfo(self):
        # logger.info("getFactoryinfo from %s" % (self.getClientHost()))
        return self.getTheFactoryInfo()
        
    def setFactoryInfo(self, objValue):#need write lock
        if self.condition.acquire() :
            try :
                self.factoryInfo = objValue
            finally :
                self.condition.release()
        return True

    def askMaster(self):
        logger.info("receive askMaster................")
        return self.factoryLeader.getMaster()

    def askLeader(self):
        logger.info("receive askLeader................")
        sv = []
        if self.factoryLeader.checkMasterFactory(sv, self) :
            return True
        else :
            return False
            
    def heartbeat(self, domainNodeKeys, sessionId):
        retValue = False
        if domainNodeKeys :
           self.hbInfo.setObj(domainNodeKeys, time.time())
           domainNodes = domainNodeKeys.split(HbDaemon.vs)
           if self.get(domainNodes[0],domainNodes[1],sessionId):
               retValue = True
        HbDaemon.runGetTask(self.hbInfo, self)
        return retValue

    def getClientHost(self):
        return ""

    def buildServices(self,node):
        pass
#end
