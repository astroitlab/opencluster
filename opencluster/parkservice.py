import binascii

from errors import *
from hbdaemon import *
from item import *
from parkleader import ParkLeader


logger = logging.getLogger(__name__)

class ParkService(object):
    
    def __init__(self, host, port, servers, serviceName):
        self.parkLeader = ParkLeader(host, port, serviceName, servers)
        self.parkInfo = ParkObjValue()
        self.hbInfo = ObjValue()
        self.condition = threading.Condition()
    
    def wantBeMaster(self):
        self.parkLeader.wantBeMaster(self)
        
    def checkSessionId(self, sessionId):
        if sessionId :
            return sessionId
        else :
            return "se%s%s" % (str(time.time()).replace(".", ""),id(self))
    
    
    def updateDomainVersion(self, domain=None):
        if not domain :
            return self.getObjectVersion(str(time.time()).replace(".", ""))
        domainversion = self.parkInfo[meta.getMetaVersion(domain)]
        nodeversions = self.parkInfo.getWidely(meta.getMetaVersion(domain+"\\..+"));
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
        
    def create(self, domain, node, obj, sessionId, isHeartBeat):
        ClosetoOverError.checkMemCapacity()
        objv = None
        if domain and node :
            #lock
            if self.condition.acquire() :
                try :
                    domainNodeKey = ParkObjValue.getDomainNodekey(domain, node)
                    if not self.parkInfo.has_key(domainNodeKey) :
                        if not self.parkInfo.has_key(domain) :
                            self.parkInfo.setObj(domain, 0l)
                            self.parkInfo.setObj(meta.getMetaVersion(domain), 0l)
                            self.parkInfo.setObj(meta.getMetaCreater(domain), sessionId)
                            self.parkInfo.setObj(meta.getMetaCreaterIP(domain), "")
                            self.parkInfo.setObj(meta.getMetaCreateTime(domain), time.time())

                        self.parkInfo.setObj(domainNodeKey, obj)
                        self.parkInfo.setObj(meta.getMetaVersion(domainNodeKey), self.getObjectVersion(obj))
                        self.parkInfo.setObj(meta.getMetaVersion(domain), self.updateDomainVersion())
                        self.parkInfo.setObj(meta.getMetaCreater(domain), 0l)
                        self.parkInfo.setObj(meta.getMetaCreaterIP(domain), "")
                        self.parkInfo.setObj(meta.getMetaCreateTime(domain), time.time())
                        nodeNum = self.parkInfo.getObj(domain)
                        self.parkInfo.setObj(domain, nodeNum+1);

                        if isHeartBeat :
                            self.parkInfo.setObj(meta.getMeta(domainNodeKey), meta.HEARTBEAT)
                        self.parkLeader.runCopyTask(domainNodeKey, self)
                        print self.parkInfo
                        objv = self.get(domain, node, sessionId)

                    else :
                        logger.info("%s is exist!" %(domainNodeKey))

                except Exception,e:
                    logger.error(e)
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
                    domainNodeKey = ParkObjValue.getDomainNodekey(domain, node)
                    #if canWrite
                    if self.parkInfo.has_key(domainNodeKey) :
                        self.parkInfo.setObj(domainNodeKey, obj)
                        theversion = self.getObjectVersion(obj);
                        if theversion != self.parkInfo[meta.getMetaVersion(domainNodeKey)] :
                            self.parkInfo.setObj(meta.getMetaVersion(domainNodeKey), theversion)
                            self.parkInfo.setObj(meta.getMetaVersion(domain), self.updateDomainVersion())
                        self.parkInfo.setObj(meta.getMetaUpdater(domain), sessionId)
                        self.parkInfo.setObj(meta.getMetaUpdaterIP(domain), "")
                        self.parkInfo.setObj(meta.getMetaUpdateTime(domain), time.time())

                        self.parkLeader.runCopyTask(domainNodeKey, self)
                        objv = self.get(domain, node, sessionId)
                    else :
                        logger.info("%s is not exist!" %(domainNodeKey))
                except Exception,e:
                    logger.error(e)
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
                    domainNodeKey = ParkObjValue.getDomainNodekey(domain, node)
                    objv = self.parkInfo.removeNode(domain, node)

                    if len(objv.items()) > 0 :
                        nodeNum = self.parkInfo.getObj(domain)
                        if nodeNum :
                            if nodeNum==1 :
                                self.parkInfo.removeDomain(domain)
                            else :
                                self.parkInfo.setObj(domain,nodeNum-1)
                        self.parkLeader.runCopyTask(domainNodeKey,self)
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
                    # logger.info("get:%s"%ParkObjValue.getDomainNodekey(domain,node))
                    ov = self.parkInfo.getNode(domain, node)
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
                nodeVersion = self.parkInfo.getObj(meta.getMetaVersion(ParkObjValue.getDomainNodekey(domain,node)))

                if nodeVersion and nodeVersion!=version :
                    self.get(domain,node,sessionId)
                    logger.info("getLastest:%s,%s" % (nodeVersion,version))
            finally :
                self.condition.release()

        return ov

    def getTheParkInfo(self):#need read lock
        if self.condition.acquire() :
            try :
                ov = self.parkInfo.getParkInfo()
            finally :
                self.condition.release()
        return ov
        
    def getParkInfo(self):
        logger.info("getParkinfo from %s" % (self.getClientHost()))
        return self.getTheParkInfo()
        
    def setParkInfo(self, objValue):#need write lock
        if self.condition.acquire() :
            try :
                self.parkInfo = objValue
            finally :
                self.condition.release()
        return True

    def askMaster(self):
        logger.info("receive askMaster................")
        return self.parkLeader.getMaster()

    def askLeader(self):
        logger.info("receive askLeader................")
        sv = []
        if self.parkLeader.checkMasterPark(sv, self) :
            return True
        else :
            raise LeaderError(self.parkLeader.getThisServer(), sv)
            
    def heartbeat(self, domainNodeKeys, sessionId):
        hbback = False
        if domainNodeKeys :
           for curKey in domainNodeKeys :
               self.hbInfo.setObj(curKey, str(time.time()).replace(".",""))
           hbback = True
        HbDaemon.runGetTask(self.hbInfo, self)
        return hbback
    
    def getClientHost(self):
        return ""
#end
