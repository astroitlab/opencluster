import re
import copy
import time

import meta


ITEM_NOTREADY = 1
ITEM_READY = 0
ITEM_EXCEPTION = -1
class ObjectBean(object):
    
    def __init__(self):
        self.obj = None
        self.name = ""
        self.vid = 0l

    def toObject(self):
        return self.obj
        
    def getName(self):
        return self.name

    def getVid(self):
        return self.vid

    def getDomain(self):
        if self.name :
            return ParkObjValue.getDomainNode(self.name)[0]
        else :
            return None
    def getNode(self):
        if self.name :
            keys = ParkObjValue.getDomainNode(self.name)
            if len(keys) == 2 :
                return keys[1]
        return None
    def toString(self):
        return "%s:%s" %(self.name , obj)

class ObjectBeanList(list) :
    def __init__(self,vid):
        super(ObjectBeanList,self).__init__()
        self.vid = vid

class ObjValue(dict):
    def __init__(self):
        pass
    def getWidely(self, regex):
        p = re.compile(regex)
        obj = ObjValue()
        for key in self.keys() :
            if p.match(key) :
                obj[key] = self[key]
        return obj
    def removeWidely(self, regex):
        p = re.compile(regex)
        obj = ObjValue()
        keys = []
        for key in self.keys() :
            if p.match(key) :
                keys.append(key)
                obj[key] = self[key]
        for key in keys :
            self.remove(key)
        return obj
    def setObj(self, k, v):
        self[k] = v

    def getObj(self, k):
        if self.has_key(k) :
            return self[k]

    def remove(self, key):
        obj = None
        if self.has_key(key) :
            obj = self[key]
            del self[key]
        return obj

    def putAll(self,d):
        if len(d) == 0 :
            return
        for (k,v) in d.items() :
            self.setObj(k,v)
    def isEmpty(self):
        return len(self.items()) <= 0
class WareHouse(ObjValue):
    def __init__(self,ready=True):
        super(WareHouse,self).__init__()

        self.ready = ready
        self.status = ITEM_NOTREADY
        self.mark = True

    def getStatusName(self):
        names = ["EXCEPTION","READY","NOTREADY"]
        return  names[self.status + 1]
    def setReady(self,status):
        self.ready = True
        self.status = status

class ParkObjValue(ObjValue):
    def getNodeWidely(self, nodeKey):
        p = re.compile(nodeKey + "\\..*")
        obj = ObjValue()
        getobj = self.getObj(nodeKey)
        if getobj :
            obj.setObj(nodeKey,getobj)
            for key in self.keys() :
                if p.match(key) :
                    obj.setObj(key,self.get(key))
        return obj
    def getNode(self, domain, node):
        ov = ObjValue()
        if domain :
            if node :
                domainNodeKey = self.getDomainNodekey(domain, node)
                obj = self.getObj(domainNodeKey)
                version = self.getObj(meta.getMetaVersion(domainNodeKey))
                createBy = self.getObj(meta.getMetaCreater(domainNodeKey))
                creatip = self.getObj(meta.getMetaCreaterIP(domainNodeKey))
                creattime = self.getObj(meta.getMetaCreateTime(domainNodeKey))
                prop = self.getObj(meta.getMetaProperties(domainNodeKey))
                updateby = self.getObj(meta.getMetaUpdater(domainNodeKey))
                updateip = self.getObj(meta.getMetaUpdaterIP(domainNodeKey))
                updatetime = self.getObj(meta.getMetaUpdateTime(domainNodeKey))

                if obj:
                    ov.setObj(domainNodeKey,obj)
                if version:
                    ov.setObj(meta.getMetaVersion(domainNodeKey), version)
                if createBy :
                    ov.setObj(meta.getMetaCreater(domainNodeKey), createBy)
                if creatip :
                    ov.setObj(meta.getMetaCreaterIP(domainNodeKey), creatip)
                if creattime :
                    ov.setObj(meta.getMetaCreateTime(domainNodeKey), creattime)
                if prop :
                    ov.setObj(meta.getMetaProperties(domainNodeKey), prop)
                if updateby :
                    ov.setObj(meta.getMetaUpdater(domainNodeKey), updateby)
                if updateip :
                    ov.setObj(meta.getMetaUpdaterIP(domainNodeKey), updateip)
                if updatetime :
                    ov.setObj(meta.getMetaUpdateTime(domainNodeKey), updatetime)

            else :
                ov = self.getNodeWidely(domain)
        return ov  
        
    @classmethod
    def getDomainNode(cls, domainNodeKey):
        return domainNodeKey.split("\\.")

    @classmethod
    def checkGrammer(cls,k):
        p = re.compile("^[a-z0-9A-Z_-]+$")
        if k and p.match(k) :
            return True
        else:
            return False
    @classmethod
    def getDomainNodekey(self, domain, node):
        if node is None:
            return domain
        else :
            return "%s.%s" % (domain, node)

    def getParkInfo(self):
        return copy.deepcopy(self)

    def removeDomain(self,domain):
        ov = ObjValue()
        if domain :
            obj = self.remove(domain)
            version = self.remove(meta.getMetaVersion(domain))
            createBy = self.remove(meta.getMetaCreater(domain))
            creatip = self.remove(meta.getMetaCreaterIP(domain))
            creattime = self.remove(meta.getMetaCreateTime(domain))
            if obj :
                ov.setObj(domain, obj)
            if version :
                ov.setObj(meta.getMetaVersion(domain), version)
            if createBy :
                ov.setObj(meta.getMetaCreater(domain), createBy)
            if creatip :
                ov.setObj(meta.getMetaCreaterIP(domain), creatip)
            if creattime :
                ov.setObj(meta.getMetaCreateTime(domain), creattime)
        return ov

    def removeNode(self, domain, node):
        ov = ObjValue()
        if domain :
            if node :
                domainNodeKey = self.getDomainNodekey(domain, node)
                obj = self.remove(domainNodeKey)
                version = self.remove(meta.getMetaVersion(domainNodeKey))
                createBy = self.remove(meta.getMetaCreater(domainNodeKey))
                creatip = self.remove(meta.getMetaCreaterIP(domainNodeKey))
                creattime = self.remove(meta.getMetaCreateTime(domainNodeKey))
                prop = self.remove(meta.getMetaProperties(domainNodeKey))
                updateby = self.remove(meta.getMetaUpdater(domainNodeKey))
                updateip = self.remove(meta.getMetaUpdaterIP(domainNodeKey))
                updatetime = self.remove(meta.getMetaUpdateTime(domainNodeKey))

                if obj :
                    ov.setObj(domainNodeKey, obj)
                if version :
                    ov.setObj(meta.getMetaVersion(domainNodeKey), version)
                if createBy :
                    ov.setObj(meta.getMetaCreater(domainNodeKey), createBy)
                if creatip :
                    ov.setObj(meta.getMetaCreaterIP(domainNodeKey), creatip)
                if creattime :
                    ov.setObj(meta.getMetaCreateTime(domainNodeKey), creattime)
                if prop :
                    ov.setObj(meta.getMetaProperties(domainNodeKey), prop)
                if updateby :
                    ov.setObj(meta.getMetaUpdater(domainNodeKey), updateby)
                if updateip :
                    ov.setObj(meta.getMetaUpdaterIP(domainNodeKey), updateip)
                if updatetime :
                    ov.setObj(meta.getMetaUpdateTime(domainNodeKey), updatetime)

            else :
                ov = self.removeNodeWidely(domain)
        return ov
    def removeNodeWidely(self, nodeKey):
        obj = ObjValue()
        node = self.remove(nodeKey)
        p = re.compile(nodeKey + "\\..*")
        
        if node :
            obj.setObj(nodeKey, node)
            keys = []
            for key in self.keys() :
                if p.match(key) :
                    keys.append(key)
        return obj

    def getParkInfoExp(self,exp):
        keys = []
        for key in self.keys() :
            if key.find(meta.METACREATETIME) > -1 :
                domainNodeKey = key[0:key.find(meta.METACREATETIME)]
                keyArr = self.getDomainNode(domainNodeKey)
                if keyArr and len(keyArr)==2 :
                    propValue = self.getObj(meta.getMetaProperties(domainNodeKey))
                    if not propValue or propValue != meta.HEARTBEAT :
                        if time.time()-long(self.get(key)) > exp :
                            keys.append(keyArr)
        return keys

if __name__ == "__main__" :
    obj = ParkObjValue()
    obj["dd"] = "dxx"

    obj2 = ParkObjValue()
    obj2["ddx"] = "dxxx"

    obj.putAll(obj2)

    print ParkObjValue.checkGrammer("2323x2dsdafa")
    print obj.getParkInfo()
