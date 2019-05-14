import os,sys
import re
import copy
import time
import meta
import importlib
import imp

TaskStateName = {
    6: "TASK_STAGING",  # Initial state. Framework status updates should not use.
    0: "TASK_STARTING",
	1: "TASK_RUNNING",
	2: "TASK_FINISHED", # TERMINAL.
	3: "TASK_FAILED",   # TERMINAL.
	4: "TASK_KILLED",   # TERMINAL.
	5: "TASK_LOST",      # TERMINAL.
    7: "TASK_ERROR"
}

class ObjectBean(object):
    
    def __init__(self):
        self.obj = None
        self.name = ""
        self.vid = 0l
        self.createTime = 0l

    def toObject(self):
        return self.obj
        
    def getName(self):
        return self.name

    def getVid(self):
        return self.vid

    def getDomain(self):
        if self.name :
            return FactoryObjValue.getDomainNode(self.name)[0]
        else :
            return None
    def getNode(self):
        if self.name :
            keys = FactoryObjValue.getDomainNode(self.name)
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
        super(ObjValue,self).__init__()

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

class Task:
    TASK_STAGING = 6   # Initial state.
    TASK_STARTING = 0
    TASK_RUNNING = 1
    TASK_FINISHED = 2
    TASK_FAILED = 3
    TASK_KILLED = 4
    TASK_LOST = 5
    TASK_ERROR = 7

    def __init__(self, id, tried = 0,state = 6,state_time = 0,workerClass=None,data = None,workDir = None,priority=1,resources={"cpus":0,"mem":0,"gpus":0},warehouse="",jobName=""):
        self.id = id
        self.tried = tried
        self.state = state
        self.state_time = state_time
        self.workerClass = workerClass
        self.data = data
        self.workDir = workDir
        self.priority = priority
        self.resources = resources
        self.warehouse = warehouse
        self.jobName = jobName
        self.result = None

    def run(self,attemptId):
        sys.path.extend([self.workDir])
        moduleAndClass = self.workerClass.split(".")
        workerModule  = importlib.import_module('.'.join(moduleAndClass[:-1]))
        worker = getattr(workerModule, moduleAndClass[-1])()
        return worker.doTask(self.data);

    def getStateName(self):
        return TaskStateName[self.state]

    def preferredLocations(self):
        return self.locs

    def __str__(self):
        return "task:{id:%s,state:%s,tried:%s,workerClass:%s,workDir:%s}"%(self.id,self.getStateName(),self.tried,self.workerClass,self.workDir)

class ManagerOption(object):
    def __init__(self, cpus=1, mem = 100,gpus = 0,parallel = 0,group=None,workertype = None,warehouse = None,retry=0,name=""):
        self.cpus = cpus
        self.mem = mem
        self.gpus = gpus
        self.parallel = parallel
        self.group = group
        self.workertype = workertype
        self.warehouse = warehouse
        self.retry = retry
        self.name = name

class CompletionEvent:
    def __init__(self, task, reason, result):
        self.task = task
        self.reason = reason
        self.result = result

class TaskEndReason: pass

class Success(TaskEndReason): pass

class OtherFailure(TaskEndReason):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return '<OtherFailure %s>' % self.message

class FutureResult(TaskEndReason):
    def __init__(self,  id, state, message='', result=None):
        self.id = id
        self.message = message
        self.state = state
        self.result = result
    def __str__(self):
        return 'FutureResult[%s], message : %s,state : %s' % (self.message,TaskStateName[self.state])

class FactoryObjValue(ObjValue):
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

    def getNodeByPrefix(self, prefix):
        p = re.compile(prefix + "*")
        obj = ObjValue()
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
                timeout = self.getObj(meta.getMetaTimeout(domainNodeKey))
                meetadata = self.getObj(meta.getMeta(domainNodeKey))

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
                if timeout :
                    ov.setObj(meta.getMetaTimeout(domainNodeKey), timeout)
                if meetadata :
                    ov.setObj(meta.getMeta(domainNodeKey), meetadata)

            else :
                ov = self.getNodeWidely(domain)
        return ov  
        
    @classmethod
    def getDomainNode(cls, domainNodeKey):
        return domainNodeKey.split(".")

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

    def getFactoryInfo(self):
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
                metadata = self.remove(meta.getMeta(domainNodeKey))

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
                if metadata :
                    ov.setObj(meta.getMeta(domainNodeKey), metadata)

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

    def getFactoryInfoExp(self,exp):
        keys = []
        for key in self.keys() :
            if key.find(meta.METACREATETIME) > -1 :
                domainNodeKey = key[0:key.find(meta.METACREATETIME)]
                keyArr = self.getDomainNode(domainNodeKey)

                timeout = self.getObj(meta.getMetaTimeout(domainNodeKey))
                createtime = long(self.get(key))
                if timeout is not None :
                    if time.time()- createtime > long(self.getObj(meta.getMetaTimeout(domainNodeKey))) :
                        keys.append(keyArr)
                    continue

                if keyArr and len(keyArr)==2 :
                    propValue = self.getObj(meta.getMeta(domainNodeKey))
                    if propValue is None or propValue != meta.HEARTBEAT :
                        if time.time()-long(self.get(key)) > exp :
                            keys.append(keyArr)
        return keys

if __name__ == "__main__" :
    obj = FactoryObjValue()
    obj["_worker_workerUVFITS"] = "1"
    obj["_worker_workerdemo"] = "0"
    for (k,v) in obj.items() :
        print k,v
    p = re.compile("_worker_*")
    obj = ObjValue()
    if p.match("_worker_workerdemo.") :
        print "1"

    obj2 = FactoryObjValue()
    obj2["ddx"] = "dxxx"

    obj.putAll(obj2)

    print FactoryObjValue.checkGrammer("2323x2dsdafa")
