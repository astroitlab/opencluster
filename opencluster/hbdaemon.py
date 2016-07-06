import threading
import logging
import time
import Pyro4
import configuration as conf


logger = logging.getLogger(__name__)

class HbDaemon :
    putTask = None
    getTask = None
    clrTask = None

    pt = conf.Conf.getHeartBeat()
    dt = conf.Conf.getMaxDelay()
    gt = pt*2
    vs = "|"
        
    @classmethod
    def runPutTask(cls, factory, factoryLeader, domain, node, obj, sessionId):
        if HbDaemon.putTask is None :
            logger.info("heartbeat runPutTask")
            HbDaemon.putTask = PutHbTask(factory,factoryLeader,domain,node,obj,sessionId,HbDaemon.pt)#second
            HbDaemon.putTask.start()

    @classmethod
    def runGetTask(cls, hbinfo, factory):
        if HbDaemon.getTask is None :
            logger.info("heartbeat runGetTask")
            HbDaemon.getTask = GetHbTask(factory,hbinfo,HbDaemon.pt)#second
            HbDaemon.getTask.start()

    @classmethod
    def runClearTask(cls, factory):
        if HbDaemon.clrTask is None :
            cpd = int(conf.Conf.getClearPeriod())
            if cpd > 0 :
                logger.info("Run ClearTask")
                exp = int(conf.Conf.getExpiration())
                HbDaemon.clrTask = ClearTask(factory,exp,cpd)#second
                HbDaemon.clrTask.start()

class PutHbTask(threading.Thread) :
    def __init__(self,factoryService,factoryLeader,domain,node,obj,sessionId,interval):
        super(PutHbTask,self).__init__()
        self.__factoryService = factoryService
        self.__factoryLeader = factoryLeader
        self.__domain = domain
        self.__node = node
        self.__obj = obj
        self.__sessionId = sessionId
        self.__putList = []
        self.finished = threading.Event()
        self.interval = interval

    def run(self):
        while True :
            try:
                self.finished.wait(self.interval)

                if not self.finished.is_set():
                    if not self.__factoryService.heartbeat(self.__domain+HbDaemon.vs+self.__node,self.__sessionId):
                        self.__factoryService.create(self.__domain,self.__node,self.__obj,self.__sessionId,True);
            except Exception,e :
                logger.error(e)
                if isinstance(e, Pyro4.errors.CommunicationError) :
                    self.__factoryService = self.__factoryLeader.getNextLeader()

class GetHbTask(threading.Thread) :
    def __init__(self,factoryService,hbinfo,interval):
        super(GetHbTask,self).__init__()
        self.__factoryService = factoryService
        self.__hbinfo = hbinfo
        self.finished = threading.Event()
        self.interval = interval
    def run(self):
        while True :
            self.finished.wait(self.interval)

            if not self.finished.is_set():
                for key in self.__hbinfo.keys() :
                    curtime = long(time.time())
                    lasttime = long(self.__hbinfo.getObj(key))

                    t = 0
                    if lasttime :
                        t = int(curtime - lasttime)
                    if t > HbDaemon.gt :
                        if HbDaemon.dt > 0 and t/HbDaemon.gt < 2 :
                            logger.warning("%s Slow and week heartbeat" % key)
                        if t > HbDaemon.gt + HbDaemon.dt :
                            if HbDaemon.dt > 0 :
                                logger.warning("Dead %s has exceeded max delaytime and will be removed by factory." % key)
                            self.__hbinfo.remove(key)

                            keys = key.split(HbDaemon.vs)
                            self.__factoryService.delete(keys[0],keys[1])


class ClearTask(threading.Thread) :
    def __init__(self,factoryService,expl,interval):
        super(ClearTask,self).__init__()
        self.__factoryService = factoryService
        self.__expl = expl
        self.finished = threading.Event()
        self.interval = interval
    def run(self):
        while True :
            self.finished.wait(self.interval)

            if not self.finished.is_set():
                pov = self.__factoryService.getTheFactoryInfo()

                keyArray = pov.getFactoryInfoExp(self.__expl)

                if len(keyArray) > 0 :
                    logger.info("Get some expiration data and save for backup...")
                    #to do...
                for keys in keyArray :
                    logger.info("[Clear],[Expiration]:%s" % (pov.getDomainNodekey(keys[0],keys[1])))
                    self.__factoryService.delete(keys[0],keys[1])
            # self.finished.set()