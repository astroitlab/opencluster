import threading
import logging
import time

import errors
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
    vs = "\u3001"
        
    @classmethod
    def runPutTask(cls, park, parkLeader, domain, node, sessionId):
        if HbDaemon.putTask is None :
            logger.info("heartbeat runPutTask")
            HbDaemon.putTask = PutHbTask(park,parkLeader,domain,node,sessionId,HbDaemon.pt)#second
            HbDaemon.putTask.start()

    @classmethod
    def runGetTask(cls, hbinfo, park):
        if HbDaemon.getTask is None :
            logger.info("heartbeat runGetTask")
            HbDaemon.getTask = GetHbTask(park,hbinfo,HbDaemon.pt)#second
            HbDaemon.getTask.start()

    @classmethod
    def runClearTask(cls, park):
        if HbDaemon.clrTask is None :
            cpd = int(conf.Conf.getClearPeriod())
            if cpd > 0 :
                logger.info("Run ClearTask")
                exp = int(conf.Conf.getExpiration())
                HbDaemon.clrTask = ClearTask(park,HbDaemon.pt,cpd)#second
                HbDaemon.clrTask.start()

class PutHbTask(threading.Thread) :
    def __init__(self,parkService,parkLeader,domain,node,sessionId,interval):
        self.__parkServrice = parkService
        self.__parkLeader = parkLeader
        self.__domain = domain
        self.__node = node
        self.__sessionId = sessionId
        self.__putList = []
        self.finished = threading.Event()
        self.interval = interval
        self.__putList.append(domain+HbDaemon.vs+node)
    def run(self):
        while True :
            try:
                self.finished.wait(self.interval*1000)
                if not self.finished.is_set():
                    self.__parkServrice.hearbeat(self.__putList)
            except Exception,e :
                logger.error(e)
                if isinstance(e, Pyro4.errors.CommunicationError) :
                    self.__parkServrice = self.__parkLeader.getNextLeader()

class GetHbTask(threading.Thread) :
    def __init__(self,parkService,hbinfo,interval):
        self.__parkServrice = parkService
        self.__hbinfo = hbinfo
        self.finished = threading.Event()
        self.interval = interval
    def run(self):
        while True :
            self.finished.wait(self.interval*1000)
            if not self.finished.is_set():
                for key in self.__hbinfo.keys() :
                    curtime = long(time.time())
                    lasttime = self.__hbinfo.getObj(key)
                    t = 0l
                    if not lasttime :
                        t = curtime - long(lasttime)
                    if t > HbDaemon.gt :
                        if HbDaemon.dt > 0 and t/HbDaemon.gt < 2 :
                            logger.warning("%s Slow and week heartbeat" % key)
                        if t > HbDaemon.gt + HbDaemon.dt :
                            if HbDaemon.dt > 0 :
                                logger.warning("Dead %s has exceeded max delaytime and is regarded as dead by park." % key)
                            self.__hbinfo.remove(key)

                            keys = key.split(HbDaemon.vs)
                            self.__parkServrice.delete(keys[0],keys[1])
                            logger.info("hbinfo:%s",self.__hbinfo)


class ClearTask(threading.Thread) :
    def __init__(self,parkService,expl,interval):
        threading.Thread.__init__(self)
        self.__parkServrice = parkService
        self.__expl = expl*3600
        self.finished = threading.Event()
        self.interval = interval
    def run(self):
        while True :
            self.finished.wait(self.interval)

            if not self.finished.is_set():
                pov = self.__parkServrice.getTheParkInfo()
                keyArray = pov.getParkInfoExp(self.__expl)

                if len(keyArray) > 0 :
                    logger.info("Get some expiration data and save for backup...")
                    #to do...
                for keys in keyArray :
                    logger.info("[Clear],[Expiration]:%,%s" (pov.getDomainNodekey(keys[0],keys[1])))
                    self.__parkServrice.delete(keys[0],keys[1])
            # self.finished.set()