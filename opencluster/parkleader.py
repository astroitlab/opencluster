import logging
import Queue
import hbdaemon
import Pyro4
from asyncexector import AsyncExector
from errors import LeaderError
from configuration import Conf
from servicecontext import ServiceContext


logger = logging.getLogger(__name__)

class ParkLeader(object):
    def __init__(self, host, port, serviceName, servers = None):
        self.master = False
        self.alwaysTry = Conf.getAlwaysTryLeader().lower() == "true"
        self.serviceName = serviceName
        self.thisServer = "%s:%s" % (host, port)
        self.groupServers = servers
        self.queue = Queue.Queue()
        self.rpl = None
    def getThisServer(self):
        return self.thisServer
        
    def wantBeMaster(self, parkservice):
        logger.info("wantBeMaster.............................")
        sv = []
        othermaster = self.getOtherMasterPark(sv)
        if not othermaster :
            logger.info("get one of other parks for init parkInfo.........")
            pks = self.getOtherPark()
            if len(pks) > 0 :
                self.setInitParkInfo(pks[0], parkservice)
            self.setMaster(True, parkservice)
        else :
            logger.info("wantBeMaster,master is %s:%s" % (sv[0], sv[1]))
    def getLeaderPark(self):
        index = self.getLeaderIndex(self.thisServer)
        return self.electionLeader(-1,index)
        
    def getNextLeader(self):
        logger.info("getNextLeader........")
        index = self.getLeaderIndex(self.thisServer)
        return self.electionLeader(index, index+1)
        
    def electionLeader(self, begin, index):
        theOk = True
        if index >= len(self.groupServers) :
            index = 0
        server = self.groupServers[index].split(":")
        parkService = None
        
        try :
            parkService = ServiceContext.getService(server[0], int(server[1]), self.serviceName)
        except LeaderError , le :
            logger.error("electionLeader %s:%s----:%" % (server[0], server[1], le))
            theOk = False
            leaderIndex = self.getLeaderIndex(le.getLeaderServer())
            parkService = self.electionLeader(-1, leaderIndex)

        except Exception , e :
            logger.error("electionLeader %s:%s----:%" % (server[0], server[1], e))
            theOk = False
            if begin != index :
                if not self.alwaysTry and begin < 0: 
                    begin = index
                parkService = self.electionLeader(begin, index + 1)
        if theOk :
            self.thisServer = self.groupServers[index]
            logger.info("leader server is %s:%s" % (server[0], server[1]))
        return parkService    
    def electionFixedLeader(self, index):
        theOk = True
        if index >= len(self.groupServers) :
            index = 0
        server = self.groupServers[index].split(":")
        parkService = None
        try :
            parkService = ServiceContext.getService(server[0], int(server[1]), self.serviceName)
            if parkService :
                parkService.askLeader()
        except LeaderError , le :
            logger.error("electionLeader %s:%s----:%" % (server[0], server[1], le))
            theOk = False
            leaderIndex = self.getLeaderIndex(le.getLeaderServer())
            parkService = self.electionLeader(leaderIndex)

        except Exception , e :
            logger.error("electionLeader %s:%s----:%" % (server[0], server[1], e))
            theOk = False
            parkService = self.electionLeader(index + 1)        
        if theOk :
            self.thisServer = self.groupServers[index]
            logger.info("leader server is %s:%s" % (server[0], server[1]))
        return parkService
    def getLeaderIndex(self, sa):
        i = 0;
        for server in self.groupServers :
            if sa == server :
                break
            i += 1
        return i
        
    def setMaster(self, isMaster, park):
        self.master = isMaster
        if isMaster :
            logger.info("leader server is %s" % self.thisServer)  
        if self.master :
            hbdaemon.HbDaemon.runClearTask(park)
            
    def getMaster(self):
        try :
            if self.master :
                return self.thisServer.split(":")
        except Exception,e :
            logger.error(e)


    def getOtherPark(self):
        parks = []
        for str in self.groupServers :
            if str != self.thisServer :
                server = str.split(":")
                try :
                    pk =  ServiceContext.getService(server[0], int(server[1]), self.serviceName)
                    clientHost = pk.getClientHost()#check alive state,if not connected,raise CommunicationError
                    parks.append(pk)
                except Exception,e :
                    if isinstance(e,Pyro4.errors.CommunicationError) :
                        logger.error("%s maybe is shutdown,can't connected." % str)
                
        return parks
        
    def checkMasterPark(self, sv, park):
        if self.master or not self.getOtherMasterPark(sv) :
            sv = self.thisServer.split(":")
            self.setMaster(True, park)
            return True
        else :
            return False
            
    def getOtherMasterPark(self, sv):
        pkMaster = None
        
        try :
            for pk in self.getOtherPark() :
                ask = pk.askMaster()

                if ask :
                    pkMaster = pk
                    sv.append(ask[0])
                    sv.append(ask[1])

        except Exception, e :
            logger.error("getOtherMasterPark...Error:%s" % e)
        return pkMaster
        
    def setInitParkInfo(self, fromPark, toPark):
        try :
            toPark.setParkInfo(fromPark.getParkInfo())
        except Exception, e :
            logger.error("setInitParkInfo...Error:%s" % e)


            
    def runCopyTask(self, domainNodeKey, park):
        logger.info("runCopyTask:"+domainNodeKey+"............................")
        # def task(vpark, queue):
        #     curKey = queue.get()
        #     if queue.empty() :
        #         self.copyParkInfo(vpark.getParkInfo())
        # self.queue.put(domainNodeKey)
        # self.rpl = AsyncExector(task , (park, self.queue))
        # self.rpl.run()
        try:
            obj = park.getParkInfo()
            for pk in self.getOtherPark() :
                pk.setParkInfo(obj)
        except Exception,e:
            logger.error("runCopyTask error:%s"%e)

    def copyParkInfo(self, obj):  
        sendlist = []
        for park in self.getOtherPark() :
            sendlist.append(park.setParkInfo(obj))
        return sendlist
    
#end        
        
