import logging
import queue
import Pyro4
import time
import traceback

from opencluster.hbdaemon import HbDaemon
from opencluster.errors import LeaderError
from opencluster.configuration import Conf

logger = logging.getLogger(__name__)

class FactoryLeader(object):
    def __init__(self, host, port, serviceName, servers = None):
        self.master = False
        self.alwaysTry = Conf.getAlwaysTryLeader()
        self.serviceName = serviceName
        self.thisServer = "%s:%s" % (host, port)
        self.groupServers = servers
        self.queue = queue.Queue()
        self.rpl = None

    def getThisServer(self):
        return self.thisServer

    def wantBeMaster(self, factoryservice):
        logger.info("wantBeMaster.............................")
        sv = []
        othermaster = self.getOtherMasterFactory(sv)
        if not othermaster :
            logger.info("get one of other factorys for init factoryInfo.........")
            pks = self.getOtherFactory()
            if len(pks) > 0 :
                self.setInitFactoryInfo(pks[0], factoryservice)
            self.setMaster(True, factoryservice)
        else :
            logger.info("but master is %s:%s" % (sv[0], sv[1]))

    def getLeaderFactory(self):
        index = self.getLeaderIndex(self.thisServer)
        return self.electionLeader(-1, index)

    def getNextLeader(self):
        logger.info("getNextLeader........")
        index = self.getLeaderIndex(self.thisServer)
        return self.electionLeader(index, index+1)

    def electionLeader(self, begin=0, index=0):
        theOk = False
        if index >= len(self.groupServers) :
            index = 0
        server = self.groupServers[index].split(":")
        factoryService = None

        try :
            factoryService = RPCContext.getService(server[0], int(server[1]),
                                                   self.serviceName)
            logger.debug(factoryService)
            if factoryService and factoryService.askLeader():
                theOk = True

        except Exception as e :
            logger.error("electionLeader2 %s:%s----%s" % (server[0], server[1], e))
            theOk = False
            if begin != index :
                if not self.alwaysTry and begin < 0:
                    begin = index
                time.sleep(3)
                factoryService = self.electionLeader(begin, index + 1)
        if theOk :
            self.thisServer = self.groupServers[index]
            logger.info("leader server is %s:%s" % (server[0], server[1]))
        return factoryService

    def electionFixedLeader(self, index=0):
        theOk = True
        if index >= len(self.groupServers) :
            index = 0
        server = self.groupServers[index].split(":")
        factoryService = None
        try :
            factoryService = RPCContext.getService(server[0], int(server[1]),
                                                   self.serviceName)
            if factoryService :
                factoryService.askLeader()
        except LeaderError as le :
            logger.error("electionLeader %s:%s----:%s" % (server[0], server[1], le))
            theOk = False
            leaderIndex = self.getLeaderIndex(le.getLeaderServer())
            factoryService = self.electionLeader(leaderIndex)
        except Exception as e :
            logger.error("electionLeader %s:%s----:%s" % (server[0], server[1], e))
            theOk = False
            factoryService = self.electionLeader(index + 1)
        if theOk :
            self.thisServer = self.groupServers[index]
            logger.info("leader server is %s:%s" % (server[0], server[1]))
        return factoryService
    def getLeaderIndex(self, sa):
        i = 0
        for server in self.groupServers :
            if sa == server :
                break
            i += 1
        return i

    def setMaster(self, isMaster, factory):
        self.master = isMaster
        if isMaster :
            logger.info("leader server is %s" % self.thisServer)
        if self.master :
            HbDaemon.runClearTask(factory)

    def getMaster(self):
        try :
            if self.master :
                return self.thisServer.split(":")
        except Exception as e :
            logger.error(e)

    def getOtherFactory(self):
        factorys = []
        for str in self.groupServers :
            if str != self.thisServer :
                server = str.split(":")
                try :
                    pk = RPCContext.getService(server[0], int(server[1]),
                                               self.serviceName)
                    clientHost = pk.getClientHost() #check alive state,if not connected,raise CommunicationError
                    factorys.append(pk)
                except Exception as e :
                    if isinstance(e,Pyro4.errors.CommunicationError) :
                        logger.error("%s maybe is shutdown,can't connected." % str)

        return factorys

    def checkMasterFactory(self, sv, factory):
        if self.master or not self.getOtherMasterFactory(sv) :
            sv = self.thisServer.split(":")
            self.setMaster(True, factory)
            return True
        else :
            return False

    def getOtherMasterFactory(self, sv):
        pkMaster = None

        try :
            for pk in self.getOtherFactory() :
                ask = pk.askMaster()

                if ask :
                    pkMaster = pk
                    sv.append(ask[0])
                    sv.append(ask[1])

        except Exception as e :
            logger.error("getOtherMasterFactory...Error:%s" % e)
        return pkMaster

    def setInitFactoryInfo(self, fromFactory, toFactory):
        try :
            toFactory.setFactoryInfo(fromFactory.getFactoryInfo())
        except Exception as e :
            logger.error("setInitFactoryInfo...Error:%s" % e)



    def runCopyTask(self, domainNodeKey, factory):
        logger.info("runCopyTask:"+domainNodeKey+"............................")
        # def task(vfactory, queue):
        #     curKey = queue.get()
        #     if queue.empty() :
        #         self.copyFactoryInfo(vfactory.getFactoryInfo())
        # self.queue.put(domainNodeKey)
        # self.rpl = AsyncExector(task , (factory, self.queue))
        # self.rpl.run()
        try:
            obj = factory.getFactoryInfo()
            for pk in self.getOtherFactory() :
                pk.setFactoryInfo(obj)
        except Exception as e:
            logger.error("runCopyTask error:%s"%e)

    def copyFactoryInfo(self, obj):
        sendlist = []
        for factory in self.getOtherFactory() :
            sendlist.append(factory.setFactoryInfo(obj))
        return sendlist

#end
