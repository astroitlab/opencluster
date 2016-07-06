"""
opencluster - Python Distibuted Computing API.
Copyright by www.cnlab.net
"""

import logging
import Pyro4

from opencluster.configuration import Conf, setLogger
from opencluster.servicecontext import ServiceContext
from opencluster.factoryservice import FactoryService
from opencluster.factorylocal import FactoryLocal
from opencluster.workerlocal import WorkerLocal
from opencluster.workerservice import WorkerService

Pyro4.config.SERIALIZER = "pickle"
Pyro4.config.SERIALIZERS_ACCEPTED = set(['pickle'])
Pyro4.config.COMPRESSION = True
Pyro4.config.POLLTIMEOUT = 5
Pyro4.config.SERVERTYPE = "multiplex"    #  multiplex or thread
Pyro4.config.SOCK_REUSE = True

logger = logging.getLogger(__name__)

pyroLoopCondition = True

def checkPyroLoopCondition() :
    global pyroLoopCondition
    return pyroLoopCondition

class FactoryContext(ServiceContext):
    def __init__(self):
        super(FactoryContext,self).__init__()

    @staticmethod
    def setConfigFile(filePath):
        Conf.setConfigFile(filePath)
        
    @classmethod
    def startDefaultFactory(cls):
        servers = Conf.getFactoryServers()
        servs = servers.split(",")
        server = servs[0].split(":")

        cls.startFactory(server[0], int(server[1]), servs, Conf.getFactoryServiceName())

    @classmethod
    def startSlaveFactory(cls):
        servers = Conf.getFactoryServers()
        servs = servers.split(",")
        server = servs[1].split(":")
        
        cls.startFactory(server[0], int(server[1]), servs, Conf.getFactoryServiceName())

    @classmethod
    def startFactory(cls, host, port, servers, serviceName):
        global pyroLoopCondition
        setLogger(serviceName, port)
        try:
            d = Pyro4.Daemon(host=host, port=port)
            factory = FactoryService(host, port, servers, serviceName)
            d.register(factory, serviceName)
            factory.wantBeMaster()
            d.requestLoop(checkPyroLoopCondition)
        except KeyboardInterrupt, e:
            pyroLoopCondition = False
            logger.warning('stopped by KeyboardInterrupt')

    @classmethod
    def startWorker(cls, worker,workerType, host, port, workerOrService="_worker_"):
        global pyroLoopCondition
        try:
            d = Pyro4.Daemon(host=host, port=port)
            d.register(WorkerService(worker), workerType)
            cls.getDefaultFactory().createDomainNode(workerOrService + workerType, "".join(host.split("."))+str(port), host+":"+str(port),True)
            d.requestLoop(checkPyroLoopCondition)
        except KeyboardInterrupt ,e :
            pyroLoopCondition = False
            print "Interrupt by KeyboardInterrupt"


    @classmethod
    def getFactory(cls, host, port, servers, serviceName):
        return FactoryLocal(host, port, serviceName, servers)
        
    @classmethod
    def getDefaultFactory(cls):
        servers = Conf.getFactoryServers()
        servs = servers.split(",")
        server = servs[0].split(":")

        return cls.getFactory(server[0], int(server[1]), servs, Conf.getFactoryServiceName())

    @classmethod
    def getWorker(cls, host, port, workerType,synchronous=False):
        return WorkerLocal(host, port, workerType,synchronous)

    @classmethod
    def getNodeDaemon(cls, node):
        return cls.getService(node.host, node.port, node.name)

    @classmethod
    def startInetServer(cls):
        pass

if __name__ == "__main__" :
    FactoryContext.startDefaultFactory()
