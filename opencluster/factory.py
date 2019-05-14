"""
opencluster - Python Distibuted Computing API.
Copyright by www.cnlab.net
"""
import logging
import sys
import Pyro4
import threading
import signal

from opencluster.configuration import Conf, setLogger
from opencluster.rpc import RPCContext
from opencluster.factoryservice import FactoryService
from opencluster.factorylocal import FactoryLocal
from opencluster.workerlocal import WorkerLocal
from opencluster.workerservice import WorkerService
from opencluster.util import spawn

logger = logging.getLogger(__name__)

class FactoryContext(object):
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
        factoryService = FactoryService(host, port, servers, serviceName)
        factory_rpc = RPCContext()
        
        def handle_signal(signNo, stack_frame):
            factory_rpc.stop()
            logger.info("Exiting from factory %s, come back again :-)" %(serviceName) )
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        factory_rpc.start(host, port, serviceName, service_instance=factoryService)
        factoryService.wantBeMaster()
        # server_t = threading.Thread(target=server.start, args=(opts.host, opts.port), name="HTTP server")
        # server_t.start()
        # Now simply wait...
        signal.pause()
    @classmethod
    def startWorker(cls, worker,workerType, host, port, workerOrService="_worker_"):
        global pyroLoopCondition
        try:
            d = Pyro4.Daemon(host=host, port=port)
            d.register(WorkerService(worker), workerType)
            cls.getDefaultFactory().createDomainNode(workerOrService + workerType, "".join(host.split("."))+str(port), host+":"+str(port),True)
            d.requestLoop(checkPyroLoopCondition)
        except KeyboardInterrupt:
            pyroLoopCondition = False
            logger.info("Interrupt by KeyboardInterrupt")


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


if __name__ == "__main__" :
    FactoryContext.startDefaultFactory()
