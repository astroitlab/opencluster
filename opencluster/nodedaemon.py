import sys
import signal
import socket
import Pyro4
import zmq

from opencluster.configuration import Conf, setLogger
from opencluster.node import *
from opencluster.factorypatternexector import FactoryPatternExector
from opencluster.errors import *
from opencluster.service import *
from opencluster.process import ServiceProcess, WorkerProcess
from opencluster.util import spawn

Pyro4.config.SERIALIZER = "pickle"
Pyro4.config.SERIALIZERS_ACCEPTED = set(['pickle'])
Pyro4.config.COMPRESSION = True
Pyro4.config.SERVERTYPE = "multiplex"    #  multiplex or thread


logger = logging.getLogger(__name__)

pyroLoopCondition = True
pyroUri = None

def checkPyroLoopCondition() :
    global pyroLoopCondition
    return pyroLoopCondition

class NodeService(object) :
    def __init__(self,node):
        self.node = node
        self.nodeFile = NodeFile(self.node.diskPath)

    def startNewWorker(self,workerType):
        logger.info("start new worker instance of " + workerType)
        if workerType not in self.node.availWorkers :
            return ServiceError( workerType + " is not supported in this node.")
        workerPro = self.node.availWorkers[workerType]
        return workerPro.start()

    def getNode(self):
        return self.node

    def stopWorker(self,workerType,port):
        logger.info("kill worker instance of %s:%s"%(workerType,port))
        workerType = workerType[8:] #workertype like "_worker_workerUVFITS"
        if workerType not in self.node.availWorkers :
            logger.info(workerType + " is not supported in this node.")
            return ServiceError( workerType + " is not supported in this node.")

        workerPro = self.node.availWorkers[workerType]
        return workerPro.stop(port)

    def stopAllWorker(self,workerType):
        logger.info("kill worker instance of %s"%(workerType))
        if workerType not in self.node.availWorkers :
            logger.info(workerType + " is not supported in this node.")
            return ServiceError( workerType + " is not supported in this node.")

        workerPro = self.node.availWorkers[workerType]
        return workerPro.stopAll()

    def startNewService(self,serviceName):
        logger.info("start new service instance of " + serviceName)
        if serviceName not in self.node.availServices :
            return ServiceError( serviceName + " is not supported in this node.")
        servicePro = self.node.availServices[serviceName]
        return servicePro.start()

    def stopService(self,serviceName,port):
        logger.info("kill service instance of %s:%s"%(serviceName,port))
        serviceName = serviceName[9:] #serviceName like "_service_XXXX"
        if serviceName not in self.node.availServices :
            return ServiceError( serviceName + " is not supported in this node.")
        servicePro = self.node.availServices[serviceName]
        return servicePro.stop(port)

    def stopAllService(self,serviceName):
        logger.info("kill all service instance of %s"%(serviceName))
        if serviceName not in self.node.availServices :
            return ServiceError(serviceName + " is not supported in this node.")
        servicePro = self.node.availServices[serviceName]
        return servicePro.stopAll()

    '''
    file operation---------------------------------------------------------------------------
    '''
    def listFiles(self,relFilePath):
        return self.nodeFile.listDir(relFilePath)

    def fReadByte(self,fileName,begin,length):
        return self.nodeFile.readByte(fileName,begin,length)

    def fReadFile(self,fileName):
        return self.nodeFile.readWholeFile(fileName)

    def fWriteByte(self,fileName,begin,length):
        pass

    def fWriteFile(self,fileName,fileByte):
        return self.nodeFile.writeFile(fileName,fileByte)

    def fcreate(self,fileName):
        pass
    def fCreateDir(self,curDir,dirName):
        self.nodeFile.createDir(curDir,dirName)

    def fDelete(self,fileName):
        return self.nodeFile.remove(fileName)

    def fcopy(self,fileName):
        pass

    def fRename(self,fileName,newName):
        return self.nodeFile.rename(fileName,newName)

class NodeDademon(object):
    def __init__(self,host, port, name):
        global pyroLoopCondition
        global pyroUri

        setLogger("Node-%s"%host,port)

        self.node = Node(host, port, "Node" + name)
        for workerStr in Conf.getNodeAvailWorkers().split(",") :
            worker = workerStr.split("|")
            self.node.availWorkers[worker[0]] = WorkerProcess(host,worker[0],worker[1])
        for serviceStr in Conf.getNodeAvailServices().split(",") :
            service = serviceStr.split("|")
            self.node.availServices[service[0]] = ServiceProcess(host,service[0],service[1])

        self.node = self.node.calRes()

        def runNodeService():
            global pyroUri,pyroLoopCondition
            d = Pyro4.Daemon(host=host, port=port)
            pyroUri = d.register(NodeService(self.node), self.node.name)
            d.requestLoop(checkPyroLoopCondition)
        spawn(runNodeService)
        time.sleep(1)

        logger.info("Node:%s started...uri:%s" % (self.node.name,pyroUri))
        try :
            FactoryPatternExector.createPhysicalNode(self.node.calRes())
            self.start()
        except KeyboardInterrupt:
            pyroLoopCondition = False
            logger.warning('stopped by KeyboardInterrupt')
            sys.exit(1)

    def start(self):
        context = zmq.Context(1)
        backend = context.socket(zmq.ROUTER)  # ROUTER

        backend.bind("tcp://*:%s" % Conf.getNodePortForService())  # For services

        poll_services = zmq.Poller()
        poll_services.register(backend, zmq.POLLIN)
        heartbeat_at = time.time() + Conf.getExpiration()

        while checkPyroLoopCondition():
            socks = dict(poll_services.poll((Conf.getExpiration()-1) * 1000))
            # Handle service activity on backend
            if socks.get(backend) == zmq.POLLIN:
                # Use service address for LRU routing
                frames = backend.recv_multipart()
                if not frames:
                    break

                addressList = str(frames[0]).split(":")
                address = addressList[0]
                port = int(addressList[1])
                serviceName = addressList[2]
                self.node.services.pop("%s:%s:%s"%(address,port,serviceName),None)
                self.node.services["%s:%s:%s"%(address,port,serviceName)] = Service(address,port,serviceName)

                ov = FactoryPatternExector.createServiceNode(address,port,serviceName)

                msg = frames[1:]
                if len(msg) == 1:
                    if msg[0] not in (Conf.PPP_READY, Conf.PPP_HEARTBEAT):
                        logger.error("Invalid message from service: %s" % msg)
                if time.time() >= heartbeat_at:
                    for service in self.node.services:
                        msg = [service, Conf.PPP_HEARTBEAT]
                        backend.send_multipart(msg)
                    heartbeat_at = time.time() + Conf.HEARTBEAT_INTERVAL

            expired = []
            t = time.time()
            for address,service in self.node.services.iteritems():
                if t > service.expiry:  # Worker expired
                    expired.append(address)
            for address in expired:
                logger.info("Service expired: %s" % address)
                self.node.services.pop(address, None)

            #update node of factory
            FactoryPatternExector.updatePhysicalNode(self.node.calRes())
        #end while true
    #end def start()
#end class NodeDademon

if __name__ == "__main__" :
    def handler(signm, frame):
        global pyroLoopCondition
        pyroLoopCondition = False
        logger.warning("got signal %d, exit now", signm)
        sys.exit(1)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGABRT, handler)
    NodeDademon("localhost",Conf.getNodeDefaultPort(),socket.gethostname())
