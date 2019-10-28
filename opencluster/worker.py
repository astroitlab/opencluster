import datetime
import os, sys
import random
import logging
import optparse
import signal

from opencluster.configuration import setLogger,Conf
from opencluster.workerparallel import WorkerParallel
from opencluster.rpc import RPCContext
from opencluster.util import port_opened
from opencluster.factorypatternexector import FactoryPatternExector
from opencluster.workerservice import WorkerService
from opencluster.util import norm_host_str

logger = logging.getLogger(__file__)


parser = optparse.OptionParser(usage="Usage: python %prog [options]")

def add_default_options():
    parser.disable_interspersed_args()
    parser.add_option("-i", "--host", type="string", default="localhost")
    parser.add_option("-p", "--port", type="int", default=0, help="port")
    parser.add_option("-r", "--retry", type="int", default=0, help="retry times when failed (default: 0)")
    parser.add_option("-c", "--cpus", type="float", default=1.0, help="cpus used")
    parser.add_option("-G", "--gpus", type="float", default=0, help="gpus used")
    parser.add_option("-M", "--mem", type="float", help="memory used")
    parser.add_option("-e", "--config", type="string", default="/work/opencluster/config.ini",
                      help="path for configuration file")
    parser.add_option("-q", "--quiet", action="store_true", help="be quiet", )
    parser.add_option("-v", "--verbose", action="store_true", help="show more useful log", )

add_default_options()

def parse_options():
    (options, args) = parser.parse_args()
    if not options:
        parser.print_help()
        sys.exit(2)

    if options.mem is None:
        options.mem = Conf.MEM_PER_TASK

    options.logLevel = (options.quiet and logging.ERROR or options.verbose and logging.DEBUG or logging.INFO)

    if options.config:
        if os.path.exists(options.config) and os.path.isfile(options.config):
            Conf.setConfigFile(options.config)
        else:
            logger.error("configuration file is not found. (%s)" %(options.config,))
            sys.exit(2)
    return options

class Worker(WorkerParallel):
    def __init__(self, workerType):
        super(Worker,self).__init__()
        self.options = parse_options()
        self.host = self.options.host
        self.port = self.options.port
        self.workerType = workerType
        self._interrupted = False
        j = str(random.randint(0, 9000000)).ljust(7,'0')
        workerId = "%s%s" % (datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"),j)
        setLogger(self.workerType, workerId, self.options.logLevel)

    def doTask(self, task):
        pass
    def stopTask(self):
        pass
    
    def waitWorkingByService(self, host, port) :

        if self.port == 0:
            self.port = random.randint(30000, 40000)

        while port_opened(self.host, self.port) :
            self.port = random.randint(30000, 40000)

        rpc = RPCContext()
        def handle_signal(signNo, stack_frame):
            rpc.stop()
            logger.info("Exiting from work %s, come back again :-)" %(self.workerType) )
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        rpc.start(self.host, self.port, self.workerType, service_instance=WorkerService(self))
        FactoryPatternExector.getFactory().createDomainNode("_worker_" + self.workerType, norm_host_str(self.host)+str(self.port), self.host+":"+str(self.port),True)
        signal.pause()

    def getWorkerIndex(self, index, workerType=None):
        if not workerType :
            return self.getWorkerIndex(index, self.workerType)
            
        wsList = self.getWorkerList(workerType)
        if index >= 0 and index < len(wsList) :
            wsInfo = wsList[index]
            return FactoryPatternExector.getWorker(wsInfo[0], int(wsInfo[1]), wsInfo[2])

    def getWorkerAll(self, workerType=None):
        if not workerType : 
            return self.getWorkers(None, 0, self.workerType)
        return self.getWorkers(None, 0, workerType)
    
    def getWorkers(self, host, port, workerType):
        wkList = []
        wsList = self.getWorkerList(workerType)
        i = 0
        for wsInfo in wsList :
            if self.host != wsInfo[0] and self.port != int(wsInfo[1]) :
                wkList.append(FactoryPatternExector.getWorker(wsInfo[0], int(wsInfo[1]), wsInfo[2]))
            else :
                self.selfIndex = i
            i = i + 1
        return wkList
    def getSelfIndex(self):
        if self.selfIndex == -1 :
            wsList = self.getWorkerList(self.workerType)
            i = 0
            for wsInfo in wsList :
                if self.host == wsInfo[0] and self.port == int(wsInfo[1]) :
                    return i
                i = i + 1
                
        return self.selfIndex
    
    def getWorkerElse(self, workerType=None, host=None, port = None):
        if not workerType :
            workerType = self.workerType
            
        if host is None or port is None :
            return self.getWorkers(self.host, self.port, workerType)
        
        
        if self.host == host and self.port == port :
            return None
        wsList = self.getWorkerList(workerType)
        for wsInfo in wsList :
            if self.host == wsInfo[0] and self.port == int(wsInfo[1]) :
                return FactoryPatternExector.getWorker(wsInfo[0], int(wsInfo[1]), wsInfo[2])
        return None
        
    def receive(self, task):
        return True

    def setSelfIndex(self, i):
        self.selfIndex = i
        
    def isInterrupted(self):
        return self._interrupted
    
    def interrupted(self,  interrupted) :
        self._interrupted = interrupted

    def getServices(self,serviceName, asynchronous = False, worker = None):
        wslist = self.getServiceList(serviceName)
        wklist = []
        for wsinfo in wslist :
            wklist.append(FactoryPatternExector.getWorker(wsinfo[0],int(wsinfo[1]),wsinfo[2],asynchronous))

        return  wklist

    def getWorkerList(self, workerType, host=None, port=None):
        obList = FactoryPatternExector.getWorkerTypeList(workerType);
        wsList = []
        if not obList is None :
            for obj in obList :
                hostPort = str(obj.obj).split(":")
                if hostPort[0] != host or int(hostPort[1]) != port:
                    wsList.append([hostPort[0], hostPort[1], workerType])
        return wsList

    def getServiceList(self, serviceName, host=None, port=None):
        obList = FactoryPatternExector.getServiceNameList(serviceName);
        wsList = []
        if not obList is None :
            for obj in obList :
                hostPort = str(obj.obj).split(":")
                if hostPort[0] != host or int(hostPort[1]) != port:
                    wsList.append([hostPort[0], hostPort[1], serviceName])
        return wsList