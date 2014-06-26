"""
opencluster - Python Distibuted Computing API.
Copyright by www.cnlab.net
"""
import os
import sys
import signal
import configuration
import threading
import time

from servicecontext import ServiceContext
from parkservice import ParkService
from parklocal import ParkLocal
from workman import Workman
from workservice import WorkService

def sigint_handler(signum, frame):
    print "sign_handler"
    sys.exit()

class BeanContext(ServiceContext):
    def __init__(self):
        super(BeanContext,self).__init__()

    @staticmethod
    def setConfigFile(filePath):
        configuration.Conf.setConfigFile(filePath)
        
    @classmethod
    def startDefaultPark(cls):
        servers = configuration.Conf.getParkServers()
        servs = servers.split(",")
        server = servs[0].split(":")
        
        cls.startPark(server[0], int(server[1]), servs, configuration.Conf.getParkServiceName())
    @classmethod
    def startSlavePark(cls):
        servers = configuration.Conf.getParkServers()
        servs = servers.split(",")
        server = servs[1].split(":")
        
        cls.startPark(server[0], int(server[1]), servs, configuration.Conf.getParkServiceName())
    @classmethod
    def startPark(cls, host, port, servers, serviceName):
        pyro_thread = cls.startService(host, port, serviceName, ParkService(host, port, servers, serviceName))
        park = cls.getService(host, port, serviceName)
        park.wantBeMaster()

        if configuration.Conf.getStartWebapp() :
            serverStr = configuration.Conf.getWebServers()
            server = serverStr.split(":")
            cls.startWeb(server[0],server[1])

        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
        pyro_thread.join()

    @classmethod
    def startWorker(cls, host, port, workerType, worker):
        pyro_thread = cls.startService(host, port, workerType, WorkService(worker))
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
        cls.getDefaultPark().createDomainNode("_worker_" + workerType, str(time.time()).replace(".", ""), host+":"+str(port),True)
        try:
            while True :
                time.sleep(10)
        except KeyboardInterrupt ,e :
            sys.exit(0)

    @classmethod
    def getPark(cls, host, port, servers, serviceName):
        return ParkLocal(host, port, serviceName, servers)
        
    @classmethod
    def getDefaultPark(cls):
        servers = configuration.Conf.getParkServers()
        servs = servers.split(",")
        server = servs[0].split(":")

        return cls.getPark(server[0], int(server[1]), servs, configuration.Conf.getParkServiceName())
    @classmethod
    def getWorkman(cls, host, port, workerType):
        return Workman(host, port, workerType)

    @classmethod
    def startInetServer(cls):
        pass

if __name__ == "__main__" :
    BeanContext.startDefaultPark()
    # BeanContext.startSlavePark()
