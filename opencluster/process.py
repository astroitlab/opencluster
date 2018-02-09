import os
import logging

from opencluster.configuration import Conf

logger = logging.getLogger(__name__)

class WorkerProcess(object) :
    def __init__(self,host,workerType,workerScript):
        self.workerType = workerType
        self.workerScript = workerScript
        self.host = host
    def start(self):
        cmd = os.path.join(Conf.getNodeWorkerDir(),"worker.sh") + " " + self.workerScript + " start " + self.host
        return os.system(cmd)

    def stop(self,port):
        filenames = self.workerScript.split("/")
        filename = filenames[-1]
        cmd = os.path.join(Conf.getNodeWorkerDir(),"worker.sh") + " " + filename + " stop " + str(port)

        return os.system(cmd)

    def stopAll(self):
        filenames = self.workerScript.split("/")
        filename = filenames[-1]
        cmd = os.path.join(Conf.getNodeWorkerDir(),"worker.sh")  + " " + filename + " stopAll";
        return os.system(cmd)

class ServiceProcess(object) :

    def __init__(self,host,serviceName,serviceScript):
        self.serviceName = serviceName
        self.serviceScript = serviceScript
        self.host = host
    def start(self):
        cmd = os.path.join(Conf.getNodeServiceDir(),"service.sh") + " " + self.serviceScript + " start " + self.host
        return os.system(cmd)

    def stop(self,port):
        logger.info("stop")
        filenames = self.serviceScript.split("/")
        filename = filenames[-1]
        cmd = os.path.join(Conf.getNodeServiceDir(),"service.sh") + " " + filename + " stop " + str(port)
        logger.info(cmd)
        return os.system(cmd)

    def stopAll(self):
        filenames = self.serviceScript.split("/")
        filename = filenames[-1]
        cmd = os.path.join(Conf.getNodeServiceDir(),"service.sh") + " " + filename  + " stopAll";
        logger.info(cmd)
        return os.system(cmd)