import random
import logging

from opencluster.worker import Worker
from opencluster.factory import FactoryContext
from opencluster.util import port_opened

class Service(Worker):

    def __init__(self,workerType, level=logging.DEBUG):
        super(Service, self).__init__(workerType,level)

    def waitWorkingByService(self, host, port=None) :
        self.host = host
        self.port = port

        if self.host is None:
            self.host = "127.0.0.1"

        if self.port is None:
            self.port = random.randint(30000, 40000)

        while port_opened(self.host, self.port) :
            self.port = random.randint(30000, 40000)

        FactoryContext.startWorker(self, self.workerType, self.host, self.port, "_service_")