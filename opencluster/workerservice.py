import logging
import threading
import pickle
import traceback
import Pyro4

from opencluster.util import compress, decompress
from opencluster.errors import ServiceError

logger = logging.getLogger(__name__)

class WorkerService(object):
    def __init__(self, worker):
        self.worker = worker
        self.condition = threading.Condition()

    def setWorker(self, worker):
        worker.host = self.worker.host
        worker.port = self.worker.port
        worker.workerType = self.worker.workerType
        self.worker = worker

    def doTask(self, v_task):
        try :
            task =  pickle.loads(decompress(v_task))
            return compress(pickle.dumps(self.worker.doTask(task.data),-1))
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            raise ServiceError(e)

    def stopTask(self):
        #to-do, need condition
        self.worker.interrupted(True)

    def receive(self, v_task):
        return self.worker.receive(pickle.loads(decompress(v_task)))
