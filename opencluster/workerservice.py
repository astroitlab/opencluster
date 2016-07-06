import logging
import threading
import cPickle
import traceback

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
            task =  cPickle.loads(decompress(v_task))
            return compress(cPickle.dumps(self.worker.doTask(task.data),-1))
        except Exception, e:
            print '>>> traceback <<<'
            traceback.print_exc()
            print '>>> end of traceback <<<'
            logger.error(e)
            raise ServiceError(e)

    def stopTask(self):
        #to-do, need condition
        self.worker.interrupted(True)

    def receiveMaterials(self, v_task):
        return self.worker.receiveMaterials(cPickle.loads(decompress(v_task)))
