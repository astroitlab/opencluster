import sys
import os
import time
import logging
import socket

sys.path.extend([os.path.join(os.path.abspath(os.path.dirname(__file__)),'..')])
from opencluster.worker import Worker

from opencluster.configuration import Conf

logger = logging.getLogger(__name__)

class HelloWorker(Worker) :
    def __init__(self):
        super(HelloWorker,self).__init__(workerType="helloWorker")

    def doTask(self, task_data):
        logger.debug("begin...HelloWorker.... :" + task_data)
        time.sleep(1)
        return socket.gethostname() + " say: hello " + task_data

if __name__ == "__main__" :
    wk = HelloWorker()

    if len(sys.argv) != 2 :
        print "Usage: python helloWorker.py localIP"
        sys.exit(1)

    wk.waitWorking(sys.argv[1])

