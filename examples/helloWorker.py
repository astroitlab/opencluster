import sys
import os
import time
import logging
import socket

from opencluster.worker import Worker
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
    wk.waitWorking()

