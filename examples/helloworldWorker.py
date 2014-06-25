import sys
import os
sys.path.extend([os.getcwd() +'..\\opencluster'])
from opencluster.worker import Worker
from opencluster.item import WareHouse
from opencluster.configuration import Conf
class HelloWorldWorker(Worker) :
    def __init__(self,name):
        super(HelloWorldWorker,self).__init__()
        self.name = name
    def doTask(self, inhouse):
        print "do task :" + inhouse.getObj("word")
        wh = WareHouse(True)
        wh.setObj("word", "hello, i am " +self.name)
        wms = self.getWorkerElse("helloworker")
        for wm in wms :
            wm.receive(wh)
        return wh
    def receive(self,inhouse):
        print inhouse.getObj("word")
        return True
if __name__ == "__main__" :
    serverStr = Conf.getWorkerServers()
    server = serverStr.split(":")

    wk = HelloWorldWorker("worker1")
    wk.waitWorking("helloworker",server[0],int(server[1]))
