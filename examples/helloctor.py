import sys
import os
sys.path.extend([os.getcwd() +'..\\opencluster'])
from opencluster.worker import Worker
from opencluster.item import WareHouse
from opencluster.contractor import Contractor

from opencluster.configuration import Conf
class HelloCtor(Contractor) :
    def __init__(self):
        super(HelloCtor,self).__init__()
    def giveTask(self, inhouse):
        wks = self.getWaitingWorkers("helloworker")

        print "workers length:",len(wks)

        wh = WareHouse(True)
        wh.setObj("word","hello, i am your Contractor.")

        hmarr = self.doTaskBatch(wh,wks)

        for result in hmarr :
            print result

if __name__ == "__main__" :


    wk = HelloCtor()
    wk.giveTask(None)

