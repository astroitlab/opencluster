import sys
import os
sys.path.extend([os.getcwd() +'..\\opencluster'])
from opencluster.worker import Worker
from opencluster.item import WareHouse
from opencluster.contractor import Contractor
import time
from opencluster.configuration import Conf
class WordCountCtor(Contractor) :
    def __init__(self):
        super(WordCountCtor,self).__init__()
    def giveTask(self, inhouse):
        wks = self.getWaitingWorkers("wordcount")
        wordcount = {}
        hmarr = self.doTaskBatch(inhouse,wks)

        for hm in hmarr :
            wordhm = hm.getObj("word")
            for curword in wordhm.keys() :
                if wordcount.has_key(curword) :
                    wordcount[curword] = wordcount[curword] + wordhm[curword]
                else:
                    wordcount[curword] = wordhm[curword]
        wh = WareHouse()
        wh.setObj("word",wordcount)
        return wh

if __name__ == "__main__" :

    wk = WordCountCtor()
    wh = WareHouse()
    wh.setObj("filepath",os.getcwd() +"\\logs\\wordcount.log")
    bt = time.time()
    result = wk.giveTask(wh)
    print time.time()-bt
    print result.getObj("word")

