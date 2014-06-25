import sys
import os
sys.path.extend([os.getcwd() +'..\\opencluster'])
from opencluster.worker import Worker
from opencluster.item import WareHouse
from opencluster.configuration import Conf

class WordCountWorker(Worker) :

    def __init__(self,name):
        super(WordCountWorker,self).__init__()
        self.name = name
    def doTask(self, inhouse):
        filepath = inhouse.getObj("filepath")

        wordcount = {}
        fp = open(filepath,'r')
        i = 0
        for line in fp.readlines():
            line = line.strip()
            for word in line.split(" ") :
                if wordcount.has_key(word) :
                    wordcount[word] += 1
                else:
                    wordcount[word] = 1


        fp.close()

        wh = WareHouse(True)
        wh.setObj("word",wordcount)
        print wh
        return wh

if __name__ == "__main__" :
    serverStr = Conf.getWorkerServers()
    server = serverStr.split(":")

    wk = WordCountWorker("worker1")
    wk.waitWorking("wordcount",server[0],int(server[1]))

