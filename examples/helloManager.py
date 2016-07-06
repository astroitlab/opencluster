import sys
import os
import time
import datetime
import random
import cPickle

sys.path.extend([os.path.join(os.path.abspath(os.path.dirname(__file__)),'..')])
from opencluster.util import decompress
from opencluster.item import Task,Success
from opencluster.manager import Manager

class HelloManager(Manager) :
    def __init__(self):
        super(HelloManager,self).__init__()

if __name__ == "__main__" :
    wk = HelloManager()
    wk.init_cmd_option()

    try :

        tasks = []

        print os.path.dirname(os.path.abspath(__file__))

        for i in range(0,5) :
            j = str(random.randint(0, 9000000)).ljust(7,'0')
            taskId = "%s%s" % (datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"),j)
            task = Task(taskId,workerClass="helloWorker.HelloWorker",workDir = os.path.dirname(os.path.abspath(__file__)))
            task.data = taskId + " world"
            bt = time.time()
            tasks.append(task)
        tasksLen = len(tasks)
        wk.schedule(tasks)

        i = 1

        for e in wk.completionEvents() :
            if isinstance(e.reason,Success) :
                print i,e.task.id,cPickle.loads(decompress(e.result))
            else:
                print i,"Failed:" + str(e.reason.message)
            i += 1

        time.sleep(1)
        wk.shutdown()


    except KeyboardInterrupt:
        print "stopped by KeyboardInterrupt"
        sys.exit(1)



