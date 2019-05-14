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
from opencluster.configuration import Conf

loopCondition = True

class HelloManager(Manager) :
    def __init__(self):
        super(HelloManager,self).__init__()

if __name__ == "__main__" :

    def check_kafka_events():
        global loopCondition
        from kafka import KafkaConsumer, KafkaClient, SimpleProducer
        warehouse_addr = Conf.getWareHouseAddr()
        consumer = KafkaConsumer("%sResult"%wk.options.warehouse,
                               bootstrap_servers=[warehouse_addr],
                               group_id="cnlab",
                               auto_commit_enable=True,
                               auto_commit_interval_ms=30 * 1000,
                               auto_offset_reset='smallest')

        while loopCondition:
            for message in consumer.fetch_messages():
                print ("topic=%s, partition=%s, offset=%s, key=%s" % (message.topic, message.partition, message.offset, message.key))
                task = cPickle.loads(message.value)

                if task.state == Task.TASK_FINISHED:
                    print "taskId:%s,success!!!:%s"%(task.id,task.result)
                else:
                    print "taskId:%s,failed!!!"%task.id

                consumer.task_done(message)
                last_data_time = time.time()
                if not loopCondition:
                    break

    wk = HelloManager()
    wk.init_cmd_option()

    try :
        #update 2016-10-10
        tasks = []

        start = time.time()
        for i in range(0,5) :
            j = str(random.randint(0, 9000000)).ljust(7,'0')
            taskId = "%s%s" % (datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"),j)
            task = Task(taskId,workerClass="helloWorker.HelloWorker",workDir = os.path.dirname(os.path.abspath(__file__)))
            task.data = taskId + " world"
            # task.priority = 1
            # task.resources = {"cpus":1,"mem":100,"gpus":0}
            # task.warehouse = wk.options.warehouse
            bt = time.time()
            tasks.append(task)
        tasksLen = len(tasks)
        wk.schedule(tasks)

        i = 1

        if wk.mode=="factory":
            check_kafka_events()
        else:

            for e in wk.completionEvents() :
                if isinstance(e.reason,Success) :
                    print i,e.task.id,cPickle.loads(decompress(e.result))
                else:
                    print i,"Failed:" + str(e.reason.message)
                i += 1
            print "%d tasks finished in %.1f seconds" % (tasksLen, time.time() - start)

        time.sleep(1)
        wk.shutdown()


    except KeyboardInterrupt:
        print "stopped by KeyboardInterrupt"
        loopCondition = False
        sys.exit(1)



