import logging
import os
import sys
import os.path
import cPickle
import socket

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from opencluster.configuration import Conf,setLogger

logger = logging.getLogger("opencluster.executor@%s" % socket.gethostname())

from kafka import KafkaConsumer,KafkaClient,KeyedProducer

class DockerExecutor(object):
    def __init__(self,warehouse,warehouse_result):
        self.warehouse = warehouse
        self.warehouse_result = warehouse_result

        self.kafka = KafkaClient(Conf.getWareHouseAddr())
        self.producer = KeyedProducer(self.kafka)
        self.consumer = KafkaConsumer(self.warehouse,
                               bootstrap_servers=[Conf.getWareHouseAddr()],
                               group_id="cnlab",
                               auto_commit_enable=True,
                               auto_commit_interval_ms=30 * 1000,
                               auto_offset_reset='smallest')

    def run(self):
        i=1
        for message in self.consumer.fetch_messages():
            logger.debug("%d,%s:%s:%s: key=%s " % (i,message.topic, message.partition, message.offset, message.key))
            task = cPickle.loads(message.value)
            i = i + 1
            result = task.run(0)
            self.producer.send_messages(self.warehouse_result, task.id, cPickle.dumps(result))

if __name__ == '__main__':
    setLogger("docker-executor",socket.gethostname())
    logger.debug("Staring executor")
    executor = DockerExecutor(sys.argv[1],sys.argv[2])
    executor.run()
