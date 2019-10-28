import logging
import threading

logger = logging.getLogger(__name__)

class AsyncExector(object):
    def __init__(self, task, args):
        self.task=task
        self.args=args
    def run(self):
        try :
            t = threading.Thread(target=self.task, name=target.__name__, args=self.args, kwargs=None)
            t.start()
        except Exception as e: 
            logger.error("AsyncExector task :%s" % (e))
