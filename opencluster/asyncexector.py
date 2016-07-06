import logging
import thread

logger = logging.getLogger(__name__)

class AsyncExector(object):
    def __init__(self, task, args):
        self.task=task
        self.args=args
    def run(self):
        try :
            thread.start_new_thread(self.task, self.args)    
        except Exception , e: 
            logger.error("AsyncExector task :%s" % (e))
