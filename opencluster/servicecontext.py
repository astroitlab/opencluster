
import logging
import threading
import Pyro4
from configuration import Conf


logger = logging.getLogger(__name__)

class ServiceContext(object):
    Pyro4.config.SERIALIZER = "pickle"
    Pyro4.config.SERIALIZERS_ACCEPTED = set(['pickle'])   
    Pyro4.config.COMPRESSION = True
    Pyro4.config.SERVERTYPE = "thread"    #  multiplex or thread    
    def __init__(self):
        pass
        
    @classmethod    
    def startService(self, host, port, servicename, obj):
       
        """
        start PyroDaemon
        :rtype : PyroDaemon Thread
        """
        pyro_thread=PyroDaemon(host, port, servicename, obj)
        pyro_thread.setDaemon(True)
        pyro_thread.start()
        pyro_thread.started.wait()
        
        logger.info("%s started...uri:%s" % (servicename,pyro_thread.uri))
        return pyro_thread
        
    @classmethod    
    def getService(cls, host, port, servicename):
        uri = Conf.getUri(host, port, servicename)
        
        return Pyro4.Proxy(uri)
    def getClientHost(self):
        return ""

    @classmethod
    def startWeb(cls,host, port):
        web_thread=WebDaemon(host, port)
        web_thread.start()
        logger.info("web started...uri:%s:%s" % (host,port))

class PyroDaemon(Pyro4.threadutil.Thread):
    def __init__(self, host, port, servicename, obj):
        Pyro4.threadutil.Thread.__init__(self)
        self.host=host
        self.port=int(port)
        self.servicename=servicename
        self.obj=obj
        self.started=Pyro4.threadutil.Event()
    def run(self):
        self.d=Pyro4.Daemon(host=self.host, port=self.port)
        self.uri=self.d.register(self.obj, self.servicename)
        self.started.set()
        self.d.requestLoop()
        
class WebDaemon(threading.Thread):
    def __init__(self, host, port):
        threading.Thread.__init__(self)
        self.host=host
        self.port=port
        self.setDaemon(1)

    def run(self):
        pass