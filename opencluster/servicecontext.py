import logging
import Pyro4

from opencluster.configuration import Conf

logger = logging.getLogger(__name__)

Pyro4.config.SERIALIZER = "pickle"
Pyro4.config.SERIALIZERS_ACCEPTED = set(['pickle'])
Pyro4.config.COMPRESSION = True
Pyro4.config.POLLTIMEOUT = 5
Pyro4.config.SERVERTYPE = "multiplex"    #  multiplex or thread
Pyro4.config.SOCK_REUSE = True


class ServiceContext(object):

    def __init__(self):
        pass
        
    @classmethod    
    def startService(self, host, port, serviceName, obj, loopCondition=True):
       
        """
        start PyroDaemon
        :rtype : PyroDaemon Thread
        """
        pyro_thread=PyroDaemon(host, port, serviceName, obj, loopCondition)
        pyro_thread.setDaemon(True)
        pyro_thread.start()
        pyro_thread.started.wait()
        
        logger.info("%s started...uri:%s" % (serviceName, pyro_thread.uri))
        return pyro_thread
        
    @classmethod    
    def getService(cls, host, port, serviceName):
        uri = Conf.getUri(host, port, serviceName)
        return Pyro4.Proxy(uri)

    @classmethod
    def getAsynchronousService(cls, host, port, serviceName):
        uri = Conf.getUri(host, port, serviceName)
        return Pyro4.async(Pyro4.Proxy(uri))


class PyroDaemon(Pyro4.threadutil.Thread):
    def __init__(self, host, port, serviceName, obj, loopCondition=True):
        Pyro4.threadutil.Thread.__init__(self)
        self.host = host
        self.port = int(port)
        self.serviceName = serviceName
        self.obj = obj
        self.started=Pyro4.threadutil.Event()
        self.loopCondition = loopCondition
    def run(self):
        self.d = Pyro4.Daemon(host=self.host, port=self.port)
        self.uri = self.d.register(self.obj, self.serviceName)
        self.started.set()
        self.d.requestLoop(self.loopCondition)
