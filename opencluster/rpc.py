import logging
import threading
import Pyro4
from opencluster.configuration import Conf

logger = logging.getLogger(__name__)

class RPCContext(object):
    def __init__(self):
        Pyro4.config.SERIALIZER = "pickle"
        Pyro4.config.SERIALIZERS_ACCEPTED = set(['pickle'])
        Pyro4.config.COMPRESSION = True
        Pyro4.config.POLLTIMEOUT = 5
        Pyro4.config.SERVERTYPE = "multiplex"    #  multiplex or thread
        Pyro4.config.SOCK_REUSE = True
        Pyro4.config.REQUIRE_EXPOSE = False
        Pyro4.config.COMMTIMEOUT = 60

    def start(self, host, port, service_name, service_instance, loop_condition=True):
        self._pyrodaemon = Pyro4.Daemon(host=host, port=port)
        self._pyrodaemon.register(service_instance, service_name)
        self._pyroserverthread = threading.Thread(
            target=self._pyrodaemon.requestLoop,
            name="%s Server" % (service_name,))
        self._pyroserverthread.start()
    
    def stop(self):
        self._pyrodaemon.shutdown()
        self._pyroserverthread.join(5)

    @classmethod
    def getService(cls, host, port, serviceName):
        uri = Conf.getUri(host, port, serviceName)
        return Pyro4.Proxy(uri)

    @classmethod
    def getAsynchronousService(cls, host, port, serviceName):
        uri = Conf.getUri(host, port, serviceName)
        return Pyro4.asyncproxy(Pyro4.Proxy(uri))