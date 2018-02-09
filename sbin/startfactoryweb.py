import sys
import os
import logging

sys.path.extend([os.path.join(os.path.abspath(os.path.dirname(__file__)),'..')])
from opencluster.ui.main import WebServer
from opencluster.configuration import Conf,setLogger

logger = logging.getLogger(__name__)
if __name__ == "__main__" :
    setLogger("OCWeb","")
    thisServer = WebServer(Conf.getWebServers())
    thisServer.start()