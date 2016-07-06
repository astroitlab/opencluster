import sys
import os
sys.path.extend([os.path.join(os.path.abspath(os.path.dirname(__file__)),'..')])
from opencluster.ui.main import WebServer
from opencluster.configuration import Conf

if __name__ == "__main__" :
    thisServer = WebServer(Conf.getWebServers())
    thisServer.start()