import sys
import os
import socket

sys.path.extend([os.path.join(os.path.abspath(os.path.dirname(__file__)),'..')])
from opencluster.nodedaemon import NodeDademon
from opencluster.configuration import Conf

if __name__ == "__main__" :
    if len(sys.argv) != 2 :
        print "Usage : %s LocalIP" % sys.argv[0]
        sys.exit(1)

    NodeDademon(sys.argv[1],Conf.getNodeDefaultPort(),"".join(sys.argv[1].split(".")))