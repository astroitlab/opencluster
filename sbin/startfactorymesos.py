import sys
import os
sys.path.extend([os.path.join(os.path.abspath(os.path.dirname(__file__)),'..')])
from opencluster.factorymesos import start_factory_mesos

start_factory_mesos()
