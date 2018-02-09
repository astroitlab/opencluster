import sys
import os
sys.path.extend([os.path.join(os.path.abspath(os.path.dirname(__file__)),'..')])
from opencluster.factory import FactoryContext

FactoryContext.startDefaultFactory()
