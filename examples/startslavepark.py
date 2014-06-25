import sys
import os
sys.path.extend([os.getcwd() +'\\..\\opencluster'])
from opencluster.beancontext import BeanContext

BeanContext.startSlavePark()
