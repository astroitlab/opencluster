import sys
import os
sys.path.extend([os.getcwd() +'\\..\\opencluster'])
from opencluster.beancontext import BeanContext

pk = BeanContext.getPark()

pk.createDomainNode("group","node1","1",True)


