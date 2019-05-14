import os, logging
import time
import socket
import shutil

from opencluster import util
from opencluster.configuration import Conf

logger = logging.getLogger(__name__)

class OpenClusterEnv:
    environ = {}
    @classmethod
    def register(cls, name, value):
        cls.environ[name] = value
    @classmethod
    def get(cls, name, default=None):
        return cls.environ.get(name, default)

    def __init__(self):
        self.started = False

    def start(self, isMaster, environ={}):
        if self.started:
            return
        logger.debug("start env in %s: %s %s", os.getpid(), isMaster, environ)
        self.isMaster = isMaster
        if isMaster:
            roots = Conf.getRootDir()
            if isinstance(roots, str):
                roots = roots.split(',')
            name = '%s-%s-%d' % (time.strftime("%Y%m%d-%H%M%S"),
                socket.gethostname(), os.getpid())
            self.workdir = [os.path.join(root, name) for root in roots]
            for d in self.workdir:
                util.mkdir_p(d)
            self.environ['SERVER_URI'] = 'file://' + self.workdir[0]
            self.environ['WORKDIR'] = self.workdir
            self.environ['COMPRESS'] = util.COMPRESS
        else:
            self.environ.update(environ)
            if self.environ['COMPRESS'] != util.COMPRESS:
                raise Exception("no %s available" % self.environ['COMPRESS'])

        #self.ctx = zmq.Context()
        self.started = True
        logger.debug("env started")

    def stop(self):
        if not getattr(self, 'started', False):
            return
        logger.debug("stop env in %s", os.getpid())


        logger.debug("cleaning workdir ...")
        for d in self.workdir:
            shutil.rmtree(d, True)
        logger.debug("done.")

        self.started = False

env = OpenClusterEnv()
