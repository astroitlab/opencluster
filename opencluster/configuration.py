import os,sys
import ConfigParser
import logging
import logging.handlers

logger = logging.getLogger()

def setLogger(name,id,level=logging.DEBUG):
    if sys.platform == "win32":
        logPathFmt = "E:\\astrodata\\logs\\%s-%s.log"
    else:
        logPathFmt = "/work/opencluster/logs/%s-%s.log"

    if len(logging.getLogger().handlers) > 0 :
        return
    rollfdlr = logging.handlers.RotatingFileHandler(filename=logPathFmt%(name,id),mode='a', maxBytes=10*1024*1024,backupCount=1)
    consoledlr = logging.StreamHandler(sys.stdout)
    rollfdlr.setLevel(level)
    consoledlr.setLevel(level)

    formatter = logging.Formatter('[%(asctime)s](%(levelname)s)-%(filename)s:%(lineno)s: %(message)s')
    rollfdlr.setFormatter(formatter)
    consoledlr.setFormatter(formatter)
    logging.getLogger().addHandler(rollfdlr)
    logging.getLogger().addHandler(consoledlr)
    logger.setLevel(level)

class Conf(object):
    HEARTBEAT_LIVENESS = 3
    HEARTBEAT_INTERVAL = 10
    INTERVAL_INIT = 1
    INTERVAL_MAX = 32
    PPP_READY = "\x01"      # Signals worker is ready
    PPP_HEARTBEAT = "\x02"  # Signals worker heartbeat

    MEM_PER_TASK = 200.0
    if sys.platform == "win32":
        configRootPath = os.path.dirname(__file__)
    else:
        configRootPath = "/work/opencluster"
    cf = ConfigParser.ConfigParser()
    configFilePath = os.path.join(configRootPath,"config.ini")
    cf.read(configFilePath)

    def __init__(self):
        if not Conf.cf :
            Conf.cf = ConfigParser.ConfigParser()
            Conf.cf.read(Conf.configFilePath)
            
    @classmethod
    def setConfigFile(cls, filePath):
        cls.configFilePath = filePath
        cls.cf = ConfigParser.ConfigParser()
        cls.cf.read(Conf.configFilePath)

    @classmethod
    def getMesosMaster(cls):
        return cls.cf.get("factory", "mesos")

    @classmethod
    def getWareHouseAddr(cls):
        return cls.cf.get("factory", "warehouse")

    @classmethod
    def getFactoryServiceName(cls):
        return cls.cf.get("factory", "service")

    @classmethod
    def getFactoryServers(cls):
        return cls.cf.get("factory", "servers")

    @classmethod
    def getComputeMode(cls):
        return cls.cf.getint("factory", "computeMode")

    @classmethod
    def getAlwaysTryLeader(cls):
        return bool(cls.cf.get("factory", "alwaysTryLeader")=="true")

    @classmethod
    def getSafeMemoryPerNode(cls):
        return cls.cf.getint("factory", "safeMemoryPerNode")

    @classmethod
    def getHeartBeat(cls):
        return cls.cf.getint("factory", "heartbeat")

    @classmethod
    def getClearPeriod(cls):
        return cls.cf.getint("factory", "clearPeriod")

    @classmethod
    def getExpiration(cls):
        return cls.cf.getint("factory", "expiration")

    @classmethod
    def getStartWebapp(cls):
        return bool(cls.cf.get("factory", "startWebapp")=="True")

    @classmethod
    def getMaxDelay(cls):
        return cls.cf.getint("factory", "maxdelay")

    @classmethod
    def getWorkerTimeout(cls):
        return cls.cf.getint("worker", "timeout")

    @classmethod
    def getNodeDiskPath(cls):
        return cls.cf.get("node", "diskPath")

    @classmethod
    def getWebServers(cls):
        return cls.cf.get("webapp", "servers")

    @classmethod
    def getUri(cls, host, port, serviceName):
        return "PYRO:%s@%s:%s" % (serviceName, host, str(port))

    @classmethod
    def getWebStaticFullPath(cls):
        return cls.cf.get("webapp", "static_full_path")

    @classmethod
    def getWebTemplatesPath(cls):
        return cls.cf.get("webapp", "templates_path")

    @classmethod
    def getNodeDefaultPort(cls):
        return cls.cf.getint("node", "defaultPort")

    @classmethod
    def getNodePortForService(cls):
        return cls.cf.getint("node", "portForService")

    @classmethod
    def getNodeAvailWorkers(cls):
        return cls.cf.get("node", "availWorkers")

    @classmethod
    def getNodeAvailServices(cls):
        return cls.cf.get("node", "availServices")

    @classmethod
    def getNodeWorkerDir(cls):
        return cls.cf.get("node", "workerDir")

    @classmethod
    def getNodeServiceDir(cls):
        return cls.cf.get("node", "serviceDir")

    @classmethod
    def getLogRootDir(cls):
        return cls.cf.get("log", "logRootDir")

    @classmethod
    def getRootDir(cls):
        return cls.cf.get("node", "root")

    @classmethod
    def getShareFileDir(cls):
        return cls.cf.get("node", "share")

    @classmethod
    def getWorkDirs(cls):
        return cls.cf.get("node", "work_dirs")





