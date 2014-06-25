import ConfigParser
import logging
import logging.config
import os

class Conf(object):
    cf = ConfigParser.ConfigParser()
    configFilePath = os.getcwd() + "/config.ini"
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
    def getParkServiceName(cls):
        return cls.cf.get("park", "service")

    @classmethod
    def getParkServers(cls):
        return cls.cf.get("park", "servers")

    @classmethod
    def getComputeMode(cls):
        return cls.cf.getint("park", "computeMode")

    @classmethod
    def getAlwaysTryLeader(cls):
        return cls.cf.get("park", "alwaysTryLeader")

    @classmethod
    def getSafeMemoryPerNode(cls):
        return cls.cf.getint("park", "safeMemoryPerNode")

    @classmethod
    def getHeartBeat(cls):
        return cls.cf.getint("park", "heartbeat")

    @classmethod
    def getClearPeriod(cls):
        return cls.cf.getint("park", "clearPeriod")

    @classmethod
    def getExpiration(cls):
        return cls.cf.getint("park", "expiration")

    @classmethod
    def getStartWebapp(cls):
        return bool(cls.cf.get("park", "startWebapp"))

    @classmethod
    def getMaxDelay(cls):
        return cls.cf.getint("park", "maxdelay")
        
    @classmethod
    def getWorkerServiceFlag(cls):
        return cls.cf.getint("worker", "service")

    @classmethod
    def getWorkerServers(cls):
        return cls.cf.get("worker", "servers")

    @classmethod
    def getWorkerTimeout(cls):
        return cls.cf.getint("worker", "timeout")

    @classmethod
    def getCtorServers(cls):
        return cls.cf.get("ctor", "servers")

    @classmethod
    def getWebServers(cls):
        return cls.cf.get("webapp", "servers")

    @classmethod
    def getCtorInitServices(cls):
        return cls.cf.getint("ctor", "initServices")

    @classmethod
    def getCtorMaxServices(cls):
        return cls.cf.getint("ctor", "maxServices")

    @classmethod
    def getUri(cls, host, port, serviceName):
        return "PYRO:%s@%s:%s" % (serviceName, host, str(port))

logging.config.fileConfig("logging.conf")



