import sys
import os
import logging
import optparse
import Queue
import datetime
import time
import pickle
import MySQLdb

logger = logging.getLogger(__name__)
from opencluster.factory import FactoryContext
from opencluster.factorypatternexector import FactoryPatternExector
from opencluster.configuration import Conf, setLogger
from opencluster.schedule import LocalScheduler, MultiProcessScheduler, StandaloneScheduler, FactoryScheduler, \
    MesosScheduler
from opencluster.util import random_time_str
from opencluster.item import ManagerOption, CompletionEvent, Success, OtherFailure

parser = optparse.OptionParser(usage="Usage: python %prog [options]")


def add_default_options():
    parser.disable_interspersed_args()
    parser.add_option("-m", "--mode", type="string", default="local", help="local, process, standalone, mesos, factory")
    parser.add_option("-n", "--name", type="string", default="", help="job name")
    parser.add_option("-W", "--workertype", type="string", default="",
                      help="valid worker type registered in factory(specify when mode is standalone)")
    parser.add_option("-w", "--warehouse", type="string", default="",
                      help="warehouse location in factory(specify when mode is factory)")
    parser.add_option("-p", "--parallel", type="int", default=0, help="number of processes")

    parser.add_option("-r", "--retry", type="int", default=0, help="retry times when failed (default: 0)")
    parser.add_option("-c", "--cpus", type="float", default=1.0, help="cpus used per task")
    parser.add_option("-G", "--gpus", type="float", default=0, help="gpus used per task")
    parser.add_option("-M", "--mem", type="float", help="memory used per task")
    parser.add_option("-g", "--group", type="string", default="", help="which group of machines")
    parser.add_option("-e", "--config", type="string", default="/work/opencluster/config.ini",
                      help="path for configuration file")
    parser.add_option("-q", "--quiet", action="store_true", help="be quiet", )
    parser.add_option("-v", "--verbose", action="store_true", help="show more useful log", )


add_default_options()


def parse_options():
    (options, args) = parser.parse_args()
    if not options:
        parser.print_help()
        sys.exit(2)

    if options.mem is None:
        options.mem = Conf.MEM_PER_TASK

    options.logLevel = (options.quiet and logging.ERROR or options.verbose and logging.DEBUG or logging.INFO)
    setLogger(__name__, options.name, options.logLevel)

    if options.config:
        if os.path.exists(options.config) and os.path.isfile(options.config):
            Conf.setConfigFile(options.config)
        else:
            logger.error("configuration file is not found. (%s)" %(options.config,))
            sys.exit(2)

    return options


class ManagerStatus(object):
    def __init__(self, name):
        self.name = name
        self.mode = ""
        self.timeout = 3600 * 24 * 10  # 10 days
        self.totalNum = 0
        self.finished_count = 0
        self.fail_count = 0
        self.stopped = False
        self.started = False
        self.startTime = datetime.datetime.now()
        self.endTime = None


class Manager(object):
    def __init__(self, mode=None, name=None):
        self.mode = mode
        self.manager = None
        self.options = ManagerOption()
        self.name = name

    def initialize(self):
        if not self.name:
            self.name = "oc" + random_time_str()

        self.status = ManagerStatus(self.name)
        self.status.mode = self.mode

        if self.mode == 'standalone' and not self.options.workertype:
            logger.error("when --mode is standalone, --workertype must be specified")
            sys.exit(2)

        if self.mode == 'factory' and not self.options.warehouse:
            logger.error("when --mode is factory, --warehouse must be specified")
            sys.exit(2)

        if self.mode == 'local':
            self.scheduler = LocalScheduler(self)
            self.isLocal = True

        elif self.mode == 'process':
            self.scheduler = MultiProcessScheduler(self, self.options.parallel)
            self.isLocal = False

        elif self.mode == 'standalone':
            self.scheduler = StandaloneScheduler(self, self.options.workertype)
            self.isLocal = False

        elif self.mode == 'factory':
            self.scheduler = FactoryScheduler(self, Conf.getWareHouseAddr(), self.options.warehouse)
            self.isLocal = False

        elif self.mode == 'mesos':
            master = Conf.getMesosMaster()
            self.scheduler = MesosScheduler(self, master, self.options)
            self.isLocal = False

        else:
            logger.error("error mode, --mode should be one of [local, process, standalone, factory, mesos]")
            sys.exit(1)

        if self.options.parallel:
            self.defaultParallelism = self.options.parallel
        else:
            self.defaultParallelism = self.scheduler.defaultParallelism()

        self.initialized = True

    def init_cmd_option(self):
        self.options = parse_options()
        self.mode = self.mode or self.options.mode
        self.name = self.name or self.options.name

        self.initialize()

    def setOption(self, option):
        self.options = option

    def toNext(self, manager):
        self.manager = manager
        return manager

    def schedule(self, tasks):
        logger.debug(FactoryPatternExector.createManager(self.status))
        return self.giveTask(tasks)

    def statusUpdate(self):
        self.status.totalNum = self.scheduler.taskNum
        self.status.finished_count = self.scheduler.finished_count
        self.status.fail_count = self.scheduler.fail_count

        self.status.stopped = self.scheduler.stopped
        self.status.started = self.scheduler.started
        self.status.endTime = datetime.datetime.now()
        FactoryPatternExector.updateManager(self.status)

    def giveTask(self, tasks):
        return self.scheduler.submitTasks(tasks)

    def terminated(self):
        return self.scheduler.terminated()

    def shutdown(self):
        self.scheduler.shutdown()
        self.status.stopped = True
        self.status.endTime = datetime.datetime.now()
        FactoryPatternExector.updateManager(self.status)

    def completionEvents(self):
        if self.mode != "factory":
            while True:
                try:
                    yield self.scheduler.completionEvents.get_nowait()
                    self.scheduler.completionEvents.task_done()
                except Queue.Empty:
                    if self.status.totalNum == self.status.finished_count + self.status.fail_count:
                        break
        if self.mode == "factory":
            raise Exception("please consume results from warehouse [%s,%s]!"%(Conf.getWareHouseAddr(),self.options.warehouse))

    def getWaitingWorkers(self, workerType, asynchronous=False, worker=None):

        logger.info("getWaitingWorkers:%s" % workerType)

        wslist = self.getWorkerList(workerType)
        wklist = []

        for wsinfo in wslist:
            wklist.append(FactoryContext.getWorker(wsinfo[0], int(wsinfo[1]), wsinfo[2], asynchronous))

        return wklist

    def getWaitingServices(self, serviceName, asynchronous=False, worker=None):
        logger.info("getService:%s" % serviceName)
        wslist = self.getServiceList(serviceName)
        wklist = []

        for wsinfo in wslist:
            wklist.append(FactoryContext.getWorker(wsinfo[0], int(wsinfo[1]), wsinfo[2], asynchronous))

        return wklist

    def getWorkerList(self, workerType, host=None, port=None):
        obList = FactoryPatternExector.getWorkerTypeList(workerType);
        wsList = []
        if not obList is None:
            for obj in obList:
                hostPort = str(obj.obj).split(":")
                if hostPort[0] != host or int(hostPort[1]) != port:
                    wsList.append([hostPort[0], hostPort[1], workerType])
        return wsList

    def getServiceList(self, serviceName, host=None, port=None):
        obList = FactoryPatternExector.getServiceNameList(serviceName);
        wsList = []
        if not obList is None:
            for obj in obList:
                hostPort = str(obj.obj).split(":")
                if hostPort[0] != host or int(hostPort[1]) != port:
                    wsList.append([hostPort[0], hostPort[1], serviceName])
        return wsList
