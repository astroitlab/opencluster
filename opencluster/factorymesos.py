#!/usr/bin/env python
import os
import sys
import time
import datetime
import socket
import random
from optparse import OptionParser
import threading
import logging
import signal
import marshal
import pickle
import MySQLdb

import Pyro4
from opencluster.configuration import Conf, setLogger
from opencluster.env import env
from opencluster.item import TaskStateName
from opencluster.util import getuser, safe, decompress, spawn,compress,mkdir_p

from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
from mesos.interface import Executor, Scheduler, mesos_pb2
from kafka import KafkaConsumer, KafkaClient, SimpleProducer

logger = logging.getLogger(__name__)

EXECUTOR_MEMORY = 64 # cache
MAX_WAITING_TASK = 1000
MAX_EMPTY_TASK_PERIOD = 1

pyroLoopCondition = True

def checkPyroLoopCondition() :
    global pyroLoopCondition
    return pyroLoopCondition

class FactoryMesos(object):

    def __init__(self, options, command,implicitAcknowledgements):
        self.framework_id = None
        self.executor = None
        self.implicitAcknowledgements = implicitAcknowledgements

        self.framework = mesos_pb2.FrameworkInfo()
        self.framework.user = getuser()
        # if self.framework.user == 'root':
        #     raise Exception("drun is not allowed to run as 'root'")
        name = 'OpenCluster-Factory'
        if len(name) > 256:
            name = name[:256] + '...'
        self.framework.name = name
        self.framework.hostname = socket.gethostname()

        self.options = options
        self.command = command
        self.tasks_waiting = {}
        self.task_launched = {}
        self.slaveTasks = {}
        self.stopped = False
        self.status = 0
        self.last_offer_time = time.time()
        self.lock = threading.RLock()
        self.priority_size = 4

        self.warehouse_addrs = self.options.warehouse_addr.split(",")
        self.outputdb = None
        self.kafka_client = None
        self.producer = None

        if len(self.warehouse_addrs) > 2:
            mysqlIpAndPort = self.warehouse_addrs[0].split(":")
            self.outputdb = MySQLdb.connect(host=mysqlIpAndPort[0], port = int(mysqlIpAndPort[1]), db =self.warehouse_addrs[1],user=self.warehouse_addrs[2],passwd=self.warehouse_addrs[3])
        else:
            self.kafka_client = KafkaClient(self.options.warehouse_addr)
            self.producer = SimpleProducer(self.kafka_client)
        for i in range(1,self.priority_size+1):
            self.tasks_waiting[i] = []

    def getExecutorInfo(self):
        execInfo = mesos_pb2.ExecutorInfo()
        execInfo.executor_id.value = "default"
        execInfo.command.value = '%s %s' % (
            sys.executable, # .../python.exe
            os.path.abspath(os.path.join(os.path.dirname(__file__), 'simpleexecutor.py'))
        )
        v = execInfo.command.environment.variables.add()
        v.name = 'UID'
        v.value = str(os.getuid())
        v = execInfo.command.environment.variables.add()
        v.name = 'GID'
        v.value = str(os.getgid())

        if hasattr(execInfo, 'framework_id'):
            execInfo.framework_id.value = str(self.framework_id)


        if self.options.image and hasattr(execInfo, 'container'):
            logger.debug("container will be run in Docker")
            execInfo.container.type = mesos_pb2.ContainerInfo.DOCKER
            execInfo.container.docker.image = self.options.image

            execInfo.command.value = '%s %s' % (
                sys.executable, # /usr/bin/python.exe or .../python
                os.path.abspath(os.path.join(os.path.dirname(__file__), 'simpleexecutor.py'))
            )
            for path in ['/etc/passwd', '/etc/group']:
                v = execInfo.container.volumes.add()
                v.host_path = v.container_path = path
                v.mode = mesos_pb2.Volume.RO

            if self.options.volumes:
                for volume in self.options.volumes.split(','):
                    fields = volume.split(':')
                    if len(fields) == 3:
                        host_path, container_path, mode = fields
                        mode = mesos_pb2.Volume.RO if mode.lower() == 'ro' else mesos_pb2.Volume.RW
                    elif len(fields) == 2:
                        host_path, container_path = fields
                        mode = mesos_pb2.Volume.RW
                    elif len(fields) == 1:
                        container_path, = fields
                        host_path = ''
                        mode = mesos_pb2.Volume.RW
                    else:
                        raise Exception("cannot parse volume %s", volume)

                    mkdir_p(host_path)

                    v = execInfo.container.volumes.add()
                    v.container_path = container_path
                    v.mode = mode
                    if host_path:
                        v.host_path = host_path


        Script = os.path.realpath(sys.argv[0])
        if hasattr(execInfo, 'name'):
            execInfo.name = Script
        execInfo.data = marshal.dumps((Script, os.getcwd(), sys.path, dict(os.environ), self.options.task_per_node, env.environ))
        return execInfo

    def registered(self, driver, fid, masterInfo):
        logger.debug("Registered with Mesos, FID = %s" % fid.value)
        self.framework_id = fid.value
        self.executor = self.getExecutorInfo()

    def getResources(self, offer):
        gpus,cpus, mem = 0, 0, 0
        for r in offer.resources:
            if r.name == 'gpus':
                gpus += float(r.scalar.value)
            elif r.name == 'cpus':
                cpus += float(r.scalar.value)
            elif r.name == 'mem':
                mem += float(r.scalar.value)
        return cpus, mem, gpus

    def getAttributes(self, offer):
        attrs = {}
        for a in offer.attributes:
            attrs[a.name] = a.scalar.value
        return attrs

    @safe
    def append_task(self,t):
        self.tasks_waiting[t.priority].append(t)

    @safe
    def tasks_len(self,priority):
        return len(self.tasks_waiting[priority])

    @safe
    def tasks_total_len(self):
        return sum(len(v) for (k,v) in self.tasks_waiting.items())

    @safe
    def get_task_of_waiting_by_id(self,tid):
        for (k,v) in self.tasks_waiting.items():
            for t in v:
                if t.id == tid:
                    return t
        return None

    @safe
    def pop_task(self,priority):
        return self.tasks_waiting[priority].pop()

    @safe
    def remove_task(self,t):
        return self.tasks_waiting[t.priority].remove(t)

    @safe
    def waiting_tasks(self,priority):
        return self.tasks_waiting[priority]

    @safe
    def resourceOffers(self, driver, offers):
        rf = mesos_pb2.Filters()
        rf.refuse_seconds = 60
        if self.tasks_total_len()==0:
            for o in offers:
                driver.launchTasks(o.id, [], rf)
            return

        random.shuffle(offers)
        self.last_offer_time = time.time()

        for offer in offers:
            attrs = self.getAttributes(offer)

            if self.options.group and attrs.get('group', 'None') not in self.options.group:
                driver.launchTasks(offer.id, [], rf)
                continue

            cpus, mem, gpus = self.getResources(offer)

            logger.debug("got resource offer %s: cpus:%s, mem:%s, gpus:%s at %s", offer.id.value, cpus, mem, gpus, offer.hostname)
            sid = offer.slave_id.value
            tasks = []

            for priority in range(1,self.priority_size+1):
                for t in self.waiting_tasks(priority):
                    if priority > 1 and cpus <=1 :
                        break
                    if cpus>=t.resources.get("cpus",1) and mem>= t.resources.get("mem",100) and (t.resources.get("gpus",0)==0 or attrs.get('gpus', None) is not None) :
                        self.remove_task(t)
                        t.state = mesos_pb2.TASK_STARTING
                        t.state_time = time.time()

                        task = self.create_task(offer, t)
                        tasks.append(task)
                        self.task_launched[t.id] = t
                        self.slaveTasks.setdefault(sid, set()).add(t.id)
                        cpus -= t.resources.get("cpus",1)
                        mem -= t.resources.get("mem",100)

            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)
            driver.acceptOffers([offer.id], [operation])
            if len(tasks) > 0:
                logger.debug("dispatch %d tasks to slave %s", len(tasks), offer.hostname)

    def create_task(self, offer, t):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(t.id)
        task.slave_id.value = offer.slave_id.value
        task.name = "%s-%s" % (t.id, t.tried)
        task.executor.MergeFrom(self.executor)

        task.data = compress(pickle.dumps((t, t.tried), -1))

        cpu = task.resources.add()
        cpu.name = "cpus"
        cpu.type = 0 # mesos_pb2.Value.SCALAR
        cpu.scalar.value = t.resources.get("cpus",1)

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = 0 # mesos_pb2.Value.SCALAR
        mem.scalar.value = t.resources.get("mem",100)

        # gpu = task.resources.add()
        # gpu.name = "gpus"
        # gpu.type = 0 # mesos_pb2.Value.SCALAR
        # gpu.scalar.value = t.resources.get("gpus",1)

        return task

    def statusUpdate(self, driver, update):
        logger.debug("Task %s in state [%s]" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state)))
        tid = update.task_id.value
        if tid not in self.task_launched:
            t = self.get_task_of_waiting_by_id(tid)
            if t:
                self.task_launched[tid] = t
                self.remove_task(t)
            else:
                logger.debug("Task %s is finished, ignore it", tid)
                return

        t = self.task_launched[tid]
        t.state = update.state
        t.state_time = time.time()

        if update.state == mesos_pb2.TASK_RUNNING:
            #self.task_update(t,update.state,'')
            pass

        elif update.state in (mesos_pb2.TASK_FINISHED, mesos_pb2.TASK_FAILED, mesos_pb2.TASK_LOST):
            t = self.task_launched.pop(tid)
            slave = None
            for s in self.slaveTasks:
                if tid in self.slaveTasks[s]:
                    slave = s
                    self.slaveTasks[s].remove(tid)
                    break

            if update.state == mesos_pb2.TASK_FINISHED:
                self.task_update(t,update.state,update.data)

            if update.state >= mesos_pb2.TASK_FAILED:
                if t.tried < self.options.retry:
                    t.tried += 1
                    logger.warning("task %s failed with %s, retry %s", t.id, TaskStateName[int(update.state)], t.tried)
                    self.append_task(t) # try again
                else:
                    logger.error("task %s failed with %s on %s", t.id, update.state, slave)
                    self.task_update(t,update.state,str(update.data))

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def task_update(self,t,status,result):
        if status!=mesos_pb2.TASK_FINISHED:
            t.result = Exception(str(result))
        else:
            t.result = pickle.loads(decompress(result))

        if len(self.warehouse_addrs) > 2:
            cur = self.outputdb.cursor()
            value=[status,result,t.id]
            cur.execute("update t_task set status=%s,result=%s,task_end_time=now() where task_id=%s",value)
            self.outputdb.commit()
            cur.close()
        else:
            try :
                self.producer.send_messages("%sResult"%(t.warehouse), pickle.dumps(t))
            except Exception as e:
                logger.error(e)

    @safe
    def check(self, driver):
        now = time.time()
        #logger.debug("tasks_waiting:%d,task_launched:%d"%(len(self.tasks_waiting),len(self.task_launched.items())))
        for tid, t in self.task_launched.items():
            if t.state == mesos_pb2.TASK_STARTING and t.state_time + 60*60 < now:
                logger.warning("task %s lauched failed, assign again", tid)
                t.tried += 1
                self.task_launched.pop(tid)
                if t.tried <= self.options.retry :
                    t.state = -1
                    self.append_task(t)
                else:
                    self.task_update(t,mesos_pb2.TASK_FAILED,"run timeout")
            # TODO: check run time
        if self.tasks_total_len()>0:
            driver.reviveOffers() # request more offers again


    def offerRescinded(self, driver, offer):
        logger.debug("resource rescinded: %s", offer)
        # task will retry by checking

    def slaveLost(self, driver, slave):
        logger.warning("slave %s lost", slave.value)

    @safe
    def error(self,driver,code):
        logger.error("Error from Mesos: %s" % (code))

    @safe
    def stop(self, status):
        if self.stopped:
            return
        self.stopped = True
        self.status = status

        if self.outputdb:
            self.outputdb.close()
        if self.kafka_client :
            self.kafka_client.close()

        logger.debug("scheduler stopped")


def start_factory_mesos():
    global pyroLoopCondition
    parser = OptionParser(usage="Usage: python factorymesos.py [options] <command>")
    parser.allow_interspersed_args=False
    parser.add_option("-s", "--master", type="string", default="", help="url of master (mesos://172.31.252.180:5050)")
    parser.add_option("-f", "--factory", type="string", default="", help="host:port of master (172.31.252.180:6666)")
    parser.add_option("-w", "--warehouse_addr", type="string", default="", help="kafka-172.31.252.182:9092|mysql-172.31.254.25:3306,db,username,password")
    parser.add_option("-p", "--task_per_node", type="int", default=0, help="max number of tasks on one node (default: 0)")
    parser.add_option("-I", "--image", type="string",  help="image name for Docker")
    parser.add_option("-V", "--volumes", type="string",  help="volumes to mount into Docker")
    parser.add_option("-r","--retry", type="int", default=0, help="retry times when failed (default: 0)")
    parser.add_option("-e", "--config", type="string", default="/work/opencluster/config.ini", help="absolute path of configuration file(default:/work/opencluster/config.ini)")

    parser.add_option("-g","--group", type="string", default='', help="which group to run (default: ''")
    parser.add_option("-q", "--quiet", action="store_true", help="be quiet", )
    parser.add_option("-v", "--verbose", action="store_true", help="show more useful log", )

    (options, command) = parser.parse_args()

    if not options:
        parser.print_help()
        sys.exit(2)

    if options.config:
        Conf.setConfigFile(options.config)

    options.master = options.master or Conf.getMesosMaster()
    options.warehouse_addr = options.warehouse_addr or Conf.getWareHouseAddr()

    servers = options.factory or Conf.getFactoryServers()
    servs = servers.split(",")
    server = servs[0].split(":")

    options.logLevel = (options.quiet and logging.ERROR or options.verbose and logging.DEBUG or logging.INFO)
    setLogger(Conf.getFactoryServiceName(), "MESOS",options.logLevel)


    implicitAcknowledgements = 1
    if os.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS"):
        implicitAcknowledgements = 0
    sched = FactoryMesos(options, command, implicitAcknowledgements)

    driver = MesosSchedulerDriver(sched, sched.framework, options.master,implicitAcknowledgements)
    driver.start()
    logger.debug("Mesos Scheudler driver started")

    warehouse_addrs = options.warehouse_addr.split(",")

    def fetchTasksFromMySQL():
        global pyroLoopCondition
        mysqlIpAndPort = warehouse_addrs[0].split(":")
        last_data_time = time.time()

        while pyroLoopCondition:
            db = MySQLdb.connect(host=mysqlIpAndPort[0], port = int(mysqlIpAndPort[1]), db =warehouse_addrs[1],user=warehouse_addrs[2],passwd=warehouse_addrs[3])
            try :
                cur = db.cursor()
                curUpt = db.cursor()
                dataResults = cur.execute("select task_id,task_desc,task_start_time,status from t_task where status=0 order by priority asc limit 200")
                results = cur.fetchmany(dataResults)
                for r in results :
                    sched.append_task(pickle.loads(r[1]))
                    curUpt.execute("update t_task set task_start_time=now(),status=1 where task_id='" + r[0] + "'")
                if len(results) > 0:
                    db.commit()
                    last_data_time = time.time()
                    driver.reviveOffers()

                if sched.tasks_total_len() > MAX_WAITING_TASK :
                    time.sleep(2)
                if time.time() - last_data_time > MAX_EMPTY_TASK_PERIOD:
                    time.sleep(10)

                if cur:
                    cur.close()
                if curUpt:
                    curUpt.close()
            finally:
                db.close()

    def fetchTasksFromKafka(priority):
        global pyroLoopCondition

        consumer = KafkaConsumer('OpenCluster%s'%priority,
                               bootstrap_servers=[options.warehouse_addr],
                               group_id="cnlab",
                               auto_commit_enable=True,
                               auto_commit_interval_ms=30 * 1000,
                               auto_offset_reset='smallest')

        last_data_time = time.time()
        while pyroLoopCondition:
            for message in consumer.fetch_messages():
                logger.error("%s:%s:%s: key=%s " % (message.topic, message.partition,
                                             message.offset, message.key))
                sched.append_task(pickle.loads(message.value))
                consumer.task_done(message)
                last_data_time = time.time()
            if sched.tasks_len(priority) > MAX_WAITING_TASK :
                time.sleep(2)
            if time.time() - last_data_time > MAX_EMPTY_TASK_PERIOD:
                time.sleep(10)

    if len(warehouse_addrs) > 2:
        spawn(fetchTasksFromMySQL)
    else:
        for i in range(1,sched.priority_size+1):
            spawn(fetchTasksFromKafka, i)

    def handler(signm, frame):
        logger.warning("got signal %d, exit now", signm)
        sched.stop(3)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGABRT, handler)

    try:
        while not sched.stopped:
            time.sleep(0.5)
            sched.check(driver)

            now = time.time()
            if now > sched.last_offer_time + 60 + random.randint(0,5):
                logger.warning("too long to get offer, reviving...")
                sched.last_offer_time = now
                driver.reviveOffers()

    except KeyboardInterrupt:
        logger.warning('stopped by KeyboardInterrupt. The Program is exiting gracefully! Please wait...')
        sched.stop(4)

    #terminate pyrothread
    pyroLoopCondition = False

    time.sleep(5)
    driver.stop(False)
    sys.exit(sched.status)

if __name__ == "__main__":
    start_factory_mesos()