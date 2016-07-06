import os, sys
import socket
import logging
import marshal
import datetime
import cPickle
import threading, Queue
import time
import random
import multiprocessing
from Pyro4.errors import CommunicationError
import MySQLdb
import traceback

logger = logging.getLogger(__name__)
from opencluster.util import compress, decompress, mkdir_p, getuser, safe,int2ip,parse_mem,spawn
from opencluster.env import env
from opencluster.item import Task,Success,OtherFailure,TaskStateName,CompletionEvent

try :
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import mesos_pb2
except :
    print "warning no module named mesos.native or mesos.interface."

MAX_FAILED = 3
EXECUTOR_MEMORY = 64 # cache
POLL_TIMEOUT = 0.1
RESUBMIT_TIMEOUT = 60
MAX_IDLE_TIME = 30
SHUTDOWN_TIMEOUT = 3  # in seconds

def run_task(task, aid):
    logger.debug("Running task %s", task.id)
    try:
        result = task.run(aid)
        # find Worker instance by workerType
        return (task.id, Success(), compress(cPickle.dumps(result,-1)))
    except Exception, e:
        logger.error("error in task %s", task)
        import traceback
        traceback.print_exc()
        return (task.id, OtherFailure("exception:" + str(e)), None)

class Scheduler(object):
    def __init__(self, manager):
        self.completionEvents = Queue.Queue()
        self.shuttingdown = False
        self.stopped = False
        self.finished_count = 0
        self.fail_count = 0
        self.taskNum = 0
        self.started = False
        self.manager = manager

    def shutdown(self):
        self.shuttingdown = True

    def terminated(self):
        return self.stopped

    def submitTasks(self, tasks):
        if self.started:
            logger.error("Scheduler was started,submitting new tasks is not allowd")
            return

    def taskEnded(self, task, reason, result):
        if isinstance(reason,Success) :
            self.finished_count +=1
        else:
            self.fail_count +=1

        self.completionEvents.put(CompletionEvent(task, reason, result))
        self.manager.statusUpdate()

    def start(self): pass

    def defaultParallelism(self):
        return 2

class LocalScheduler(Scheduler):
    attemptId = 0
    def __init__(self, manager):
        Scheduler.__init__(self,manager)

    def nextAttempId(self):
        self.attemptId += 1
        return self.attemptId

    def submitTasks(self, tasks):
        #self.completionEvents.join()
        self.taskNum = self.taskNum + len(tasks)
        self.started = True

        self.manager.statusUpdate()

        logger.debug("submit %s tasks  in LocalScheduler", len(tasks))
        for task in tasks:
            _, reason, result = run_task(task, self.nextAttempId())
            self.taskEnded(task, reason, result)


def run_task_in_process(task, tid):
    logger.debug("run task in process %s %s", task, tid)
    try:
        return run_task(task, tid)
    except KeyboardInterrupt:
        sys.exit(0)

class MultiProcessScheduler(LocalScheduler):
    def __init__(self, manager,threads):
        LocalScheduler.__init__(self,manager)
        self.threads = threads
        self.tasks = {}
        self.pool = multiprocessing.Pool(self.threads or self.defaultParallelism())
        self.started = True

    def submitTasks(self, tasks):
        if not tasks:
            return

        if not self.started:
            logger.error("process pool stopped")
            return

        self.taskNum = self.taskNum + len(tasks)
        self.manager.statusUpdate()

        logger.info("Got a job with %d tasks", self.taskNum)

        start = time.time()
        def callback(args):
            tid, reason, result = args
            task = self.tasks.pop(tid)
            self.taskEnded(task, reason, result)

            logger.info("Task %s finished (%d/%d)", tid, self.finished_count, self.taskNum)

            if self.finished_count == self.taskNum:
                logger.info("%d tasks finished in %.1f seconds" + " "*20,  self.finished_count, time.time() - start)
        for task in tasks:
            self.tasks[task.id] = task
            self.pool.apply_async(run_task_in_process, [task, self.nextAttempId()], callback=callback)

    def shutdown(self):
        self.pool.terminate()
        self.pool.join()
        self.started = False
        logger.debug("process pool stopped")

    def defaultParallelism(self):
        return multiprocessing.cpu_count()


class StandaloneScheduler(Scheduler):
    def __init__(self, manager,workerType):
        Scheduler.__init__(self,manager)
        self.workerType = workerType
        self.slaveTasks = {}
        self.current_task_num = 0
        self.futureResults = {}
        self.shutdowned = False
        self.lock = threading.RLock()

        def futureResultCheck():
            while not self.shutdowned:
                finishedTaskIds = []
                for tid in self.futureResults :
                    logger.debug(self.futureResults[tid])
                    (v_tid,state,results) = self.futureResults[tid]
                    if results.ready and self.slaveTasks.has_key(tid) :
                        if state == Task.TASK_FINISHED :
                            try :
                                self.taskEnded(self.slaveTasks[tid], Success(), results.value)
                                logger.debug("Task %s finished  - (%d/%d)", tid, self.finished_count, self.taskNum)
                            except CommunicationError,ce:
                                #logger.error("CommunicationErrorCommunicationError")
                                continue
                            except Exception,e :
                                self.taskEnded(self.slaveTasks[tid], OtherFailure(e), None)
                                logger.debug("Task %s fail  - (%d/%d)", tid, self.fail_count, self.taskNum)
                                logger.error(traceback.print_exc())
                        if state == Task.TASK_ERROR :
                            self.taskEnded(self.slaveTasks[tid], results.value, None) # data is typeof OtherFailure
                            logger.debug("Task %s failed  - (%d/%d)", tid, self.fail_count, self.taskNum)
                        self.slaveTasks.pop(tid)
                        finishedTaskIds.append(tid)
                for f_tid in finishedTaskIds:
                    self.popFutureResults(f_tid)

                time.sleep(0.5)
                logger.info("%s,(%d/%d) tasks finished" + " "*20,  self.workerType,self.finished_count, self.taskNum)
        spawn(futureResultCheck)

    @safe
    def putFutureResults(self,tid,value):
        self.futureResults[tid] = value

    @safe
    def popFutureResults(self,tid):
        self.futureResults.pop(tid)


    def submitTasks(self, tasks):
        if not tasks:
            return

        self.started = True
        start = time.time()

        self.current_task_num = len(tasks)
        self.taskNum = self.taskNum + self.current_task_num
        self.manager.statusUpdate()

        logger.info("Got a job with %d tasks", self.taskNum)

        workers = self.manager.getWaitingWorkers(self.workerType, True)
        if len(workers) == 0 :
            logging.error("no %s worker found",self.workerType)
            return

        j = random.randint(0, len(workers))

        for task in tasks:
            if j == len(workers) :
                j = 0
            logger.debug("dispather task(%s) to %s" % (task.id,workers[j]))
            w = workers[j].doTask(task)
            self.slaveTasks[task.id]=task
            self.putFutureResults(task.id,w)
            j += 1

    def defaultParallelism(self):
        return multiprocessing.cpu_count()
    def shutdown(self):
         self.shutdowned = True

class FactoryScheduler(Scheduler):
    def __init__(self, manager, addr, warehouse):
        Scheduler.__init__(self,manager)
        self.addr = addr
        self.warehouse = warehouse
        self.fail_count = 0

    def submitTasks(self, tasks):
        if not tasks:
            return
        self.taskNum = self.taskNum + len(tasks)

        if len(self.addr.split(",")) > 2 :
            self.mysqlTasks(self.addr,self.warehouse,tasks)
        else:
            self.kafkaTasks(self.addr,self.warehouse,tasks)

        self.manager.statusUpdate()
        logger.info("(%d) tasks are waiting in warehouse" + " "*20,   self.taskNum)

    def mysqlTasks(self,connectStr,warehouse,tasks):
        urls = connectStr.split(",");
        mysqlIpAndPort = urls[0].split(":")
        db = None
        try :
            db = MySQLdb.connect(host=mysqlIpAndPort[0], port = int(mysqlIpAndPort[1]),db =urls[1], user=urls[2],passwd=urls[3])
            cur = db.cursor()

            count=cur.execute("select * from t_job where job_id='" + self.manager.name + "'")
            if count == 0:
                value=[self.manager.name,1,'factory']
                cur.execute('insert into t_job values(%s,%s,%s,now())',value)

            values=[]
            for task in tasks:
                values.append((task.id,cPickle.dumps(task),0,task.priority,self.manager.name))

            cur.executemany('insert into t_task (task_id,task_desc,status,priority,job_id) values(%s,%s,%s,%s,%s)',values)

            db.commit()
            cur.close()
        finally:
            if db :
                db.close()

    def kafkaTasks(self, addr, topic,tasks):
        try :
            from kafka import SimpleProducer, KafkaClient, KeyedProducer
        except:
            logger.error("kafka-python is not installed")
            raise Exception("kafka-python is not installed")
        kafka_client = None
        try :
            kafka_client = KafkaClient(addr)
            producer = KeyedProducer(kafka_client)

            for task in tasks:
                #self.producer.send_messages(self.warehouse,task.id, json.dumps(task,default=object2dict))
                producer.send_messages(topic, self.manager.name, cPickle.dumps(task))
        finally:
            if kafka_client:
                kafka_client.close()
    def shutdown(self):
        pass

def profile(f):
    def func(*args, **kwargs):
        path = '/tmp/worker-%s.prof' % os.getpid()
        import cProfile
        import pstats
        func = f
        cProfile.runctx('func(*args, **kwargs)',
            globals(), locals(), path)
        stats = pstats.Stats(path)
        stats.strip_dirs()
        stats.sort_stats('time', 'calls')
        stats.print_stats(20)
        stats.sort_stats('cumulative')
        stats.print_stats(20)
    return func

class MesosScheduler(Scheduler):
    def __init__(self, manager, master, options):
        Scheduler.__init__(self,manager)
        self.master = master
        self.cpus = options.cpus
        self.mem = parse_mem(options.mem)
        self.gpus = options.gpus
        self.task_per_node = options.parallel or multiprocessing.cpu_count()
        self.options = options
        self.group = options.group
        self.last_finish_time = 0
        self.executor = None
        self.driver = None
        self.lock = threading.RLock()
        self.task_waiting = []
        self.task_launched = {}
        self.slaveTasks = {}
        self.starting = False

    def start_driver(self):
        name = 'OpenCluster'
        if self.options.name :
            name = "%s-%s" % (name,self.options.name)
        else:
            name = "%s-%s" % (name,datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"))

        if len(name) > 256:
            name = name[:256] + '...'

        framework = mesos_pb2.FrameworkInfo()
        framework.user = getuser()
        if framework.user == 'root':
            raise Exception("OpenCluster is not allowed to run as 'root'")
        framework.name = name
        framework.hostname = socket.gethostname()

        self.driver = MesosSchedulerDriver(self, framework, self.master)
        self.driver.start()
        logger.debug("Mesos Scheudler driver started")

        self.shuttingdown = False
        self.last_finish_time = time.time()
        self.stopped = False
        #
        # def check():
        #     while self.started:
        #         now = time.time()
        #         if not self.task_waiting and now - self.last_finish_time > MAX_IDLE_TIME:
        #             logger.info("stop mesos scheduler after %d seconds idle", now - self.last_finish_time)
        #             self.shutdown()
        #             break
        #         time.sleep(1)
        #
        #         if len(self.task_success()) + len(self.task_failed) == self.taskNum:
        #             self.shutdown()
        # spawn(check)

    @safe
    def registered(self, driver, frameworkId, masterInfo):
        self.started = True
        logger.debug("connect to master %s:%s(%s), registered as %s",
            int2ip(masterInfo.ip), masterInfo.port, masterInfo.id,
            frameworkId.value)
        self.executor = self.getExecutorInfo(str(frameworkId.value))

    @safe
    def reregistered(self, driver, masterInfo):
        logger.warning("re-connect to mesos master %s:%s(%s)",
            int2ip(masterInfo.ip), masterInfo.port, masterInfo.id)

    @safe
    def disconnected(self, driver):
        logger.debug("framework is disconnected")

    @safe
    def getExecutorInfo(self, framework_id):
        execInfo = mesos_pb2.ExecutorInfo()
        execInfo.executor_id.value = "multiframework"
        execInfo.command.value = '%s %s' % (
            sys.executable, # /usr/bin/python.exe or .../python
            os.path.abspath(os.path.join(os.path.dirname(__file__), 'simpleexecutor.py'))
        )
        v = execInfo.command.environment.variables.add()
        v.name = 'UID'
        v.value = str(os.getuid())
        v = execInfo.command.environment.variables.add()
        v.name = 'GID'
        v.value = str(os.getgid())

        if hasattr(execInfo, 'framework_id'):
            execInfo.framework_id.value = str(framework_id)

        Script = os.path.realpath(sys.argv[0])
        if hasattr(execInfo, 'name'):
            execInfo.name = Script

        execInfo.data = marshal.dumps((Script, os.getcwd(), sys.path, dict(os.environ), self.task_per_node, env.environ))

        return execInfo
    @safe
    def clearCache(self):
        self.task_launched.clear()
        self.slaveTasks.clear()

    @safe
    def submitTasks(self, tasks):
        if not tasks:
            return
        self.completionEvents.join() #Blocks until all items in the events queue have been gotten and processed.
        self.clearCache()
        self.task_waiting.extend(tasks)
        self.taskNum = self.taskNum + len(tasks)
        logger.debug("Got job with %d tasks",  len(tasks))

        if not self.started and not self.starting:
            self.starting = True
            self.start_driver()
        while not self.started:
            self.lock.release()
            time.sleep(0.01)
            self.lock.acquire()

        self.requestMoreResources()
        self.manager.statusUpdate()

    def requestMoreResources(self):
        if self.started:
            self.driver.reviveOffers()

    @safe
    def resourceOffers(self, driver, offers):

        rf = mesos_pb2.Filters()
        if not self.task_waiting:
            rf.refuse_seconds = 5
            for o in offers:
                driver.launchTasks(o.id, [], rf)
            return

        random.shuffle(offers)
        self.last_offer_time = time.time()
        for offer in offers:
            if self.shuttingdown:
                print "Shutting down: declining offer on [%s]" % offer.hostname
                driver.declineOffer(offer.id)
                continue

            attrs = self.getAttributes(offer)
            if self.options.group and attrs.get('group', 'None') not in self.options.group:
                driver.launchTasks(offer.id, [], rf)
                continue

            cpus, mem, gpus = self.getResources(offer)
            logger.debug("got resource offer %s: cpus:%s, mem:%s, gpus:%s at %s", offer.id.value, cpus, mem, gpus, offer.hostname)
            logger.debug("attributes,gpus:%s",attrs.get('gpus', None))
            sid = offer.slave_id.value
            tasks = []
            while (len(self.task_waiting)>0 and cpus >= self.cpus and mem >= self.mem and (self.gpus==0 or attrs.get('gpus', None) is not None)):

                logger.debug("Accepting resource on slave %s (%s)", offer.slave_id.value, offer.hostname)
                t = self.task_waiting.pop()
                t.state = mesos_pb2.TASK_STARTING
                t.state_time = time.time()

                task = self.create_task(offer, t, cpus)
                tasks.append(task)

                self.task_launched[t.id] = t
                self.slaveTasks.setdefault(sid, set()).add(t.id)

                cpus -= self.cpus
                mem -= self.mem
                # gpus -= self.gpus

            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)
            driver.acceptOffers([offer.id], [operation])

    @safe
    def offerRescinded(self, driver, offer_id):
        logger.debug("rescinded offer: %s", offer_id)
        if self.task_waiting:
            self.requestMoreResources()

    def getResources(self, offer):
        cpus, mem, gpus = 0, 0, 0
        for r in offer.resources:
            if r.name == 'gpus':
                gpus = float(r.scalar.value)
            elif r.name == 'cpus':
                cpus = float(r.scalar.value)
            elif r.name == 'mem':
                mem = float(r.scalar.value)
        return cpus, mem, gpus

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0

    def getAttribute(self, attrs, name):
        for r in attrs:
            if r.name == name:
                return r.scalar.value

    def getAttributes(self, offer):
        attrs = {}
        for a in offer.attributes:
            attrs[a.name] = a.scalar.value
        return attrs

    def create_task(self, offer, t, cpus):
        task = mesos_pb2.TaskInfo()

        task.task_id.value = t.id
        task.slave_id.value = offer.slave_id.value
        task.name = "task(%s/%d)" % (t.id, self.taskNum)
        task.executor.MergeFrom(self.executor)

        task.data = compress(cPickle.dumps((t, t.tried), -1))

        cpu = task.resources.add()
        cpu.name = "cpus"
        cpu.type = 0 # mesos_pb2.Value.SCALAR
        cpu.scalar.value = min(self.cpus, cpus)

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = 0 # mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem
        #
        # gpu = task.resources.add()
        # gpu.name = "gpus"
        # gpu.type = 0 # mesos_pb2.Value.SCALAR
        # gpu.scalar.value = self.gpus

        return task

    @safe
    def statusUpdate(self, driver, update):
        logger.debug("Task %s in state [%s]" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state)))
        tid = str(update.task_id.value)

        if tid not in self.task_launched:
            # check failed after launched
            for t in self.task_waiting:
                if t.id == tid:
                    self.task_launched[tid] = t
                    self.task_waiting.remove(t)
                    break
            else:
                logger.debug("Task %s is finished, ignore it", tid)
                return

        t = self.task_launched[tid]
        t.state = update.state
        t.state_time = time.time()
        self.last_finish_time = t.state_time

        if update.state == mesos_pb2.TASK_RUNNING:
            self.started = True
            # to do task timeout handler
        elif update.state == mesos_pb2.TASK_LOST:
            self.task_launched.pop(tid)

            if t.tried < self.options.retry:
                t.tried += 1
                logger.warning("task %s lost, retry %s", t.id, update.state, t.tried)
                self.task_waiting.append(t) # try again
            else:
                self.taskEnded(t, OtherFailure("task lost,exception:" + str(update.data)), "task lost")

        elif update.state in (mesos_pb2.TASK_FINISHED, mesos_pb2.TASK_FAILED, mesos_pb2.TASK_ERROR, mesos_pb2.TASK_KILLED):
            self.task_launched.pop(tid)

            slave = None
            for s in self.slaveTasks:
                if tid in self.slaveTasks[s]:
                    slave = s
                    self.slaveTasks[s].remove(tid)
                    break

            if update.state == mesos_pb2.TASK_FINISHED :
                self.taskEnded(t, Success(), update.data)

            if update.state == mesos_pb2.TASK_ERROR :
                logger.error(update.message)
                self.taskEnded(t, OtherFailure(update.message), update.message)
                driver.abort()
                self.shutdown()

            if update.state == mesos_pb2.TASK_FAILED or update.state == mesos_pb2.TASK_KILLED or update.state == mesos_pb2.TASK_LOST:
                if t.tried < self.options.retry:
                    t.tried += 1
                    logger.warning("task %s failed with %s, retry %s", t.id, update.state, t.tried)
                    self.task_waiting.append(t) # try again
                else:
                    self.taskEnded(t, OtherFailure("exception:" + str(update.data)), None)
                    logger.error("task %s failed on %s", t.id, slave)

        if not self.task_waiting:
            self.requestMoreResources() # request more offers again

    @safe
    def check(self, driver):
        now = time.time()
        for tid, t in self.task_launched.items():
            if t.state == mesos_pb2.TASK_STARTING and t.state_time + 30 < now:
                logger.warning("task %s lauched failed, assign again", tid)
                if not self.task_waiting:
                    self.requestMoreResources()
                t.tried += 1
                t.state = -1
                self.task_launched.pop(tid)
                self.task_waiting.append(t)
            # TODO: check run time

    @safe
    def shutdown(self):
        if not self.started:
            return

        wait_started = datetime.datetime.now()
        while (len(self.task_launched) > 0) and \
          (SHUTDOWN_TIMEOUT > (datetime.datetime.now() - wait_started).seconds):
            time.sleep(1)

        logger.debug("total:%d, task finished: %d,task failed: %d", self.taskNum, self.finished_count, self.fail_count)

        self.shuttingdown = True
        # self.driver.join()
        self.driver.stop(False)

        #self.driver = None
        logger.debug("scheduler stop!!!")
        self.stopped = True
        self.started = False

    @safe
    def error(self, driver, code):
        logger.warning("Mesos error message: %s", code)

    def defaultParallelism(self):
        return 16

    def frameworkMessage(self, driver, executor, slave, data):
        logger.warning("[slave %s] %s", slave.value, data)

    def executorLost(self, driver, executorId, slaveId, status):
        logger.warning("executor at %s %s lost: %s", slaveId.value, executorId.value, status)
        self.slaveTasks.pop(slaveId.value, None)

    def slaveLost(self, driver, slaveId):
        logger.warning("slave %s lost", slaveId.value)
        self.slaveTasks.pop(slaveId.value, None)

    def killTask(self, job_id, task_id, tried):
        tid = mesos_pb2.TaskID()
        tid.value = "%s:%s:%s" % (job_id, task_id, tried)
        self.driver.killTask(tid)

