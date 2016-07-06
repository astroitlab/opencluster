import logging
import os
import sys
import signal
import os.path
import marshal
import cPickle
import multiprocessing
import threading
import shutil
import socket
import gc
import time

try:
    from mesos.interface import Executor, mesos_pb2
    from mesos.native import MesosExecutorDriver
except ImportError:
    pass
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from opencluster.util import compress, decompress,safe
from opencluster.configuration import setLogger

logger = logging.getLogger(__file__)

MAX_WORKER_IDLE_TIME = 60
MAX_EXECUTOR_IDLE_TIME = 5
Script = ''
CUDA_BIN_PATH="/usr/local/cuda-7.5/bin"


def reply_status(driver, task_id, state, data=None):
    status = mesos_pb2.TaskStatus()
    status.task_id.MergeFrom(task_id)
    status.state = state
    status.timestamp = time.time()
    if data is not None:
        status.data = data
    driver.sendStatusUpdate(status)

def run_task(task_data):
    try:
        (task, ntry) = cPickle.loads(decompress(task_data))
        logger.debug(task)

        result = task.run(ntry)
        return mesos_pb2.TASK_FINISHED, compress(cPickle.dumps(result, -1))
    except Exception, e:
        logger.error(e)
        import traceback
        msg = traceback.format_exc()
        return mesos_pb2.TASK_FAILED, msg


class SimpleWorkerExecutor(Executor):
    def __init__(self):
        self.workdir = []
        self.idle_workers = []
        self.busy_workers = {}
        self.lock = threading.RLock()

    @safe
    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        try:
            global Script
            Script, cwd, python_path, osenv, self.parallel, args = marshal.loads(executorInfo.data)
            if os.path.exists(CUDA_BIN_PATH) :
                sys.path.append(CUDA_BIN_PATH)
                # os.environ["PATH"] = os.environ["PATH"] + ":/usr/local/cuda-7.5/bin";
            self.init_args = args
            logger.debug(os.environ)
            if os.path.exists(cwd):
                try:
                    os.chdir(cwd)
                except Exception, e:
                    logger.warning("change cwd to %s failed: %s", cwd, e)
            else:
                logger.warning("cwd (%s) not exists", cwd)

            if args.has_key('WORKDIR') :
                self.workdir = args['WORKDIR']
                logger.debug("args.has_key('WORKDIR'): %s", self.workdir)

            logger.debug("executor started at %s", slaveInfo.hostname)

        except Exception, e:
            import traceback
            msg = traceback.format_exc()
            logger.error("init executor failed: %s", msg)
            raise

    def get_idle_worker(self):
        try:
            return self.idle_workers.pop()[1]
        except IndexError:
            p = multiprocessing.Pool(1, None, [self.init_args])
            p.done = 0
            return p

    @safe
    def launchTask(self, driver, task):
        """
          Invoked when a task has been launched on this executor (initiated via
          Scheduler.launchTasks).  Note that this task can be realized with a
          thread, a process, or some simple computation, however, no other
          callbacks will be invoked on this executor until this callback has
          returned.
        """
        task_id = task.task_id
        reply_status(driver, task_id, mesos_pb2.TASK_RUNNING)
        try:
            def callback((state, data)):
                reply_status(driver, task_id, state, data)
                with self.lock:
                    _, pool = self.busy_workers.pop(task.task_id.value)
                    pool.done += 1
                    self.idle_workers.append((time.time(), pool))

            pool = self.get_idle_worker()
            self.busy_workers[task.task_id.value] = (task, pool)
            pool.apply_async(run_task, [task.data], callback=callback)

        except Exception, e:
            import traceback
            traceback.print_exc()
            msg = traceback.format_exc()
            reply_status(driver, task_id, mesos_pb2.TASK_LOST, msg)

    def reregistered(self, driver, slaveInfo):
        """
          Invoked when the executor re-registers with a restarted slave.
        """

    def disconnected(self, driver):
        """
          Invoked when the executor becomes "disconnected" from the slave (e.g.,
          the slave is being restarted due to an upgrade).
        """

    @safe
    def killTask(self, driver, taskId):
        """
          Invoked when a task running within this executor has been killed (via
          SchedulerDriver.killTask).  Note that no status update will be sent on
          behalf of the executor, the executor is responsible for creating a new
          TaskStatus (i.e., with TASK_KILLED) and invoking ExecutorDriver's
          sendStatusUpdate.
        """
        reply_status(driver, taskId, mesos_pb2.TASK_KILLED)
        if taskId.value in self.busy_workers:
            task, pool = self.busy_workers.pop(taskId.value)
            pool.terminate()

    def frameworkMessage(self, driver, message):
        """
          Invoked when a framework message has arrived for this executor.  These
          messages are best effort; do not expect a framework message to be
          retransmitted in any reliable fashion.
        """

    @safe
    def shutdown(self, driver):
        """
          Invoked when the executor should terminate all of its currently
          running tasks.  Note that after Mesos has determined that an executor
          has terminated any tasks that the executor did not send terminal
          status updates for (e.g., TASK_KILLED, TASK_FINISHED, TASK_FAILED,
          etc) a TASK_LOST status update will be created.
        """
        def terminate(p):
            try:
                for pi in p._pool:
                    os.kill(pi.pid, signal.SIGKILL)
            except Exception, e:
                pass
        for _, p in self.idle_workers:
            terminate(p)
        for _, p in self.busy_workers.itervalues():
            terminate(p)

        # clean work files
        for d in self.workdir:
            try: shutil.rmtree(d, True)
            except: pass


def start_executor():
    executor = SimpleWorkerExecutor()
    driver = MesosExecutorDriver(executor)
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)


if __name__ == '__main__':
    if os.getuid() == 0:
        gid = os.environ['GID']
        uid = os.environ['UID']
        os.setgid(int(gid))
        os.setuid(int(uid))
    setLogger("mesos-simple-executor","opencluster",logging.INFO)
    start_executor()
