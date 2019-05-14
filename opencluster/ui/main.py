import os
import logging
import web
import wsgiref
import datetime
import mimetypes
import time
import traceback

from opencluster.item import FactoryObjValue
from opencluster.configuration import Conf
from opencluster.factory import FactoryContext

from opencluster.ui.api import *
logger = logging.getLogger(__name__)

PWD = os.path.abspath(os.path.dirname(__file__))
urls = [
            "/res/(.*)", "StaticRes",
            "/", "Index",
            "/jobs", "Jobs",
            "/nodes", "Nodes",
            "/workers", "Workers",
            "/services", "Services",
            "/node/(.+)", "Node",
            "/nodeWorker", "WorkerOperation",
            "/nodeService", "ServiceOperation",
            "/about/(about)", "About",
            "/about/(contact)", "About",
]

urls.extend(apiUrls)
urls.extend(["*","WebHandler"])
app = web.application(tuple(urls),globals())

folder_templates_full_path = PWD + Conf.getWebTemplatesPath()

def render(params={},partial=True):
    global_vars = dict({'title':'OpenCluster'}.items() + params.items())
    if not partial:
        return web.template.render(folder_templates_full_path, globals=global_vars)
    else:
        return web.template.render(folder_templates_full_path, base='layout', globals=global_vars)

def titled_render(subtitle=''):
    subtitle = subtitle + ' - ' if subtitle else ''
    return render({'title': subtitle + ' OpenCluster'})

class FactoryInstance(object):
    _instance = None
    _lastTime = None
    def __init__(self):
        pass
    @staticmethod
    def get():
        if not FactoryInstance._instance or time.time()-FactoryInstance._lastTime > 30:
            logger.info("Factory Instance Init......")
            FactoryInstance._instance = FactoryContext.getDefaultFactory()
            logger.info("Factory Instance Init......22222222")
            FactoryInstance._lastTime = time.time()
        return FactoryInstance._instance

class WebServer(object) :
    def __init__(self,__server):
        server = __server.split(":")
        self.server = (server[0],int(server[1]))
        self.setup_session_folder_full_path()
        # web.config.static_path = PWD + Conf.getWebStaticPath()

    def start(self) :
        app.run(self.server)

    def setup_session_folder_full_path(self):
        # global session
        #
        # if not web.config.get("_session"):
        #     folder_sessions_full_path = PWD + Conf.getWebSessionsPath()
        #     session = web.session.Session(app, web.session.DiskStore(folder_sessions_full_path), initializer = {"username": None})
        #     web.config._session = session
        # else:
        #     session = web.config._session
        pass

def server_static(filename, mime_type=None):
    ''''' Serves a file statically '''
    if mime_type is None:
        mime_type = mimetypes.guess_type(filename)[0]
    web.header('Content-Type', bytes('%s' % mime_type))
    filename = os.path.join(Conf.getWebStaticFullPath(), filename)
    if not os.path.exists(filename):
        raise web.NotFound()

    stat = os.stat(filename)
    web.header('Content-Length', '%s' % stat.st_size)
    web.header('Last-Modified', '%s' %
    web.http.lastmodified(datetime.datetime.fromtimestamp(stat.st_mtime)))
    return wsgiref.util.FileWrapper(open(filename, 'rb'), 16384)

class StaticRes(object):
    def GET(self, name):
        return server_static(name)

class About(object):
    def GET(self, name):
        if name=="about" :
            return titled_render("About").about(about = "About")
        elif name == "contact" :
            return titled_render("Contact").about(about = "Contact")
        else:
            return web.NotFound()

class WebHandler(object):
    def GET(self):
        return "OpenCluster"

class Index(object):
    def GET(self):
        try :
            nodes = FactoryInstance.get().getNodes("_node_") or {}
            workers = FactoryInstance.get().getNodeByPrefix("_worker_") or []
            services = FactoryInstance.get().getNodeByPrefix("_service_") or []
            jobs = FactoryInstance.get().getNodes("_manager_") or []
            retNodes = []
            totalMemory = 0
            totalCPU = 0
            usedMemory = 0
            usedCPU = 0
            for v in nodes:
                retNodes.append(v.obj)
                totalMemory += v.obj.totalMemory
                totalCPU += v.obj.cpuCount
                usedMemory += v.obj.usedMemory
                usedCPU += v.obj.cpuTotalPercent
            if totalMemory > 0 :
                usedMemory = int(usedMemory*100/totalMemory)
            if totalCPU > 0 :
                usedCPU = float(usedCPU/len(nodes))

            totalMemory = float(totalMemory/1024)

            dataCPU = [{"value":"%.2f"%usedCPU,"color":"#F38630","label":"Used"},{"value":"%.2f"%(100-usedCPU),"color":"#E0E4CC","label":"UnUsed"}]
            dataMem = [{"value":"%.2f"%usedMemory,"color":"#F38630","label":"Used"},{"value":"%.2f"%(100-usedMemory),"color":"#E0E4CC","label":"UnUsed"}]
            return titled_render().index(nodes = retNodes,jobs=jobs,services=services,workers=workers,dataCPU = dataCPU,dataMem = dataMem,totalCPU=totalCPU,totalMemory=totalMemory)
        except Exception as e:
            return titled_render().error(error=e.message)
class Nodes(object):
    def GET(self):
        try :
            nodes = FactoryInstance.get().getNodes("_node_") or {}
            workers = FactoryInstance.get().getNodeByPrefix("_worker_") or []
            services = FactoryInstance.get().getNodeByPrefix("_service_") or []
            retNodes = []
            for v in nodes:
                retNodes.append(v.obj)
            return titled_render().nodes(nodes = retNodes,services=services,workers=workers)
        except Exception as e:
            return titled_render().error(error=e.message)
class Services(object):
    def GET(self):
        try :
            nodes = FactoryInstance.get().getNodes("_node_") or {}
            services = FactoryInstance.get().getNodeByPrefix("_service_") or []
            serviceTypes = FactoryInstance.get().getNodeByPrefix("_service_",True) or []

            return titled_render().services(services=services,serviceTypes=serviceTypes,time = time)
        except Exception as e:
            return titled_render().error(error=e.message)
class Workers(object):
    def GET(self):
        try :
            nodes = FactoryInstance.get().getNodes("_node_") or {}
            workers = FactoryInstance.get().getNodeByPrefix("_worker_") or []
            workerTypes = FactoryInstance.get().getNodeByPrefix("_worker_",True) or []

            retNodes = []
            for v in nodes:
                retNodes.append(v.obj)
            return titled_render().workers(nodes = retNodes,workers=workers,workerTypes=workerTypes,time = time)
        except Exception as e:
            return titled_render().error(error=e.message)
class Node(object):
    def GET(self,host):
        try :
            node = FactoryInstance.get().getDomainNode("_node_",str(host))
            workers = FactoryInstance.get().getNodeByPrefix("_worker_") or []
            services = FactoryInstance.get().getNodeByPrefix("_service_") or []

            retWorkers = []
            for v in workers:
                if "".join(v.obj.split(":")[0].split(".")) == str(host) :
                    retWorkers.append(v)
            retServices = []
            for v in services :
                if "".join(v.obj.split(":")[0].split(".")) == str(host) :
                    retServices.append(v)
            if node is None :
                raise Exception("Node(%) is not found"%host)
            node.obj.startTime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(node.obj.startTime))

            return titled_render().node(node = node.obj,services=retServices,workers=retWorkers)
        except Exception as e:
            return titled_render().error(error=e.message)
class WorkerOperation(object):
    def GET(self):
        try :
            req = web.input()
            workerType = req.workerType
            host = req.host
            action = req.action
            node = FactoryInstance.get().getDomainNode("_node_",str(host))

            if node is None :
                raise Exception("Node(%s) is not found"%host)
            node.obj.startTime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(node.obj.startTime))

            nodeService = getNodeService(node.obj)

            if action=="start" :
                nodeService.startNewWorker(workerType)
            elif action == "stop" :
                keyArr = FactoryObjValue.getDomainNode(workerType)
                worker = FactoryInstance.get().getDomainNode(keyArr[0],keyArr[1])
                whostPort = worker.obj.split(":")
                nodeService.stopWorker(keyArr[0],whostPort[1])
            elif action == "stopAll" :
                nodeService.stopAllWorker(workerType)

            web.seeother("node/"+host)
        except Exception as e:
            return titled_render().error(error=e.message)
class ServiceOperation(object):
    def GET(self):
        try :
            req = web.input()
            serviceType = req.serviceType
            host = req.host
            action = req.action

            node = FactoryInstance.get().getDomainNode("_node_",str(host))

            if node is None :
                raise Exception("Node(%s) is not found"%host)

            node.obj.startTime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(node.obj.startTime))

            nodeService = getNodeService(node.obj)

            if action=="start" :
                nodeService.startNewService(serviceType)
            elif action == "stop" :
                keyArr = FactoryObjValue.getDomainNode(serviceType)
                worker = FactoryInstance.get().getDomainNode(keyArr[0],keyArr[1])
                shostPort = worker.obj.split(":")
                nodeService.stopService(keyArr[0],shostPort[1])

            elif action == "stopAll" :
                keyArr = FactoryObjValue.getDomainNode(serviceType)
                nodeService.stopAllService(keyArr[0])

            web.seeother("node/"+host)
        except Exception as e:
            traceback.print_exc()
            return titled_render().error(error=e.message)

class Jobs(object):
    def GET(self):
        try :
            jobs = FactoryInstance.get().getNodes("_manager_") or []
            retJobs = []
            for v in jobs:
                retJobs.append(v.obj)
            return titled_render().jobs(jobs = retJobs)
        except Exception as e:
            return web.internalerror()

def getNodeService(node) :
    return FactoryContext.getNodeDaemon(node)

if __name__ == "__main__" :
    thisServer = WebServer(Conf.getWebServers())
    thisServer.start()