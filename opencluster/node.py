import sys,os
import psutil
import time
from collections import OrderedDict
from configuration import Conf

class Node(object) :
    '''
        Disk : mb
        Memory : mb
        CPUs : int
        CPU Frequency: MHZ
    '''
    mSize = 1024*1024l
    gSize = 1024*1024*1024l
    def __init__(self,host,port,name):
        self.host = host
        self.port = port
        self.name = name

        self.lastTime = 0l
        self.startTime = psutil.boot_time()

        self.totalDisk = 0l
        self.freeDisk = 0l
        self.usedDisk = 0l
        self.percentDisk = 0l


        self.totalMemory = 0l
        self.usedMemory = 0l
        self.availableMemory = 0l
        self.percentMemory = 0l

        self.cpuCount = 0
        self.cpuTotalPercent = 0

        self.availWorkers = OrderedDict()
        self.availServices = OrderedDict()

        self.workers = OrderedDict()
        self.services = OrderedDict()
        self.diskPath = Conf.getNodeDiskPath()

    def calRes(self) :
        phyMem = psutil.virtual_memory()
        self.lastTime = time.time()
        self.totalMemory = phyMem.total/Node.mSize
        self.usedMemory = phyMem.used/Node.mSize
        self.availableMemory = phyMem.available/Node.mSize
        self.percentMemory = phyMem.percent
        self.cpuCount = psutil.cpu_count()
        self.cpuTotalPercent = psutil.cpu_percent()

        disk = psutil.disk_usage(self.diskPath)
        self.totalDisk = disk.total/Node.mSize
        self.freeDisk = disk.free/Node.mSize
        self.usedDisk = disk.used/Node.mSize
        self.percentDisk = disk.percent
        return self

    def __str__(self):
        return "Node:" + self.host + ":" + str(self.port) + ",serviceNum:" + str(len(self.services)) +  ",nodeNum:" + str(len(self.workers))

    def __repr__(self):
        return "Node:" + self.host + ":" + str(self.port)  + ",serviceNum:" + str(len(self.services)) +  ",nodeNum:" + str(len(self.workers))

class FileObj(object):
    def __init__(self,fileName):
        self.fileName = fileName
        self.modifiedTime = ""
        self.type = ""
        self.size = ""

class NodeFile(object) :

    def __init__(self, rootDir):
        self.rootDir = rootDir

    def listDir(self,retDir):

        absDir = os.path.join(self.rootDir,retDir)

        if not os.path.exists(absDir) :
            raise Exception("dir[%s] does not exists"%absDir)
        if not os.path.isdir(absDir) :
            raise Exception("[%s] is not dir"%absDir)
        fileList = os.listdir(absDir) or []
        retList = []

        for f in fileList :
            absFileName = os.path.join(absDir,f)
            fileObj = FileObj(f)
            fileObj.modifiedTime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(os.path.getmtime(absFileName)))

            if os.path.isfile(absFileName) :
                fileObj.size = str(os.path.getsize(absFileName))
                tArr = os.path.splitext(absFileName)
                if len(tArr) == 2:
                    fileObj.type = tArr[1]
            elif os.path.isdir(absFileName) :
                fileObj.type = "D"
            elif os.path.islink(absFileName):
                fileObj.type = "L"

            retList.append(fileObj)
        retList.sort(cmp=lambda x,y : cmp(x.type,y.type),reverse = True)

        return retList

    def readByte(self,fileName,begin,length):

        absFileName = os.path.join(self.rootDir,fileName)
        if not os.path.exists(absFileName) :
            raise Exception("[%s] does not exists"%absFileName)
        try :
            f = open(fileName,"rb")
            f.seek(begin,0)
            return f.read(length)
        finally:
            if f :
                f.close()

    def readWholeFile(self,fileName) :
        BLOCK_SIZE = 102400
        absFileName = os.path.join(self.rootDir,fileName)
        if not os.path.exists(absFileName) :
            raise Exception("[%s] does not exists"%absFileName)
        with open(absFileName, 'rb') as f:
            while True:
                block = f.read(BLOCK_SIZE)
                if block:
                    yield block
                else:
                    return
    def remove(self,fileName):
        absFileName = os.path.join(self.rootDir,fileName)
        if not os.path.exists(absFileName) :
            raise Exception("[%s] does not exists"%absFileName)

        if os.path.isfile(absFileName):
            return os.remove(absFileName)

        if os.path.isdir(absFileName):
            return os.rmdir(absFileName)

        return False

    def rename(self,fileName,newName):
        absFileName = os.path.join(self.rootDir,fileName)
        newFileName = os.path.join(self.rootDir,newName)
        if not os.path.exists(absFileName) :
            raise Exception("[%s] does not exists"%absFileName)

        return os.rename(absFileName,newFileName)

    def createDir(self,curDir,dirName):
        absFileName = os.path.join(self.rootDir,curDir)
        if not os.path.exists(absFileName) :
            raise Exception("[%s] does not exists"%absFileName)

        absFileName = os.path.join(absFileName,dirName)
        return os.makedirs(absFileName)

    def writeFile(self,fileName,fileByte):
        absFileName = os.path.join(self.rootDir,fileName)

        if os.path.exists(absFileName) :
            raise Exception("file exists")

        with open(absFileName, "wb") as f:
            f.write(fileByte)

        return True
