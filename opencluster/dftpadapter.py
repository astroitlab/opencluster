import os

from dftplocal import DftpLocal
class DftpAdapter(object) :

    def __init__(self, rootUrl):
        self.rootUrl = rootUrl
        self.local = DftpLocal(self.rootUrl)

    def listDir(self,retDir):
        absDir = os.path.join(self.rootDir,retDir)
        if os.path.exists(absDir) :
            raise Exception("dir does not exists")
        if os.path.isdir(absDir) :
            raise Exception("it is not dir")
        return os.listdir(absDir)



