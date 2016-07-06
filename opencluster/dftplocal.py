from servicecontext import ServiceContext
class DftpLocal(object) :

    def __init__(self, url):#dftp://
        self.url = url
        self.node = ServiceContext.getService("",33,"")


    def readByte(self, f, b, t) :
        pass

    def readByteAsyn(self, f, b, t, locked=False) :
        pass

    def writeByte(self, f,  b,  t, bs) :
        pass

    def writeByteAsyn(self, f,  b,  t,  bs, locked=False) :
        pass

    def writeByteLocked(self, f, b, t, bs) :
        pass
    def getFileMeta(self, f) :
        pass
    def getChildFileMeta(self, f) :
        return []
    def getListRoots(self) :
        return []
    def getHost(self) :
        return ""
    def create(self, f, i=False) :
        pass
    def delete(self, f) :
        return False
    def copy(self, f, e, t) :
        return False
    def copyAsyn(self,f, e, t):
        pass
    def rename(self, f, newName) :
        return False


