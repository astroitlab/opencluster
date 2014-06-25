from parkpatternexector import ParkPatternExector
import configuration


class ParallelService(object):
    '''
    todo : waitWorking need to add software and hardware dependencies
    '''
    computeModeFlag = configuration.Conf.getComputeMode()

    def __init__(self):
        pass

    def waitWorking(self, workerType, host=None, port=None):
        pass
            
    def getWorkersService(self, workerType, host=None, port=None):
        obList = ParkPatternExector.getWorkerTypeList(workerType);
        wsList = []
        if not obList is None :
            for obj in obList :
                hostPort = str(obj.obj).split(":")
                if hostPort[0] != host or int(hostPort[1]) != port:
                    wsList.append([hostPort[0], hostPort[1], workerType])
        return wsList
