from py4j.protocol import Py4JJavaError

from pyspark.storagelevel import StorageLevel
from pyspark.streaming.dstream import DStream
from pyspark.serializers import NoOpSerializer
__all__ = ['MuserStreamUtils']


class MuserStreamUtils(object):

    @staticmethod
    def createStream(ssc, host, port, storageLevel=StorageLevel.MEMORY_AND_DISK_2):
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
        helper = MuserStreamUtils._get_helper(ssc._sc)

        return DStream(helper.createStream(ssc._jssc, host, port), ssc, NoOpSerializer())

    @staticmethod
    def _get_helper(sc):
        try:
            return sc._jvm.net.cnlab.muser.MUSERHelper.getInstance()
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                MuserStreamUtils._printErrorMsg(sc)
            raise

    @staticmethod
    def _printErrorMsg(sc):
        print("%s" % (sc.version,))




