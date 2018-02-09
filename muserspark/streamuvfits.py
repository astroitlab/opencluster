import sys,time,cStringIO,struct,os
sys.path.extend([os.path.join(os.path.abspath(os.path.dirname(__file__)),'..')])
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from muserstream import MuserStreamUtils
from musertime import MuserTime

# old fashion
# sc = SparkContext("spark://172.31.252.180:7077", "StreamUvfits")

# spark = SparkSession\
#     .builder\
#     .appName("StreamUvfits")\
#     .getOrCreate()
#
# sc = spark.sparkContext
def convert_time(stime):
    tmp = MuserTime()
    # print '%x' % stime
    tmp.nanosecond = (stime & 0x3f)
    if tmp.nanosecond >= 50:
        tmp.nanosecond = 0
    tmp.nanosecond *= 20
    stime >>= 6
    # read microsecond, 6-15
    tmp.microsecond = (stime & 0x3ff)
    stime >>= 10
    # read millisecond 16-25
    tmp.millisecond = (stime & 0x3ff)
    stime >>= 10
    # read second, 26-31
    tmp.second = (stime & 0x3f)
    stime >>= 6
    # read minute, 32-37
    tmp.minute = (stime & 0x3f)
    stime >>= 6
    # read hour, 38-42
    tmp.hour = (stime & 0x1f)
    stime >>= 5
    # read day
    tmp.day = (stime & 0x1f)
    stime >>= 5
    # read month, 48-51
    tmp.month = (stime & 0x0f)
    stime >>= 4
    # read year
    tmp.year = (stime & 0xfff) + 2000
    # print tmp
    return tmp

def uvfitsgen(bytes):
    binFrame = cStringIO.StringIO(bytes)
    binFrame.seek(32)
    tmp = binFrame.read(8)
    tmp_time = struct.unpack('Q', tmp)[0]
    current_frame_time = convert_time(tmp_time)
    time.sleep(1)

    return (current_frame_time.get_string(), len(bytes))

if __name__ == "__main__":
    # conf = SparkConf()
    # conf.setMaster("spark://172.31.252.180:7077").setAppName("StreamUvfits")
    sc = SparkContext(appName="StreamUvfits")
    ssc = StreamingContext(sc, 5)

    frames = MuserStreamUtils.createStream(ssc, "172.31.252.182", 8989)
    print("..............................................................starting processing")

    uvfitsfiles = frames.map(lambda x: uvfitsgen(x)).reduceByKey(lambda x,y: x or y)
    uvfitsfiles.pprint()
    uvfitsfiles.saveAsTextFiles("/astrodata/wsl/tmp/uv","rt")

    ssc.start()
    ssc.awaitTermination()

