# -*- coding: UTF-8 -*-
import os,time
import MySQLdb
import pika
import urlparse

def on_message(channel, method_frame, header_frame, body):
    channel.queue_declare(queue=body, auto_delete=True)

    if body.startswith("queue:"):
        queue = body.replace("queue:", "")
        key = body + "_key"
        print("Declaring queue %s bound with key %s" %(queue, key))
        channel.queue_declare(queue=queue, auto_delete=True)
        channel.queue_bind(queue=queue, exchange="test_exchange", routing_key=key)
    else:
        print("Message body", body)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


if __name__ == "__main__" :

    #db = MySQLdb.connect(db="pcims", user="root",passwd="wsl",host="localhost",port=3306,charset="utf8")
    db = MySQLdb.connect(db="pcims", user="root", passwd="wsl", host="::1", port=3306, charset="utf8")
    cur = db.cursor()
    list_dirs = os.walk("J:\\pcims")
    values = []
    for root, dirs, files in list_dirs:

    # files = files.map(lambda f: f.split("\\")[-1])
        for f in files:
            f = f.split("\\")[-1]
            strs = f.split("+")

            if len(strs) == 6:
                values.append((strs[2].decode('gbk').encode("utf-8"),str(strs[0]),strs[3]+"*"+strs[4],str(strs[5]).split(".")[0].decode('gbk').encode("utf-8"),strs[1].decode('gbk').encode("utf-8"),"/uploads/overview/"+f.decode('gbk').encode("utf-8"),"00","1"))
            else:
                print(strs)
    try :
        cur = db.cursor()
        n = cur.executemany("insert into t_work (title,work_no,work_size,work_type,author_name,overview,create_time,status,create_user) values(%s,%s,%s,%s,%s,%s,now(),%s,%s)",values)

        db.commit()
    except Exception,e :
        print("insert t_work error:%s"%e)




