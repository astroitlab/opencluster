#!/bin/sh
#set -x
bin=`dirname "$0"`
. $bin/env.sh

if [ $# != 2 ] && [ $# != 3 ]; then
    echo "Usage:$0 XXXXService.py [start|stop|stopAll] [host|port]"
    exit 0
fi

ip=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | grep "$network" | awk '{print $2}')

case .$2 in
    .start)
        python $bin/$1 $3 &
        sleep 1
        exit 0
	;;
    .stop)
        processID=`ps aux | grep python | grep $1 | awk '{print $2}' | awk 'NR==1{print}'`

        if [ "$processID"x = ""x ]; then
            echo "not found service"
            exit 0
        else
            echo "about to kill....."
            kill -9 $processID
            exit 0
        fi
	;;
    .stopAll)
        processID=`ps aux | grep python | grep $1 | awk '{print $2}'`

        if [ "$processID"x = ""x ]; then
            echo "not found service"
        else
            echo "about to kill....."
            kill -9 $processID
        fi
	;;
esac

exit 0



