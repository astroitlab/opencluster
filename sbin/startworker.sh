#!/bin/sh

. ./env.sh
if [ $# != 2 ]; then
    echo "Usage:$0 XXXXWorker.py [number of instances]"
    exit 0
fi

ip=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | grep "$network" | awk '{print $2}')
for ((i=1; i < $2+1; i++)); do
    echo "Starting worker $i:$1 on $ip"
    python $1 $ip &
    sleep 1
done


