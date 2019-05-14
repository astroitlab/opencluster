#!/bin/sh
bin=`dirname "$0"`
. $bin/env.sh
ip=""
if [ $# -gt 0 ]; then
 	ip=$1
else
    ip=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | grep "$network" | awk '{print $2}')
fi

if [ "$ip"x = ""x ]; then
 	echo "not found ip"
else
    echo "$ip Node is prepared to start...."
    python $bin/startnode.py $ip 1>/dev/null 2>/dev/null &
fi


