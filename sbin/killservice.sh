#!/bin/sh

processID=`ps aux | grep python | grep Service.py | awk '{print $2}'`

if [ "$processID"x = ""x ]; then
	echo "not found service"
else
	echo "about to kill service....."
	`kill -9 $processID`
fi

