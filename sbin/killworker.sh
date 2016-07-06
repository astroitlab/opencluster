#!/bin/sh

processID=`ps aux | grep python | grep Worker.py | awk '{print $2}'`

if [ "$processID"x = ""x ]; then
	echo "not found worker"
else
	echo "about to kill worker....."
	`kill -9 $processID`
fi

