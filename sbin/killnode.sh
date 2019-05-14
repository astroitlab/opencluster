#!/bin/sh

processID=`ps aux | grep python | grep startnode | awk '{print $2}'`

if [ "$processID"x = ""x ]; then
	echo "not found node"
else
	echo "about to kill node....."
	`kill -9 $processID`
fi


