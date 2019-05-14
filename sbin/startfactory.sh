#!/bin/sh
bin=`dirname "$0"`

echo "start a factory."
python ${bin}/startfactory.py > /dev/null 2>&1 &
echo "start webserver of the factory"
python ${bin}/startfactoryweb.py > /dev/null 2>&1 &
echo "start factory mesos."
python ${bin}/startfactorymesos.py > /dev/null 2>&1 &


