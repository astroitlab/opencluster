#!/bin/sh

extractHostName() {
    # extract first part of string (before any whitespace characters)
    SLAVE=$1
    # Remove types and possible comments
    if [[ "$SLAVE" =~ ^([0-9a-zA-Z/.-]+).*$ ]]; then
            SLAVE=${BASH_REMATCH[1]}
    fi
    # Extract the hostname from the network hierarchy
    if [[ "$SLAVE" =~ ^.*/([0-9a-zA-Z.-]+)$ ]]; then
            SLAVE=${BASH_REMATCH[1]}
    fi

    echo $SLAVE
}

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`


HOSTLIST="${bin}/nodes"


if [ ! -f "$HOSTLIST" ]; then
    echo $HOSTLIST is not a valid nodes list
    exit 1
fi

python ${bin}/startfactory.py > /dev/null 2>&1 &
python ${bin}/startfactoryweb.py > /dev/null 2>&1 &


sleep 1

GOON=true
while $GOON
do
    read line || GOON=false
    if [ -n "$line" ]; then
        HOST=$line
        echo "----------Starting Node on $HOST----------"
        ssh -n $HOST "$bin/startnode.sh $HOST &"
    fi
done < "$HOSTLIST"

echo "start the factory and all nodes....finished."
exit 0
