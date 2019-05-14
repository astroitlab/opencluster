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

echo $bin

HOSTLIST="${bin}/nodes"


GOON=true
while $GOON
do
    read line || GOON=false
    if [ -n "$line" ]; then
        HOST=$( extractHostName $line)
        echo "----------Host: $HOST----------"
        ssh -n $HOST "$bin/killnode.sh &"
        ssh -n $HOST "$bin/killworker.sh &"
        ssh -n $HOST "$bin/killservice.sh &"
    fi
done < "$HOSTLIST"

${bin}/killfactory.sh
exit 0
