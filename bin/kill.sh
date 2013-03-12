PIDFILE=/tmp/sensei-search-node.pid

if [ ! -d "$PIDFILE" ]; then
    echo no pidfile
    exit 1
fi

PID=$(cat $PIDFILE)

echo killing $PID  and wait for it to die. could take a while
kill $PID
while ps -p $PID  > /dev/null; do sleep 1; done
echo $PID killed      
echo remove ${PIDFILE}

if [ ! -d "$PIDFILE" ]; then
    echo done stop search node
else
    rm ${PIDFILE}
    echo done stop search node
fi

