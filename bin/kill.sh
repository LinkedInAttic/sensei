
PIDFILE=/tmp/sensei-search-node.pid

echo killing `cat $PIDFILE` and wait for it to die. could take long
kill `cat $PIDFILE`
while ps -p `cat $PIDFILE`  > /dev/null; do sleep 1; done
echo `cat $PIDFILE` killed
echo remove ${PIDFILE}
rm ${PIDFILE}
echo done stop search node

