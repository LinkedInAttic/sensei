

ECHO OFF
rem usage="Usage: start-sensei-node.bat <conf-dir>"


SET bin=%~dp0


SET lib=%bin%../sensei-core/target/lib
SET dist=%bin%../sensei-core/target
SET resources=%bin%../resources
SET logs=%bin%../logs

IF [%1]==[] GOTO MISSING

SET HEAP_OPTS=-Xmx1g -Xms1g -XX:NewSize=256m
REM JAVA_OPTS=-server -d64
SET JMX_OPTS=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=18889 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false

SET MAIN_CLASS=com.senseidb.search.node.SenseiServer


SET CLASSPATH=%resources%/;%lib%/*;%dist%/*;%1/ext/*
echo "Starting the Sensei. Please make sure that the Zookeeper instance is up. Logs are in the logs directory"
java %JAVA_OPTS% %JMX_OPTS% %HEAP_OPTS% %GC_OPTS% %JAVA_DEBUG% -classpath "%CLASSPATH%"  -Dlog.home=%logs% %MAIN_CLASS% %1 

goto EXIT
:MISSING
echo "Usage: start-sensei-node.bat <conf-dir>"
:EXIT