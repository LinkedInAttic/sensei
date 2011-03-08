What is Sensi
===============
([http://sna-projects.com/sensei/](http://sna-projects.com/sensei/))

Sensi is a distributed, elastic realtime searchable database.

------------------------------------

### Wiki

Wiki is available at: 

[http://snaprojects.jira.com/wiki/display/SENSEI/Home](http://snaprojects.jira.com/wiki/display/SENSEI/Home)

### Issues

Issues are tracked at: 

[http://snaprojects.jira.com/browse/SENSEI](http://snaprojects.jira.com/browse/SENSEI)

### Mailing List / Discussion Group

[http://groups.google.com/group/sensei-search](http://groups.google.com/group/sensei-search)

### Getting Started

1. Build

ant

or

mvn package

2. Start ZooKeeper

${ZK_HOME}/bin/zkServer.sh start

3. Run a search node(s)

bin/start-sensei-node.sh 0 17071 0,2,4,6,8,10 node-conf/

bin/start-sensei-node.sh 1 17072 1,3,5,7,9 node-conf/

4. Start the web server (broker)

ant server

or

mvn jetty:run

5. Starting command-line client app

bin/sensei-client.sh client-conf


