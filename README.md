What is Sensei
===============
([http://www.senseidb.com/](http://www.senseidb.com/))

Sensei is a distributed, elastic realtime searchable database.

------------------------------------

### Wiki

Wiki is available at: 

[http://linkedin.jira.com/wiki/display/SENSEI/Home](http://linkedin.jira.com/wiki/display/SENSEI/Home)

### Issues

Issues are tracked at: 

[http://linkedin.jira.com/browse/SENSEI](http://linkedin.jira.com/browse/SENSEI)

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

        bin/start-sensei-node.sh conf1/
        bin/start-sensei-node.sh conf2/

5. Starting command-line client app

        bin/sensei-client.sh client-conf

