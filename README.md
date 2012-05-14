# SenseiDB (unofficial) release-1.1.2 RC2

Having trouble compiling SenseiDB release-1.1.2-rc have no fear this repo currently hosts an updated version 
untill upstream tends to the pull request [linkedin/sensei#44](/linkedin/sensei/pull/44) Things move fast in this day and age and so even 
the libraries that SenseiDB requires which in our case meant that the ones used in the config files were not 
available anymore. Follow the instructions and find out first hand what all the hype is about.

##Compiling and running SenseiDB release-1.1.2


Grab this repo @ branch [release-1.1.2-rc2](https://github.com/nickl-/sensei-forked/tree/release-1.1.2-rc2) and clone it somewhere in a folder.

```
            git clone git://github.com/nickl-/sensei-forked.git
```

You will first need installed binarios of [java](http://www.oracle.com/technetwork/java/javase/downloads/jdk-6u32-downloads-1594644.html), 
for the virtual machine to run SenseiDB and [maven](http://maven.apache.org/download.html) which SenseiDB uses to retrieve all tie libraries
 and compile the code. 

Once you have java and mavin and the repository has been cloned change directory to sensei-forked.

Then just run the following command to download all the deps and get SenseiDB compiled.


```
           ./bin/build.sh 
```           
```
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Build Order:
[INFO] 
[INFO] sensei parent
[INFO] sensei core
[INFO] sensei gateways
[INFO] sensei hadoop indexing
[INFO] sensei java client
[INFO] sensei war
[INFO] sensei
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building sensei parent 1.1.2
[INFO] ------------------------------------------------------------------------
[INFO] 
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building sensei core 1.1.2
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building sensei gateways 1.1.2
[INFO] ------------------------------------------------------------------------
[INFO] 
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building sensei hadoop indexing 1.1.2
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building sensei java client 1.0.0
[INFO] ------------------------------------------------------------------------
[INFO] 
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building sensei war 1.1.2
[INFO] ------------------------------------------------------------------------
[INFO] 
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building sensei 1.1.2
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO] 
[INFO] sensei parent ..................................... SUCCESS [0.575s]
[INFO] sensei core ....................................... SUCCESS [23.545s]
[INFO] sensei gateways ................................... SUCCESS [3.780s]
[INFO] sensei hadoop indexing ............................ SUCCESS [1.265s]
[INFO] sensei java client ................................ SUCCESS [0.436s]
[INFO] sensei war ........................................ SUCCESS [6.432s]
[INFO] sensei ............................................ SUCCESS [55.661s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 1:32.027s
[INFO] Finished at: Mon May 14 06:33:17 SAST 2012
[INFO] Final Memory: 27M/81M
[INFO] ------------------------------------------------------------------------

```

### How cool is that!!! =)


Once that is done and you manage to hold yourself back 
from running the build script agai, 
you can follow the rest of the
[Getting Started](http://linkedin.github.com/sensei/gettingStarted.html)
instructions without hurting any kittens =)


 * __First start the zookeeper:__
 
 
```
     ./bin/zookeeper-server-start.sh resources/zookeeper.properties &

```


 * __Then start the SenseiDB server node with the cars db schema configuration:__



```
     ./bin/start-sensei-node.sh example/tweets/conf

```


 * __Finaly point your browser at the sample gui front-end and take SenseiDB for a test drive:__



     [http://localhost:8080](http://localhost:8080)


[<img alt="The cool Sensei GUI" src="http://cloud.github.com/downloads/nickl-/sensei-forked/senseidb_sml.png">](http://cloud.github.com/downloads/nickl-/sensei-forked/senseidb.png)

The cool Sensei GUI ollowing you to test just how strong your BQL-fu can be while the 
json query might look dawnting don't fear, our computer friends lap it up like bits & bytes.


# What is Sensei

([http://www.senseidb.com/](http://www.senseidb.com/))

Sensei is a distributed, elastic realtime searchable database.

------------------------------------

## Wiki

Wiki is available at: 

[http://linkedin.jira.com/wiki/display/SENSEI/Home](http://linkedin.jira.com/wiki/display/SENSEI/Home)

## Issues

Issues are tracked at: 

[http://linkedin.jira.com/browse/SENSEI](http://linkedin.jira.com/browse/SENSEI)

## Mailing List / Discussion Group

[http://groups.google.com/group/sensei-search](http://groups.google.com/group/sensei-search)


## Why is this called sensei-forked?

I don't want you to confuse this for for the real project [linkedin/sensei](/linkedin/sensei) which is 
where all the cool kids hang out and you should too. 

This facility is only here temporarily to help you out and getting you experiencing SenseiDB like it's supposed to be... without any pain.
