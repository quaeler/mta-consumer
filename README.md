# mta-consumer

## what is it?
A technology soup project consuming MTA updates which are transformed and published to Kafka, to then be
consumed by a Spark bundle which both does a map-reduce count based on train statuses, and writes elements
of the train events into a JanusGraph instance.

Over time, it builds a graph between vertices representing subway lines, stations, and trains, via edge types
representing the semantic ideas of 'serves' and 'arrived at,' the latter with a time stamp property.  In addition to
populating the graph, the Spark bundle also does a map-reduce on train statuses received.

## infrastructure
I'm running this with [Kafka 2.1.0 (targeting Scala 2.12)](https://kafka.apache.org/downloads.html) for historic
reasons, [Spark-Hadoop 2.3.0](https://spark.apache.org/downloads.html) for historic reason, and
[JanusGraph 0.4.1](https://github.com/JanusGraph/janusgraph/releases). I bring up  JanusGraph backed by Cassandra
utilizing Elastic Search.

### what's going on
Some of the code, `Maine`, in this project polls NYC's MTA authority for subway status. This information is returned in a
binary Protocol Buffers format. `Maine` then distills what it wants from the returned poll and publishes instances of
`MTAMessage` to Kafka.

Other of this code, `SparkKafkaConsumer`, acts as a Spark-submit processing bundle that consumes messages off of Kafka.
It then takes these messages and does two things:
1. populates a graph, as described above
2. does a map-reduce on the status of the trains in the messages (which, as of this writing, can be one of three
states: 'in transit to', 'incoming', and 'arrived')

### gotchyas
* Spark 2.x, x &#8805; 3.0, ships with Guava 14.0 it its jars directory; JanusGraph needs at least 18.0. You'll need to
move the 14.0 jar out of your Spark install's `jar` directory and copy in the 18.0 jar.
* Spark, at least, was unhappy with being run with OpenJDK 11, and content with Java8

## building
I've included the transpiled protocol buffer classes, as well as the `.proto` files. As taken from GitHub, all you need
do is add your MTA API from either [here](https://datamine.mta.info/user/register) or [here](https://api.mta.info/) to
the appropriate `st.theori.mta.Maine` class constants. Thereafter, `mvn package` will provide you with all runnable assets.

## running
Once the infrastructure is up and running, there are two classes that should be executed:
* `st.theori.mta.kafka.SparkKafkaConsumer` should be spark-submit with the maven-assembled jar produced by assailing
the package maven target of this project.
* the `st.theori.mta.Maine` class does the work of polling the MTA feed and then publishing the N-many `MTAMessage` to
Kafka - and then sleeping 5s before polling again.

By default, there is a lot of stdout spew going on as i wanted to watch it happen and, as this is purely for my
edification, i see no reason to nuke it until it annoys me.
In addition to that, the map-reduce summary is dumped by the Spark bundle in the following fashion:
```
Update at Sat Feb 01 16:54:59 PST 2020
Trains with status: IN_TRANSIT_TO : 29
Trains with status: INCOMING_AT : 10
Trains with status: STOPPED_AT : 27
```

## now what
As the graph is built you can ask various questions of it via your Gremlin console, for example 'what is a stop
that serves both lines 2 and 5?'
```
algebraic:janusgraph-hadoop loki$ bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/Users/loki/arbeit/third_party/janusgraph-0.4.1-hadoop2/lib/slf4j-log4j12-1.7.12.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/Users/loki/arbeit/third_party/janusgraph-0.4.1-hadoop2/lib/logback-classic-1.1.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
plugin activated: tinkerpop.server
plugin activated: tinkerpop.tinkergraph
21:14:50 WARN  org.apache.hadoop.util.NativeCodeLoader  - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
plugin activated: tinkerpop.hadoop
plugin activated: tinkerpop.spark
plugin activated: tinkerpop.utilities
plugin activated: janusgraph.imports
gremlin> :remote connect tinkerpop.server conf/remote.yaml session
==>Configured localhost/127.0.0.1:8182, localhost/0:0:0:0:0:0:0:1:8182-[b68d9ff1-66ca-420b-a25f-08112d4db45b]
gremlin> :> line5 = g.V().hasLabel('line').has('name', '5').next();[]
gremlin> :> g.V().hasLabel('line').has('name', '2').bothE().where(otherV().is(line5)).otherV().dedup().next().values()
==>206S
gremlin> 
```
Which, if we look up station id '206S' (see below) this is 225th St, which we can see to be correct looking at
[the NYC subway map.](https://new.mta.info/map/5256)

## what can i do on my own fork of this?
There's certainly somethings you can add in order to get familiar with ideas in the project and add to it. Off the top
of my head:
* someone nice took the time to make a CSV file which i've included in the resources directory; the CSV
maps station id to human display text. A nicety would be to load this CSV and have a `Map` via which the vertex
could be populated with a display name.
* tests, of course
* various 'TODO' notes in `st.theori.mta.janus.MTAGraphFactory`

