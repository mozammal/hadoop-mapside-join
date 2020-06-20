# hadoop-mapside-join

## Requirements

For building and running the application you need:

- [JDK 1.7](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- [Maven 3](https://maven.apache.org)

## Running the program locally/hadoop cluster

clone the repo with the command given below: 
```shell
git clone https://github.com/mozammal/hadoop-mapside-join.git
```


One way to run the hadoop program is to use maven from the command line:

```shell
 cd hadoop-mapside-join
 
 start-yarn.sh
 
 start-dfs.sh
 
 hadoop fs -rm -r -f /user/hduser/output/
 
 export HADOOP_CLASSPATH=/usr/lib/jvm/jdk1.7.0_80/lib/tools.jar
 
 sudo mvn install assembly:single
 
 cd target
 
 export HADOOP_CLASSPATH=hadoop-mapside-join-1.0-SNAPSHOT-jar-with-dependencies.jar
 
 hadoop fs -copyFromLocal customer.tbl /user/hduser/input
 
 hadoop fs -copyFromLocal orders.tbl /user/hduser/input
 
 hadoop com.mozammal.MapSideJoinMainJob /user/hduser/input/customer.tbl  /user/hduser/input/orders.tbl    /user/hduser/output
 
 hadoop fs -ls  /user/hduser/output/*
 
 hadoop fs -cat  /user/hduser/output/part-m-00000 | shuf -n 10
 
 hadoop fs -cat  /user/hduser/output/part-m-00001 | shuf -n 10
```


