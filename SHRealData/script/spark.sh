#!/usr/bin/env bash

cd ../../../

spark-submit --files /home/wukun/SparkKafka/log4j.properties --class com.kunyan.wokongsvc.realtimedata.MainServer ./target/spark_kafka-1.0-SNAPSHOT.jar

#java -classpath ./target/spark_kafka-1.0-SNAPSHOT.jar com.kunyan.wokongsvc.realtimedata.TestLog
