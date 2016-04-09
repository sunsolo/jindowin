#!/usr/bin/env bash

cd ../../../../../../../
java -classpath ./target/kafka_java-1.0-SNAPSHOT.jar com.kunyan.wokongsvc.mail.KafkaEmail
