/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/KafkaTest.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-30 04:55
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.XmlHandle;

import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.dom4j.DocumentException;

import java.util.Properties;


class KafkaHandle {
  private final Producer<String, String> producer;
  private static ProducerConfig config;
  private String topic_;

  static {
  }

  KafkaHandle(XmlHandle xmlHandle) {
    initialProps(xmlHandle);
    producer = new Producer<String, String>(config);
    topic_ = xmlHandle.getElem("/config/kafka/topic");
  }

  public static void initialProps(XmlHandle xmlHandle) {
    Properties props = new Properties();
    props.put("metadata.broker.list", xmlHandle.getElem("/config/kafka/broker"));
    props.put("serializer.class", xmlHandle.getElem("/config/kafka/serializer"));
    props.put("partitioner.class", xmlHandle.getElem("/config/kafka/partitioner"));

    config = new ProducerConfig(props);
  }

  public void send(String msg) {
    KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic_, msg);
    producer.send(data);
  }

  public void send(String topic, String msg) {
    KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
    producer.send(data);
  }

  public void close() {
    producer.close();
  }
}

