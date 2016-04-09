/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/kafka_java/src/main/java/KafkaEmail.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-01 19:45
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.XmlHandle;
import com.kunyan.wokongsvc.mail.SessionHandle;
import com.kunyan.wokongsvc.mail.TransportPoolException;
import com.kunyan.wokongsvc.mail.TransportHandlePool;

import org.dom4j.DocumentException;

public class KafkaEmail {
  public static void main(String[] args) {
    try {
      XmlHandle xmlHandle = XmlHandle.getInstance("./config.xml");
      SessionHandle sessionHandle = SessionHandle.getInstance(xmlHandle);
      TransportHandlePool transportHandlePool = TransportHandlePool.getInstance(xmlHandle, sessionHandle);

      ConsumerKafka consumerKafka = new ConsumerKafka(xmlHandle); 
      consumerKafka.run(transportHandlePool);

      consumerKafka.shutdown();
    } catch (InterruptedException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    } catch (DocumentException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    } catch (TransportPoolException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    }
  }
}
