/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/NewPartitioner.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-30 05:31
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail; 

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;


public class NewPartitioner implements Partitioner {
  public NewPartitioner(VerifiableProperties props) {
  }

  public int partition(Object key, int a_numPartitions) {
    int partition = 0;
    String stringKey = (String)key;
    int code = stringKey.hashCode();
    if (code > 0) {
      partition = code % a_numPartitions;
    }
    return partition;
  }
}
