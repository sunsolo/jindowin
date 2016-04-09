/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/kafka_java/src/main/java/com/kunyan/wokongsvc/mail/TransportPoolException.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-09 18:28
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail;

class TransportPoolException extends Exception {
  public TransportPoolException(String message) {
    super(message);
  }

  public TransportPoolException(String message, Throwable cause) {
    super(message, cause);
  }
}
