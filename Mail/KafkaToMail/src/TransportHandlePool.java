/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/kafka_java/src/main/java/com/kunyan/wokongsvc/mail/TransportHandlePool.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-04 18:10
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.TransportHandle; 
import com.kunyan.wokongsvc.mail.XmlHandle; 
import com.kunyan.wokongsvc.mail.SessionHandle;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * singleton class 保持工作类句柄(创建和分配) 
 * */
class TransportHandlePool {
  private LinkedList<TransportHandle> transportHandleList = new LinkedList<TransportHandle>();
  private ReentrantLock transportHandleLock = new ReentrantLock();

  private TransportHandlePool(XmlHandle xmlHandle, SessionHandle sessionHandle) {
    int num = Integer.parseInt(xmlHandle.getElem("/config/mail/handleNum"));
    createPool(xmlHandle, sessionHandle, num);
  }

  public void createPool(XmlHandle xmlHandle, SessionHandle sessionHandle, int num) {
    for(int i = 0; i < num; i++) {

      TransportHandle transportHandle = new TransportHandle(xmlHandle, sessionHandle);
      transportHandleList.add(transportHandle);

    }
  }

  public TransportHandle getTransportHandle() {

    transportHandleLock.lock();
    try {
      if(transportHandleList.size() <= 0) {
        return null;
      } else {

        return (TransportHandle)transportHandleList.pop();
      }
    } finally{                                                
      /** 
       * 为防止上面得逻辑模块出现异常或失败导致锁没有释放，其他需要此锁的线程一直等待,防止死锁
       * */
      transportHandleLock.unlock();
    }
  }

  public static TransportHandlePool getInstance(XmlHandle xmlHandle, SessionHandle sessionHandle) {
    if(transportHandlePool == null) {
      transportHandlePool = new TransportHandlePool(xmlHandle, sessionHandle);
    }

    return transportHandlePool;
  }

  private static TransportHandlePool transportHandlePool;
}
