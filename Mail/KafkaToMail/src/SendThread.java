/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/kafka_java/src/main/java/com/kunyan/wokongsvc/mail/SendThread.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-02 18:15
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.TransportHandlePool;
import com.kunyan.wokongsvc.mail.TransportHandle;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import kafka.consumer.KafkaStream;
import kafka.consumer.ConsumerIterator;

import java.util.Map;
import javax.mail.AuthenticationFailedException;
import javax.mail.MessagingException;
import javax.mail.SendFailedException;
import javax.mail.internet.AddressException;
import javax.mail.NoSuchProviderException;

class SendThread implements Runnable {
  private TransportHandle transportHandle;
  private TransportHandlePool transportHandlePool;
  /**
   * 任务保持自己的消费者流
   * */
  private KafkaStream<byte[], byte[]> consumerStream;
  private Map<String,String> hotwords;
  /**
   * 设定发邮件连接句柄异常后重连或重新分配的次数，在异常后记得一定要close()
   * */
  private int reconnectNum = 0;

  public SendThread(KafkaStream<byte[], byte[]> consumerStream, TransportHandle transportHandle, TransportHandlePool transportHandlePool) {
    this.consumerStream = consumerStream;
    this.transportHandle = transportHandle;
  }

  /** 重载Runnable接口的run()成员函数 */
  public void run() {
    ConsumerIterator<byte[], byte[]> it =consumerStream.iterator();

    /** 当消息流中没有消息时hasNext()就会阻塞 因此可以在主线程中定义好kafkaStream流，然后工作线程把这个当句柄进行消息的获取*/
    while (it.hasNext()) {     

      try {

        /**
         * 工作逻辑,根据自己需求修改
         * */
        hotwords = (Map<String,String>)JSON.parse(new String(it.next().message());
        String email = hotwords.get("email");
        String type = hotwords.get("type");
        String name = hotwords.get("name");
        String hot_words = hotwords.get("hot_words");
        String msgTemplate = "尊敬的用户:\n  您好！您订阅的" + type + "(\"" + name + "\")" + "有新的热词：" + hot_words + "增长";

        if(email == null) {
          System.out.println("The to or content is null");
          continue;
        }
        transportHandle.sendMessage(transportHandle.getReceive(email), "热词", msgTemplate);

      /**
       * 报异常时，continue代表对服务没影响，return代表结束任务，因为任务不能再提交，因此当所有任务结束时，应用也就完了
       * */
      } catch (AuthenticationFailedException e) {
        System.out.println(e.getMessage());
        System.out.println("Bad sender user or password");
        return;
      } catch (AddressException e) {
        System.out.println(e.getMessage());
        System.out.println("To address(recipient) contains control or whitespace in string");
        continue;
      } catch (SendFailedException e) {
        System.out.println(e.getMessage());
        System.out.println("The message could not be sent to some or any of the recipients(Invalid) or send too frequently or InternetAddress initial_exception.");
        continue;
      } catch (MessagingException e) {
        System.out.println(e.getMessage());
        System.out.println("The connection is dead or not in the connected state");
        try{
          transportHandle.close();
        } catch (MessagingException closeInfo) {
          System.out.println(closeInfo.getMessage());
          System.out.println("The connection is dead or not in the connected state");
          return;
        }
        /** 
         * 开始测的时候，这个地方返回后工作线程不再起作用。后来查到原因: ZookeeperConsumerConnector can create message streams at most once
         * 在主线程中的run()函数中第二次创建消息流时会触发异常，则后面的重新启动子线程任务不会触发，
         * 而主线程由于没有捕获异常逻辑，导致主线程结束, 加上捕获异常后程序运行正常
         */
        if(reconnectNum >= 5) {
          return;
        }
        TransportHandle isNull = transportHandlePool.getTransportHandle();
        if(isNull == null) {
          try {
            transportHandle.reconnect();
          } catch (NoSuchProviderException connectInfo) {
            System.out.println(connectInfo.getMessage());
            System.out.println("The connection is dead or not in the connected state");
            return;
          } catch (MessagingException connectInfo) {
            System.out.println(connectInfo.getMessage());
            System.out.println("The connection is dead or not in the connected state");
            return;
          } finally {
            transportHandle.close();
	  }
          reconnectNum++;
          continue;
        } else {
          transportHandle = isNull;
          continue;
        }
      } catch (JSONException e) {
        System.out.println(e.getMessage());
        System.out.println("The json format is error");
        continue;
      }
    }
  }
}
