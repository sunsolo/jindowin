/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/kafka_java/src/main/java/com/kunyan/wokongsvc/mail/ConsumerKafka.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-02 18:24
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.XmlHandle;
import com.kunyan.wokongsvc.mail.TransportHandlePool;
import com.kunyan.wokongsvc.mail.TransportPoolException;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector; 
import kafka.common.MessageStreamsExistException;
import com.kunyan.wokongsvc.mail.TransportHandle; 

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;

class ConsumerKafka {
  /** 
   * 消费者：被用来创建消费者流 
   * */
  private final ConsumerConnector consumer;
  /** 
   * 线程池句柄，用来装载任务 
   * */
  private ExecutorService executor;
  /**
   * 利用消费者创建的小溪流
   **/
  private List<KafkaStream<byte[], byte[]>> streams;

  public ConsumerKafka(XmlHandle xmlHandle) {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createKafkaConfig(xmlHandle));
    /** 
     * 创建线程池
     * @param   线程数量
     */
    executor = Executors.newFixedThreadPool(Integer.parseInt(xmlHandle.getElem("/config/thread/num")));
    streams = createMsgStream(xmlHandle);
  }

  public List<KafkaStream<byte[], byte[]>> createMsgStream(XmlHandle xmlHandle) {
    String topicName = xmlHandle.getElem("/config/kafka/topic");
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topicName, new Integer(xmlHandle.getElem("/config/thread/num")));
    /** 
     * 保证只调用一次createMessageStreams(否则触发异常),所以如果现在还没实现当消费流异常时该如何恢复
     * 创建消息流，一个对应一个处理逻辑，这个流会一直保持连接，没有消息时就阻塞 
     * 最好小溪流和分区数一样多，否则，多了就会有小溪流闲置(一个分区只能被一个消费者，嗯)，少了就有小溪流同时消费几个分区
     * */
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

    /** 可以创建多个会话的消息流 */
    return consumerMap.get(topicName);
  }


  public void run(TransportHandlePool transportHandlePool) throws TransportPoolException {

    for (final KafkaStream stream : streams) {

      TransportHandle transportHandle = transportHandlePool.getTransportHandle();
      if(transportHandle == null) {
        throw new TransportPoolException("The number of transport is not enough");
      }
      executor.submit(new SendThread(stream, transportHandle, transportHandlePool));
    }
  }


  /** 
   * main主线程用来结束应用程序的
   * */
  public void shutdown() throws InterruptedException {
    /** 
     * 用于阻止新任务的提交，但是已经提交的任务会执行完成，线程任务是循环的，所以只用发生异常提交的任务才会执行完成
     * */
    if (executor != null) executor.shutdown(); //shutdown()并不会等待提交任务的完成，若是等待就需调用awaitTermination函数

    try {
      /** 
       * 用于等待子线程结束，再继续执行下面的代码。参数为最长等待时间 
       * 根据官方文档解释，当池子被终止时返回为真，如果设置了时间，当时间到时还没有终止就返回false
       * */
      while (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
        //System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
      }
    } catch (InterruptedException e){
      /**
       * 为了主线程可以手动结束进程，否则线程池收不到主线程的信号
       * */
      throw new InterruptedException();
    } finally {
      if (consumer != null) consumer.shutdown();
    }
  }

  /**
   * 有关kafka的基本配置,当然自己根据需求随意添加
   * */
  private ConsumerConfig createKafkaConfig(XmlHandle xmlHandle) {
    Properties props = new Properties();
    props.put("zookeeper.connect", xmlHandle.getElem("/config/kafka/zooKeeper"));
    props.put("group.id", xmlHandle.getElem("/config/kafka/groupId"));
    props.put("zookeeper.session.timeout.ms", xmlHandle.getElem("/config/kafka/timeOut"));
    props.put("zookeeper.sync.time.ms", xmlHandle.getElem("/config/kafka/syncTime"));
    props.put("auto.commit.interval.ms", xmlHandle.getElem("/config/kafka/autoCommit"));

    return new ConsumerConfig(props);
  }


}
