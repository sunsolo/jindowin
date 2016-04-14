/*============================================================================= 
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/spark_kafka/src/main/scala/SparkKafka.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-03 21:34
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.realtimedata 

import com.kunyan.wokongsvc.realtimedata.RedisHandle
import com.kunyan.wokongsvc.realtimedata.XmlHandle
import com.kunyan.wokongsvc.realtimedata.MysqlHandle
import com.kunyan.wokongsvc.realtimedata.WorkHandle
import com.kunyan.wokongsvc.realtimedata.WorkTask

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.log4j.{Logger, PropertyConfigurator}

import java.util.{HashMap, Date, Timer}

object MainServer {
  def main(args: Array[String]) {
    PropertyConfigurator.configure("/home/wukun/SparkKafka/log4j.properties")

    val xmlHandle = XmlHandle("./config.xml")

    WorkTask(xmlHandle).run

    UpdateTask.work(xmlHandle)

    /**
     * 初始化spark运行上下文环境 
     **/
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(300))

    /**
     * 初始化kafka参数并创建Dstream对象
     **/
    val kafkaParam = Map("metadata.broker.list" -> "222.73.34.92:9092")
    val topicParam = "test"
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicParam.split(",").toSet)

    /**
     * 开始逻辑部分
     **/
    val word = lines.map(_._2)
    val words = word.map(x => (x.split("\t")(0),x.split("\t")(2).toInt))
    words.print()
    val result = words.reduceByKey(_ + _)
    result.foreachRDD(x => {

      val nowDate = new Date()
      val nowTime = (nowDate.getHours * 12 + nowDate.getMinutes / 5).toString

      x.foreach(y => {

      val poolHandle = RedisPool(xmlHandle).getHandle
      poolHandle.select(15)
      poolHandle.hset("count:" + y._1, nowTime,  y._2.toString)
      RedisPool(xmlHandle).returnHandle(poolHandle)

      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
