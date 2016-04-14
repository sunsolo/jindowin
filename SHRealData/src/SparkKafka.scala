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

import com.kunyan.wokongsvc.realtimedata.XmlHandle
import com.kunyan.wokongsvc.realtimedata.RedisHandle
import com.kunyan.wokongsvc.realtimedata.MysqlHandle

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import java.sql.SQLException
import java.util.{HashMap, Date}

object SparkKafka {
  def main(args: Array[String]) {
    val xmlHandle = XmlHandle("./config.xml")

    val redisHandle = new RedisHandle(xmlHandle)
    if (redisHandle.selectDb(0) == false) System.exit(1)
    println("connect redis success----------------------------------")
    val pipe = redisHandle.createPipe

    /**链接mysql数据库*/
    try {
      var times = ""
      val sql = "select v_code from stock_info"
      val ret = MysqlHandle(xmlHandle).executeQuery(sql)
      println("====" + ret)
      while(ret.next()) {
        for(i <- 1 to ret.getMetaData().getColumnCount()) {
          for(j <- 0 to 288) {
            times = Integer.toString(j)
            println(ret.getString(i))
            pipe.hset("SH:realtime:html:" + ret.getString(i), times, Integer.toString(0))
          }
        }
      }
      pipe.syncAndReturnAll()
    } catch {
      case e:ClassNotFoundException => {
        e.printStackTrace()
        System.exit(1)
      }
      case e:SQLException => {
        e.printStackTrace()
        System.exit(1)
      }
    }

    /**初始化spark运行上下文环境 */
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[1]")
    //val ssc = new StreamingContext(sparkConf, Seconds(10))
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    ssc.checkpoint("hdfs:///user/checkpoint")

    /**初始化kafka参数并创建Dstream对象*/
    val kafkaParam = Map("metadata.broker.list" -> "222.73.34.92:9092")
    val topicParam = "test"
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicParam.split(",").toSet)

    /**开始逻辑部分*/
    val word = lines.map(_._2)
    val words = word.map(x => (x.split("\t")(0),x.split("\t")(2).toInt))  // 传过来的数据是以制表符为间隔
    words.print()
    val result = words.reduceByKeyAndWindow(
      (x:Int, y:Int) => { x + y }, 
      (x:Int, y:Int) => { x - y },
      Seconds(300),
      Seconds(300))

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
