/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/SparkKafka/src/main/scala/WorkHandle.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-11 18:33
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import com.kunyan.wokongsvc.realtimedata.XmlHandle
import com.kunyan.wokongsvc.realtimedata.MysqlHandle
import com.kunyan.wokongsvc.realtimedata.RedisHandle 

import scala.collection.mutable.ListBuffer
import redis.clients.jedis.Pipeline
import org.slf4j.LoggerFactory
import org.slf4j.Logger

/**
 * 主构造函数，注意和嵌套语法的应用
 * 在类body内，除了声明语句外(字段声明和函数声明)，其他的代码都会被执行
 * 所以说主构造函数的范围是挺广的
 **/
class WorkHandle(xmlHandle:XmlHandle) {
  val infoLogger:Logger = LoggerFactory.getLogger(classOf[InfoLogger])
  val warnLogger:Logger = LoggerFactory.getLogger(classOf[WarnLogger])
  /** 
   * 不使用这个句柄时，mysql连接就不会被创建
   **/
  lazy val mysqlHandle:MysqlHandle = MysqlHandle(xmlHandle) 
  val redisHandle:RedisHandle = RedisHandle(xmlHandle)

  def initData() {
    if (!redisHandle.selectDb(0)) {
      warnLogger.warn("connect redis fail")
      System.exit(-1)
    }
    infoLogger.info("connect redis success")

    val pipe:Pipeline = redisHandle.createPipe
    val ret = mysqlHandle.executeQuery("select v_code from stock_info")

    while(ret.next()) {
      for(i <- 1 to ret.getMetaData().getColumnCount()) {
        /** 
         * 使用类名访问伴生对象的私有字段
         **/
        WorkHandle.stockCode += ret.getString(i)
        for(j <- 1 to 288) {
          pipe.hset("SH:realtime:sss:" + ret.getString(i), itoa(j), itoa(0))
        }
      }
    }
    pipe.syncAndReturnAll()
  }

  def update() {
    WorkHandle.i = WorkHandle.i + 1
    if (!redisHandle.selectDb(0)) {
      warnLogger.warn("connect redis fail")
    }
    infoLogger.debug("connect redis success")
    /**
     * 闭包的使用，使用外部定义的自由变量(这是因为嵌套作用域规则的影响)
     * 这样不难理解主构造器里面的语法规则
     **/
    val pipe = redisHandle.createPipe
    WorkHandle.stockCode.foreach((x:String) => {
      for(i <- 0 to 288) {
        pipe.hset("SH:realtime:nnn:_" + WorkHandle.i + x, itoa(i), itoa(0))
      }
    })
    pipe.syncAndReturnAll
  }

  def itoa(x:Int):String = {
    Integer.toString(x)
  }
}

object WorkHandle {
  private var i:Int = 0
  /** 
   * 类成员最好初始化
   **/
  private val stockCode:ListBuffer[String] = new ListBuffer[String]()

  /**
   * 这样方便实例化类对象
   **/
  def apply(xmlHandle:XmlHandle):WorkHandle = {
    new WorkHandle(xmlHandle)
  }
}
