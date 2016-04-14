/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/SparkKafka/src/main/scala/RedisHandle.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-11 11:14
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.realtimedata

import com.kunyan.wokongsvc.realtimedata.XmlHandle

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Pipeline}

/** 
 * redis线程池，主要用来在每个分区rdd作为RedisHandle提供Jedis句柄
 **/
class RedisPool private(xmlHandle:XmlHandle) extends Serializable {
  private lazy val jedisPool = init()

  def init():JedisPool = {
    val (host:String, port:Int, timeOut:Int, auth:String) = (xmlHandle.getElem("redis", "host"), 
      (xmlHandle.getElem("redis", "port")).toInt,
      30000, xmlHandle.getElem("redis", "auth"))

    new JedisPool(configPool, host, port, timeOut, auth, 0)
  }

  /** 匿名函数使用 => 符号指向函数体，有名函数使用 = 来表示*/
  def configPool():JedisPoolConfig = {
    val config = new JedisPoolConfig()
    config.setMaxTotal(xmlHandle.getElem("redis", "maxConn").toInt) 
    config.setMaxIdle(xmlHandle.getElem("redis", "maxIdle").toInt)
    config.setMaxWaitMillis(xmlHandle.getElem("redis", "maxWait").toInt)
    config.setTestOnBorrow(true)
    config.setTestOnReturn(true)
     
    return config
  }

  def getHandle():Jedis = {
    jedisPool.getResource
  }

  def returnHandle(jedis:Jedis) {
    jedisPool.returnResource(jedis)
  }
}

/**
 * 伴生类对象，除了方便实例化对象，还可以创建实例化对象的单例模式，当然它的成员全是静态成员
 **/
object RedisPool {
  var redisPool:RedisPool = null
  def apply(xmlHandle:XmlHandle):RedisPool = {
    if(redisPool == null) {
      redisPool = new RedisPool(xmlHandle)
    }
    redisPool
  }
}

/**
 * 有两个构造函数，主构造函数: 没有利用RedisPool来提供Jedis句柄，这是为了并发数较少的操作设计的，以免
 * 申请线程池,例如：在驱动进程中，一天更新一次redis，并发数很低，所以。。。
 * 辅助构造函数: 由RedisPool提供Jedis句柄，这是为了并发数较大的操作设计的,例如：spark计算后写redis
 **/
class RedisHandle(xmlHandle:XmlHandle) {

  var (auth:String, jedis:Jedis) = if(xmlHandle != null) init else ("",  new Jedis())

  def this(jedis:Jedis, auth:String) {
    this(null)
    this.jedis = jedis
    this.auth = auth
  }

  def init() = {
    val (host:String, port:Int, timeOut:Int, auth:String) = {
      (xmlHandle.getElem("redis", "host"),
        xmlHandle.getElem("redis", "port").toInt,
        xmlHandle.getElem("redis", "timeOut").toInt,
        xmlHandle.getElem("redis", "auth"))
    }
    (auth, new Jedis(host, port, timeOut))
  }

  def selectDb(index:Int):Boolean = {
    jedis.auth(auth)
    if (index < 0 && index > 15) false
    else {
      val isSuccess:String = jedis.select(index)
      if (isSuccess.compareTo("OK") != 0) false else true
    }
  }

  def createPipe():Pipeline = {
    jedis.pipelined()
  }
}

/**
 * 伴生类，在这，主要用来简化实例过程
 **/
object RedisHandle {
  def apply(xmlHandle:XmlHandle):RedisHandle = {
    new RedisHandle(xmlHandle)
  }

  def apply(jedis:Jedis, auth:String):RedisHandle = {
    new RedisHandle(jedis, auth)
  }
}
