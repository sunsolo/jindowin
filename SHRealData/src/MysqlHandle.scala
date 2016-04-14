/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/SparkKafka/src/main/scala/MysqlHandle.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-11 14:33
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import com.kunyan.wokongsvc.realtimedata.XmlHandle

import java.sql.{Connection, DriverManager, ResultSet, PreparedStatement}

/**
 * 操作mysql句柄，由于只是第一次程序启动时调用，所以没有定义成线程池形式
 **/
class MysqlHandle private(xmlHandle:XmlHandle) {
  val dbConn:Connection = init
  var pstmt:PreparedStatement = null

  def init():Connection = {
    val (url, user, passWord) = {
      (xmlHandle.getElem("mySql", "url"),
        xmlHandle.getElem("mySql", "user"),
        xmlHandle.getElem("mySql", "passWord"))
    }

    createConn(url, user, passWord)
  }

  def createConn(url:String, user:String, passWord:String):Connection = {
    classOf[com.mysql.jdbc.Driver]
    DriverManager.getConnection(url, user, passWord)
  }

  def executeQuery(sql:String):ResultSet = {
    pstmt = dbConn.prepareStatement(sql)
    pstmt.executeQuery()
  }

  def close() {
    pstmt.close
    dbConn.close
  }
}

object MysqlHandle {
  def apply(xmlHandle:XmlHandle):MysqlHandle = {
    new MysqlHandle(xmlHandle)
  }
}
