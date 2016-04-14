/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/SparkKafka/src/main/scala/WorkTask.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-14 10:54
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import com.kunyan.wokongsvc.realtimedata.WorkHandle

import org.apache.log4j.Logger

import java.lang.ClassNotFoundException
import java.sql.SQLException

class WorkTask(xmlHandle:XmlHandle) {
  val work:WorkHandle = WorkHandle(xmlHandle)
  val infoLogger:Logger = Logger.getLogger(classOf[InfoLogger])
  val warnLogger:Logger = Logger.getLogger(classOf[WarnLogger])

  def run() {
    try {
      work.initData
      infoLogger.info("StockCode initial success")
    } catch {
      case e:SQLException => {
        warnLogger.warn("SQLException:mysql exception")
      }
      case e:ClassNotFoundException => {
        warnLogger.warn("com.mysql.jdbc.Driver is not found")
      }
    }
  }
}

object WorkTask {

  def apply(xmlHandle:XmlHandle):WorkTask = {
    new WorkTask(xmlHandle)
  }
}

