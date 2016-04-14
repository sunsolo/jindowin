/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/SparkKafka/src/main/scala/WorkTask.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-12 18:57
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import com.kunyan.wokongsvc.realtimedata.WorkHandle
import com.kunyan.wokongsvc.realtimedata.DateHandle

import org.apache.log4j.Logger

import java.util.TimerTask
import java.util.TimerTask
import java.util.Timer
import java.util.Calendar

/** 
 * 定时任务实现类，用于每天凌晨更新redis数据库
 **/
class UpdateTask(xmlHandle:XmlHandle) extends TimerTask {
  val update:WorkHandle = WorkHandle(xmlHandle)

  override def run() {
    update.update()
    UpdateTask.infoLogger.info("Update redis success")
  }
}

object UpdateTask {
  val infoLogger:Logger = Logger.getLogger(classOf[InfoLogger])

  def apply(xmlHandle:XmlHandle):UpdateTask = {
    new UpdateTask(xmlHandle)
  }

  def work(xmlHandle:XmlHandle) {
    val updateTask:Timer = new Timer
    val updateTime:Calendar = Calendar.getInstance
    DateHandle.setTime(updateTime, 23, 59, 0, 0)
    updateTask.scheduleAtFixedRate(UpdateTask(xmlHandle), updateTime.getTime(), 24 * 60 * 60 * 1000)
  }
}
