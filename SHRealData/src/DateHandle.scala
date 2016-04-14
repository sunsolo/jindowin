/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/SparkKafka/src/main/scala/DateHandle.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-12 19:53
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata 

import java.util.Calendar
import java.util.Date

/**
 * 由于只提供操作日期数据的接口，所以设计成伴生类，即单例类
 **/
object DateHandle {

  def setTime(cal:Calendar, minute:Int, second:Int, milliSecond:Int) {
    cal.add(Calendar.MINUTE, minute)
    cal.set(Calendar.SECOND, second)
    cal.set(Calendar.MILLISECOND, milliSecond)
  }

  def setTime(cal:Calendar, hour:Int, minute:Int, second:Int, milliSecond:Int) {
    cal.set(Calendar.HOUR_OF_DAY, hour)
    cal.set(Calendar.MINUTE, minute)
    cal.set(Calendar.SECOND, second)
    cal.set(Calendar.MILLISECOND, milliSecond)
  }
}
