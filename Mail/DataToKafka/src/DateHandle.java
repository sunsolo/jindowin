/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/DateTest.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-31 05:06
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.mail;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

class DateHandle {
  private SimpleDateFormat formatter;

  DateHandle() {
  }

  DateHandle(SimpleDateFormat formatter) {
    this.formatter = formatter;
  }

  public int nowMinute(Calendar cal) {
    int nowMinu = cal.get(Calendar.MINUTE) + cal.get(Calendar.HOUR_OF_DAY) * 60;
    return nowMinu;
  }

  public void setFormatter(SimpleDateFormat formatter) {
    this.formatter = formatter;
  }

  public SimpleDateFormat getFormatter() {
    return formatter;
  }

  public String nowTime(Calendar cal) {
    return formatter.format(cal.getTime());
  }

  public String prevHour(Calendar cal, int gap) {
    cal.add(Calendar.HOUR_OF_DAY, gap);
    return formatter.format(cal.getTime());
  }

  public static void setTime(Calendar cal, int minute, int second, int milliSecond) {
    cal.add(Calendar.MINUTE, minute);          // 得到下一分钟的时间
    cal.set(Calendar.SECOND, second);          // 设置下一分钟开始的时间(00秒)
    cal.set(Calendar.MILLISECOND, milliSecond);     // 设置精确到微秒
  }

  public static void setTime(Calendar cal, int hour, int minute, int second, int milliSecond) {
    cal.add(Calendar.HOUR_OF_DAY, hour);          // 得到下小时的时间
    cal.set(Calendar.MINUTE, minute);          // 得到下一小时开始分钟的时间
    cal.set(Calendar.SECOND, second);          // 设置下一分钟开始的时间(00秒)
    cal.set(Calendar.MILLISECOND, milliSecond);     // 设置精确到微秒
  }

}
