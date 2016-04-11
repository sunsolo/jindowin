/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/AllServer.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-05 11:22
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.WorkTask;
import com.kunyan.wokongsvc.mail.UpdateTask;
import com.kunyan.wokongsvc.mail.DateHandle;

import java.util.Timer;                                   // 定时器类
import java.util.Calendar;

import org.dom4j.DocumentException;

public class MainServer {
  public static void main(String[] args) throws DocumentException{
    ShareSource.initSource("./config.xml");
    Timer workTask = new Timer();
    Timer updateTask = new Timer();
    Calendar workBeginTime = Calendar.getInstance();  
    Calendar updateBeginTime = Calendar.getInstance();
    DateHandle.setTime(workBeginTime, 1, 0, 0);       // 工作开始时间
    DateHandle.setTime(updateBeginTime, 1, 0, 20, 0);  // 更新开始时间
    workTask.scheduleAtFixedRate(new WorkTask(), workBeginTime.getTime(), 60 * 1000);
    updateTask.scheduleAtFixedRate(new UpdateTask(), updateBeginTime.getTime(), 60 * 60 * 1000);
  }
}
