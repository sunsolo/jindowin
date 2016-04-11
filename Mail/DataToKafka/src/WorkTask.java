/**
* Copyright @ 2015 ShanghaiKunyan. All rights reserved
* @author     : Sunsolo
* Email       : wukun@kunyan-inc.com
* Date        : 2016-04-08 17:09
* Description : 
*/

package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.DateHandle;
import com.kunyan.wokongsvc.mail.WorkSource;

import java.util.TimerTask;              
import java.util.Calendar;
import java.text.SimpleDateFormat;

/**
 * 定时邮件数据的发送(发到kafka)都在这
 **/
class WorkTask extends TimerTask {
  private DateHandle dateHandle = new DateHandle(new SimpleDateFormat("yyyyMMdd"));

  WorkTask() {
  }

  static {
  }

  @Override
  public void run() {
    Calendar cal = Calendar.getInstance();
    WorkSource workSource = new WorkSource(dateHandle.nowMinute(cal), dateHandle.nowTime(cal));
    workSource.init();
    workSource.sendStockEmailJson();
    workSource.sendIndustryEmailJson();
    workSource.sendSectionEmailJson();
  }
}
