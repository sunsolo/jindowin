/**
* Copyright @ 2015 ShanghaiKunyan. All rights reserved
* @author     : Sunsolo
* Email       : wukun@kunyan-inc.com
* Date        : 2016-04-08 17:13
* Description : 
*/
package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.ShareSource;

import java.util.TimerTask;

/**
 * 定时邮件数据的更新
 * */
class UpdateTask extends TimerTask {

  @Override
  public void run() {
    ShareSource.updateSource();
  }
}


