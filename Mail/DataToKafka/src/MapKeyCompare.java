/**
* Copyright @ 2015 ShanghaiKunyan. All rights reserved
* @author     : Sunsolo
* Email       : wukun@kunyan-inc.com
* Date        : 2016-04-08 16:49
* Description : 
*/

package com.kunyan.wokongsvc.mail;

import java.util.Comparator;

class MapKeyCompare implements Comparator<String> {

  @Override
  public int compare(String t1, String t2) {
    int ret = 0;
    do {
      if(t1.length() > t2.length()) {
        ret = -1;
        break;
      } else if(t1.length() < t2.length()) {
        ret = 1;
        break;
      } else {
        if(t1.compareTo(t2) > 0) {
          ret = -1;
          break;
        } else if(t1.compareTo(t2) < 0) {
          ret = 1;
          break;
        }
      }
    } while(false);

    return ret;
  }
}
