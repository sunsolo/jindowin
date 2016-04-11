/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/JsonHandle.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-05 13:36
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

class JsonHandle {
  private Map<String, String> content_;

  JsonHandle() {
  }

  JsonHandle(Map<String, String> initial) {
    content_ = initial;
  }

  public void setContent(Map<String, String> content) {
    content_ = content;
  }

  public void fillData(String key, String value) {
    content_.put(key, value);
  }

  public String toString(Object ownObject) {
    return JSON.toJSONString(ownObject);
  }

  public String toString() {
    return JSON.toJSONString(content_);
  }

  public void parse(String jsonString) {
    content_ = (Map<String, String>)JSON.parse(jsonString);
  }

  public Object parseObject(String jsonString) {
    return JSON.parseObject(jsonString);
  }
}

