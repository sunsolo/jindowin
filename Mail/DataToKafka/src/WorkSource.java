/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/WorkSource.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-05 11:31
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.JsonHandle;
import com.kunyan.wokongsvc.mail.KafkaHandle;
import com.kunyan.wokongsvc.mail.MysqlHandle;
import com.kunyan.wokongsvc.mail.MysqlPool;
import com.kunyan.wokongsvc.mail.RedisHandle;
import com.kunyan.wokongsvc.mail.RedisPool;
import com.kunyan.wokongsvc.mail.ShareSource;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Set;
import java.util.ArrayList;
import java.sql.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * 发送数据类
 * */
class WorkSource {
  private int minuteTime_;
  private String nowTime_;
  private static HashMap<String, String> stockName_;               // 股票代号映射到股票名称
  private HashMap<String, TreeMap<String, String>> industryStock_; // 行业映射到所包含的股票代号
  private HashMap<String, TreeMap<String, String>> sectionStock_;  // 概念映射到所包含的股票代号

  private HashMap<String, Double> stockFollowRate_ ;   // 股票被关注的比率
  private HashMap<String, Double> stockSearchRate_ ;   // 股票被搜索的比率
  private HashMap<String, Double> stockVisitRate_ ;    // 股票被查看的比率

  private HashMap<String, Double> stockFollowAddRate_;  // 被关注的增长比率
  private HashMap<String, Double> stockSearchAddRate_;  // 被搜索的增长比率
  private HashMap<String, Double> stockVisitAddRate_;   // 被查看的增长比率

  private HashMap<String, Double[]> stockEmotion_;      // 股票情感值
  private HashMap<String, Double[]> sectionEmotion_;    // 部门情感值
  private HashMap<String, Double[]> industryEmotion_;   // 行业情感值

  private MysqlPool mysqlPool_;                     // mysql连接句柄(工作线程)
  private KafkaHandle kafkaHandle_;                     // kafka连接句柄
  private RedisPool redisPool_;                     // redis连接池不必加锁保护
  //=====================================下面是查询出的订阅数据==========================
  private HashMap<String, String> stockEmail_ = new HashMap<String, String>();
  private HashMap<String, String> sectionEmail_ = new HashMap<String, String>();
  private HashMap<String, String> industryEmail_ = new HashMap<String, String>();

  WorkSource(int minuteTime, String nowTime) {
    minuteTime_ = minuteTime;
    nowTime_ = nowTime;
  }

  public void init() {
    initHandle();
    initMysqlData();
    initRedisData();
    try {
      MysqlHandle mysqlHandle = new MysqlHandle(mysqlPool_.getConnect());
      setStockEmail(mysqlHandle);
      setSectionEmail(mysqlHandle);
      setIndustryEmail(mysqlHandle);
      mysqlHandle.close();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    } finally {
    }
  }

  public void initMysqlData() {
    Object[] mysqlData = new Object[6];
    ShareSource.getMysqlData(mysqlData);
    stockName_ = (HashMap<String, String>)mysqlData[0];
    industryStock_ = (HashMap<String, TreeMap<String, String>>)mysqlData[1];
    sectionStock_ = (HashMap<String, TreeMap<String, String>>)mysqlData[2];
    stockEmotion_ = (HashMap<String, Double[]>)mysqlData[3];
    sectionEmotion_ = (HashMap<String, Double[]>)mysqlData[4];
    industryEmotion_ = (HashMap<String, Double[]>)mysqlData[5];
  }

  public void initRedisData() {
    Object[] redisData = new Object[6];
    ShareSource.getRedisData(redisData);
    stockFollowRate_ = (HashMap<String, Double>)redisData[0];
    stockSearchRate_ = (HashMap<String, Double>)redisData[1];
    stockVisitRate_ = (HashMap<String, Double>)redisData[2];
    stockFollowAddRate_ = (HashMap<String, Double>)redisData[3];
    stockSearchAddRate_ = (HashMap<String, Double>)redisData[4];
    stockVisitAddRate_ = (HashMap<String, Double>)redisData[5];
  }
  public void initHandle() {
    Object[] handleData = new Object[3];
    ShareSource.getHandleSource(handleData);
    mysqlPool_ = (MysqlPool)handleData[0];
    kafkaHandle_ = (KafkaHandle)handleData[1];
    redisPool_ = (RedisPool)handleData[2];
  }

  public void setStockEmail(MysqlHandle mysqlHandle) throws SQLException {
    mysqlHandle.executeProc("{call GetStockTimeEmail(?)}", minuteTime_, stockEmail_);
  }
  public void setSectionEmail(MysqlHandle mysqlHandle) throws SQLException {
    mysqlHandle.executeProc("{call GetSectionTimeEmail(?)}", minuteTime_, sectionEmail_);
  }
  public void setIndustryEmail(MysqlHandle mysqlHandle) throws SQLException {
    mysqlHandle.executeProc("{call GetIndustryTimeEmail(?)}", minuteTime_, industryEmail_);
  }

  public void setSingleStockInfo(JsonHandle jsonHandle, String stockCode, String stockNum) {
    jsonHandle.fillData("name" + stockNum, stockName_.get(stockCode));
    jsonHandle.fillData("code" + stockNum, stockCode);
    jsonHandle.fillData("followRate" + stockNum, stockFollowRate_.get(stockCode).toString());
    jsonHandle.fillData("followAddRate" + stockNum, stockFollowAddRate_.get(stockCode).toString());
    jsonHandle.fillData("searchRate" + stockNum, stockSearchRate_.get(stockCode).toString());
    jsonHandle.fillData("searchAddRate" + stockNum, stockSearchAddRate_.get(stockCode).toString());
    jsonHandle.fillData("visitRate" + stockNum, stockVisitRate_.get(stockCode).toString());
    jsonHandle.fillData("visitAddRate" + stockNum, stockVisitAddRate_.get(stockCode).toString());
  }

  public void setEmotion(JsonHandle jsonHandle, HashMap<String, Double[]> emotionType, String name) {
    if(emotionType.containsKey(name)) {
      Double[] emotion = emotionType.get(name);
      jsonHandle.fillData("stockEmotion", (emotion[0]).toString());
      jsonHandle.fillData("sectionEmotion", (emotion[1]).toString());
      jsonHandle.fillData("industryEmotion", (emotion[2]).toString());
    } else {
      jsonHandle.fillData("stockEmotion", "0");
      jsonHandle.fillData("sectionEmotion", "0");
      jsonHandle.fillData("industryEmotion", "0");
    }
  }

  public void setNewDetail(JsonHandle jsonHandle, RedisHandle redisHanlde, String name, String type) {
    System.out.println(name);
    System.out.println(nowTime_);
    ArrayList<String[]> newDetails = new ArrayList<String[]>();
    redisHanlde.getNewsDetail(type,  nowTime_, name, newDetails);
    String[]  newDetail = null;
    for(int i = 0; i < newDetails.size(); i++) {
      newDetail = newDetails.get(i);
      jsonHandle.fillData("news_url_" + i, newDetail[0]);
      jsonHandle.fillData("news_title_" + i, newDetail[1]);
    }
  }

  public String createStockEmailJson(JsonHandle jsonHandle, Map.Entry<String, String> entry, RedisHandle redisHanlde) {
    String stockCode = entry.getKey();
    Map<String, String> content = new HashMap<String, String>();
    jsonHandle.setContent(content);
    jsonHandle.fillData("type", "stock");
    jsonHandle.fillData("email", entry.getValue());

    setSingleStockInfo(jsonHandle, stockCode, "");
    setEmotion(jsonHandle, stockEmotion_, stockCode);

    setNewDetail(jsonHandle, redisHanlde, stockCode, "Stock");

    System.out.println(jsonHandle.toString());
    return jsonHandle.toString();
  }

  public void sendStockEmailJson() {
    JsonHandle jsonHandle = new JsonHandle();
    RedisHandle redisHanlde = new RedisHandle(redisPool_.getConnect());
    redisHanlde.selectDb(0);
    Set<Map.Entry<String, String>> entrys = stockEmail_.entrySet();
    for(Map.Entry<String, String> entry : entrys) {
      kafkaHandle_.send(createStockEmailJson(jsonHandle, entry, redisHanlde));
    }

    redisPool_.returnConnect(redisHanlde);
  }

  public String createIndustryEmailJson(JsonHandle jsonHandle, Map.Entry<String, String> entry, RedisHandle redisHanlde) {
    String name = entry.getKey();
    Map<String, String> content = new HashMap<String, String>();
    jsonHandle.setContent(content);
    jsonHandle.fillData("name", name);
    jsonHandle.fillData("type", "industry");
    jsonHandle.fillData("email", entry.getValue());
    TreeMap<String, String> stock = industryStock_.get(name);
    Set<Map.Entry<String, String>> stockEntrys = stock.entrySet();
    int stockNum = 1;
    String stockCode = null;
    for(Map.Entry<String, String> stockEntry : stockEntrys) {
      stockCode = stockEntry.getValue();
      setSingleStockInfo(jsonHandle, stockCode, String.valueOf(stockNum));
      if(++stockNum >=4 ) { break;}
    }

    setEmotion(jsonHandle, industryEmotion_, name);

    setNewDetail(jsonHandle, redisHanlde, name, "Industry");

    System.out.println(jsonHandle.toString());
    return jsonHandle.toString();
  }

  public void sendIndustryEmailJson() {
    RedisHandle redisHanlde = new RedisHandle(redisPool_.getConnect());
    redisHanlde.selectDb(0);
    JsonHandle jsonHandle = new JsonHandle();
    Set<Map.Entry<String, String>> entrys = industryEmail_.entrySet();
    for(Map.Entry<String, String> entry : entrys) {
      kafkaHandle_.send(createIndustryEmailJson(jsonHandle, entry, redisHanlde));
    }
    redisPool_.returnConnect(redisHanlde);
  }

  public String createSectionEmailJson(JsonHandle jsonHandle, Map.Entry<String, String> entry, RedisHandle redisHanlde) {
    String name = entry.getKey();
    Map<String, String> content = new HashMap<String, String>();
    jsonHandle.setContent(content);
    jsonHandle.fillData("name", name);
    jsonHandle.fillData("type", "section");
    jsonHandle.fillData("email", entry.getValue());
    TreeMap<String, String> stock = sectionStock_.get(name);
    Set<Map.Entry<String, String>> stockEntrys = stock.entrySet();
    int stockNum = 1;
    String stockCode = null;
    for(Map.Entry<String, String> stockEntry : stockEntrys) {
      stockCode = stockEntry.getValue();
      setSingleStockInfo(jsonHandle, stockCode, String.valueOf(stockNum));
      if(++stockNum >=4 ) { break;}
    }

    setEmotion(jsonHandle, sectionEmotion_, name);

    setNewDetail(jsonHandle, redisHanlde, name, "Section");

    System.out.println(jsonHandle.toString());
    return jsonHandle.toString();
  }

  public void sendSectionEmailJson() {
    RedisHandle redisHanlde = new RedisHandle(redisPool_.getConnect());
    redisHanlde.selectDb(0);
    JsonHandle jsonHandle = new JsonHandle();
    Set<Map.Entry<String, String>> entrys = sectionEmail_.entrySet();
    for(Map.Entry<String, String> entry : entrys) {
      kafkaHandle_.send(createSectionEmailJson(jsonHandle, entry, redisHanlde));
    }
    redisPool_.returnConnect(redisHanlde);
  }
}
