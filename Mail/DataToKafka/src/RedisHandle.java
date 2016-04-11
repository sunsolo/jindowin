/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/RedisTest.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-29 00:19
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.DateHandle;
import com.kunyan.wokongsvc.mail.SetElemComp;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map;
import java.util.HashMap;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import com.alibaba.fastjson.JSON;
import org.dom4j.DocumentException; 
import java.text.SimpleDateFormat;
import java.util.Calendar;

class RedisHandle {
  private Jedis jedis;

  RedisHandle(Jedis jedis) {
    this.jedis = jedis;
  }

  public Jedis get() {
    return jedis;
  }

  public boolean ping() {
    String ret = jedis.ping();
    if(ret.compareTo("PING") == 0) {
      return true;
    }

    return false;
  }

  public boolean selectDb(int index) {
    boolean ret = false;
    do {
      if (index < 0 && index > 15) {
        break;
      }

      String isSuccess = jedis.select(index);
      if(isSuccess.compareTo("OK") != 0) {
        break;
      }
      ret = true;
      System.out.println("select db success");
    } while(false);

    return ret;
  }

  public void infoStatisticAdd(HashMap<String, String> stockName, HashMap<String, Double> statisticFollow, HashMap<String, Double> statisticSearch, HashMap<String, Double> statisticVisit) {

    DateHandle dateHandle = new DateHandle(new SimpleDateFormat("yyyy-MM-dd HH"));
    Calendar cal = Calendar.getInstance();
    String nowTime = dateHandle.prevHour(cal, -1);
    String prevTime = dateHandle.prevHour(cal, -1);

    stockAddRate("follow:", nowTime, prevTime, stockName, statisticFollow);
    stockAddRate("search:", nowTime, prevTime, stockName, statisticSearch);
    stockAddRate("visit:", nowTime, prevTime, stockName, statisticVisit);
  }

  public void stockAddRate(String type, String nowTime, String prevTime, HashMap<String, String> stockName, HashMap<String, Double> rateMap) {
    String stockCode = null;
    double nowValue = 0.0;
    double prevValue = 0.0;
    double rate = 0.0;
    Map<String, String> nowCounts = jedis.hgetAll(type + nowTime);
    Map<String, String> prevCounts = jedis.hgetAll(type + prevTime);
    Set<Map.Entry<String, String>> entrys = stockName.entrySet();
    for(Map.Entry<String, String> entry : entrys) {
      stockCode = entry.getKey();
      if(!nowCounts.containsKey(stockCode)) {
        rateMap.put(stockCode, 0.0);
        continue;
      }
      if(!prevCounts.containsKey(stockCode)) {
        rateMap.put(stockCode, 1.0);
        continue;
      }
      nowValue = Double.parseDouble(nowCounts.get(stockCode));
      prevValue = Double.parseDouble(prevCounts.get(stockCode));
      rate = (nowValue - prevValue) / prevValue;
      rateMap.put(stockCode, rate);
    }
  }

  public void infoStatisticRate(String key, long start, long end, 
      HashMap<String, String> stockName, HashMap<String, Double> statisticRet) {

    double count = 0.0;
    double rate  = 0.0;
    Set<Tuple> rets = jedis.zrangeWithScores(key, start, end);
    for(Tuple ret : rets) {
      count = count + ret.getScore();
    }

    for(Tuple tupleStatis : rets) {
      rate = tupleStatis.getScore()/count;
      rate = Math.round(rate * 10000000)/10000000.0;
      statisticRet.put(tupleStatis.getElement(), rate);
    }

    String stockCode = null;
    Set<Map.Entry<String, String>> entrys = stockName.entrySet();
    for(Map.Entry<String, String> entry : entrys) {
      stockCode = entry.getKey();
      if(!statisticRet.containsKey(stockCode)) {
        statisticRet.put(stockCode, 0.0);
      }
    }
  }

  public void infoStatisticCount(String key, long start, long end, 
      HashMap<String, String> stockName, HashMap<String, Double> statisticRet, 
      HashMap<String, Double> retCount) {

    double count = 0.0;
    double rate  = 0.0;
    Set<Tuple> rets = jedis.zrangeWithScores(key, start, end);
    for(Tuple ret : rets) {
      count = count + ret.getScore();
    }

    for(Tuple tupleStatis : rets) {
      rate = tupleStatis.getScore()/count;
      rate = Math.round(rate * 10000000)/10000000.0;
      statisticRet.put(tupleStatis.getElement(), rate);
      retCount.put(tupleStatis.getElement(), tupleStatis.getScore());
    }

    String stockCode = null;
    Set<Map.Entry<String, String>> entrys = stockName.entrySet();
    for(Map.Entry<String, String> entry : entrys) {
      stockCode = entry.getKey();
      if(!statisticRet.containsKey(stockCode)) {
        statisticRet.put(stockCode, 0.0);
        retCount.put(stockCode, 0.0);
      }
    }
  }

  public void getKeys(String type, TreeSet<String> keysSet) {
    Set<String> keys = jedis.keys(type);
    for(String key : keys) {
      keysSet.add(key);
    }
  }

  public void getKeys(TreeSet<String> keysSet) {
    Set<String> keys = jedis.keys("News_*");
    for(String key : keys) {
      keysSet.add(key);
    }
  }

  public boolean hgetNewsContent(String key, String field, String[] content) {
    boolean ret = false;
    do {
      String news = jedis.hget(key, field);
      if(news == null) {
        break;
      }

      Map<String,String> newsContent = (Map<String,String>)JSON.parse(news);
      content[0] = newsContent.get("url");
      content[1] = newsContent.get("title");
      ret = true;
    } while(false);
    return ret;
  }

  public void hgetNewId(String key, String field, TreeSet<String> newSet) {
    String newsId = jedis.hget(key, field);
    if(newsId == null) {
      return;
    }
    String[] news = newsId.split(",");
    for(int i = 0; i < news.length; i++) {
      newSet.add(news[i]);
    }
  }

  public void getNewsDetail(String type, String time, String field, List<String[]> newsDetail) {

    TreeSet<String> newsSets = new TreeSet<String>(new SetElemComp());
    hgetNewId(type + "_" + time, field, newsSets);

    //System.out.println(newsSets);
    for(String newsSet : newsSets) {
      String[] content = new String[2];
      if(hgetNewsContent("News_" + time, newsSet, content) == true) {
        newsDetail.add(content);
      }
      if(newsDetail.size() == 3) {
        break;
      }
    }
  }
}

