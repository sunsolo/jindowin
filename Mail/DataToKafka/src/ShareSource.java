/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/MainServer.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-30 21:26
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail;

/**自己的类 */
import com.kunyan.wokongsvc.mail.DateHandle;
import com.kunyan.wokongsvc.mail.KafkaHandle;
import com.kunyan.wokongsvc.mail.MapKeyCompare;
import com.kunyan.wokongsvc.mail.MysqlPool;
import com.kunyan.wokongsvc.mail.MysqlHandle;
import com.kunyan.wokongsvc.mail.RedisHandle;
import com.kunyan.wokongsvc.mail.RedisPool;
import com.kunyan.wokongsvc.mail.XmlHandle;

/**第三方库 */
import com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException;
import org.dom4j.DocumentException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**标准API */
import java.sql.*;
import java.text.SimpleDateFormat;         
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;          // 互斥锁
import java.util.concurrent.locks.ReentrantReadWriteLock; // 读写锁
import java.util.concurrent.ExecutorService;              // 线程池句柄类
import java.util.concurrent.Executors;                    
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;                                 // 方便查找Map
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;                                 // 方便排序Map

class ServerException extends Exception {
  public ServerException(String msg) {
    super(msg);
  }
}

class ShareSource {
  /** 数据资源，因为这些数据内容是不变的，所以不必添加锁机制进行保护 */
  private static XmlHandle xmlHandle;
  private static HashMap<String, String> stockName_ = new HashMap<String, String>();       // 股票代号映射到股票名称
  private static HashMap<String, TreeMap<String, String>> industryStock_ = new HashMap<String, TreeMap<String, String>>(); // 行业映射到所包含的股票代号
  private static HashMap<String, TreeMap<String, String>> conceptStock_ = new HashMap<String, TreeMap<String, String>>();  // 概念映射到所包含的股票代号
  private static HashMap<String, String[]> vindustryStock_ = new HashMap<String, String[]>(); // 行业映射到所包含的股票代号
  private static HashMap<String, String[]> vconceptStock_ = new HashMap<String, String[]>();  // 概念映射到所包含的股票代号
  /** 数据资源，因为这些资源属于读的次数多，而写次数少，所以使用读写锁，读锁之间不排斥*/
  private static HashMap<String, Double> stockFollowRate_ ; // 股票被关注的比率
  private static HashMap<String, Double> stockFollow_ = new HashMap<String, Double>(); // 股票被关注的次数。用来特定行业或概念下的股票排序
  private static HashMap<String, Double> stockSearchRate_ ; // 股票被搜索的比率
  private static HashMap<String, Double> stockVisitRate_ ;  // 股票被查看的比率

  private static HashMap<String, Double> stockFollowAddRate_;  // 被关注的增长比率
  private static HashMap<String, Double> stockSearchAddRate_;  // 被搜索的增长比率
  private static HashMap<String, Double> stockVisitAddRate_;   // 被查看的增长比率

  private static HashMap<String, Double[]> stockEmotion_ = new HashMap<String, Double[]>();      // 股票情感值
  private static HashMap<String, Double[]> sectionEmotion_ = new HashMap<String, Double[]>();    // 部门情感值
  private static HashMap<String, Double[]> industryEmotion_ = new HashMap<String, Double[]>();   // 行业情感值

  private static MysqlPool mysqlPool_;                     // mysql连接池
  private static KafkaHandle kafkaHandle_;                 // kafka连接句柄

  private static RedisPool redisPool_;                     // redis连接池不必加锁保护

  private static int condition = 0;

  private static ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private static ReentrantReadWriteLock conditionLock = new ReentrantReadWriteLock();

  public static void initSource(String path) throws DocumentException {
    XmlHandle xmlHandle = XmlHandle.getInstance(path);
    setXmlHandle(xmlHandle);

    createHandleSource();
    initialMysqlConstantData();
    initialRedisData();
    initialMysqlVariantData();
  }

  public static void updateSource() {
    initialRedisData();
    updateMysqlData();
  }

  public static void setXmlHandle(XmlHandle handle) {
    xmlHandle = handle;
  }

  public static boolean getConditionValue() {
    conditionLock.readLock().lock();
    try {
      if(condition == 0) {
        return true;
      }
    } finally {
      conditionLock.readLock().unlock();
    }
    return false;
  }

  public static void setConditionValue() {
    conditionLock.writeLock().lock();
    try {
      condition = -1;
    } finally {
      conditionLock.writeLock().unlock();
    }
  }

  public static void getMysqlData(Object[] requestData) {
    requestData[0] = stockName_;
    try {
      readWriteLock.readLock().lock();
      requestData[1] = industryStock_;
      requestData[2] = conceptStock_;
      requestData[3] = stockEmotion_;
      requestData[4] = sectionEmotion_;
      requestData[5] = industryEmotion_;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public static void setMysqlData(HashMap<String, TreeMap<String, String>> industry,
      HashMap<String, TreeMap<String, String>> section,
      HashMap<String, Double[]> stockEmotion,
      HashMap<String, Double[]> sectionEmotion,
      HashMap<String, Double[]> industryEmotion) {
    try {
      readWriteLock.writeLock().lock();
      industryStock_ = industry;
      conceptStock_ = section;
      stockEmotion_ = stockEmotion;
      sectionEmotion_ = sectionEmotion;
      industryEmotion_ = industryEmotion;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public static void getRedisData(Object[] requestData) {
    try {
      readWriteLock.readLock().lock();
      requestData[0] = stockFollowRate_;
      requestData[1] = stockSearchRate_;
      requestData[2]  = stockVisitRate_;

      requestData[3] = stockFollowAddRate_;
      requestData[4] = stockSearchAddRate_;
      requestData[5]  = stockVisitAddRate_;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public static void setRedisData(
      HashMap<String, Double> stockFollowRate, 
      HashMap<String, Double> stockSearchRate, 
      HashMap<String, Double> stockVisitRate,
      HashMap<String, Double> stockFollowAddRate,
      HashMap<String, Double> stockSearchAddRate,
      HashMap<String, Double> stockVisitAddRate) {
    try {
      readWriteLock.writeLock().lock();
      stockFollowRate_ = stockFollowRate;
      stockSearchRate_ = stockSearchRate;
      stockVisitRate_  = stockVisitRate;

      stockFollowAddRate_ = stockFollowAddRate;
      stockSearchAddRate_ = stockSearchAddRate;
      stockVisitAddRate_  = stockVisitAddRate;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public static void createHandleSource() {
    try {
      redisPool_ = new RedisPool(xmlHandle);
      mysqlPool_ = new MysqlPool(xmlHandle);
    } catch (ClassNotFoundException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    } catch (MySQLNonTransientConnectionException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    } catch (JedisDataException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    } catch (JedisConnectionException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    }

    kafkaHandle_ = new KafkaHandle(xmlHandle);
  }

  public static void getHandleSource(Object[] requestHandle) {
    requestHandle[0] = mysqlPool_;
    requestHandle[1] = kafkaHandle_;
    requestHandle[2] = redisPool_;
  }

  public static boolean initialMysqlConstantData() {
    try {
      MysqlHandle mysqlUpdateHandle = new MysqlHandle(mysqlPool_.getConnect());
      mysqlUpdateHandle.execTypeInfo("select v_code, v_name from stock_info", stockName_);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    }
    return true;
  }

  public static boolean initialMysqlVariantData() {
    try {
      MysqlHandle mysqlUpdateHandle = new MysqlHandle(mysqlPool_.getConnect());
      mysqlUpdateHandle.execTypeInfo("select hy_name, hy_stock from thshy", industryStock_, vindustryStock_, stockFollow_);
      mysqlUpdateHandle.execTypeInfo("select gn_name, gn_stock from thsgn", conceptStock_, vconceptStock_, stockFollow_);
      mysqlUpdateHandle.execEmotion("select stock, p_percent, n_percent, o_percent from stock_senti_tend", stockEmotion_);
      mysqlUpdateHandle.execEmotion("select secti, p_percent, n_percent, o_percent from section_senti_tend", sectionEmotion_);
      mysqlUpdateHandle.execEmotion("select indus, p_percent, n_percent, o_percent from industry_senti_tend", industryEmotion_);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    }
    return true;
  }

  public static boolean initialRedisData() {
    try {
      RedisHandle redisHandle = new RedisHandle(redisPool_.getConnect());
      HashMap<String, Double> stockFollowRate = new HashMap<String, Double>();
      HashMap<String, Double> stockSearchRate = new HashMap<String, Double>();
      HashMap<String, Double> stockVisitRate = new HashMap<String, Double>();

      HashMap<String, Double> stockFollowAddRate = new HashMap<String, Double>();
      HashMap<String, Double> stockSearchAddRate = new HashMap<String, Double>();
      HashMap<String, Double> stockVisitAddRate = new HashMap<String, Double>();

      DateHandle dateHandle = new DateHandle(new SimpleDateFormat("yyyy-MM-dd"));
      Calendar cal = Calendar.getInstance();
      String time = dateHandle.nowTime(cal);

      redisHandle.selectDb(1);
      redisHandle.infoStatisticCount("set:follow:" + time, 0, -1, stockName_, stockFollowRate, stockFollow_); 
      redisHandle.infoStatisticRate("set:search:" + time, 0, -1, stockName_, stockSearchRate);
      redisHandle.infoStatisticRate("set:visit:"  + time, 0, -1, stockName_, stockVisitRate);

      redisHandle.infoStatisticAdd(stockName_, stockFollowAddRate, stockSearchAddRate, stockVisitAddRate);

      redisPool_.returnConnect(redisHandle);

      setRedisData(stockFollowRate, stockSearchRate, stockVisitRate, stockFollowAddRate, stockSearchAddRate, stockVisitAddRate);
    } catch (JedisConnectionException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    }
    return true;
  }

  public static void updateStockOrder(HashMap<String, TreeMap<String, String>> type, Set<Map.Entry<String, String[]>> entrys) {
    String[] includeStock = null;
    System.out.println(entrys);
    for(Map.Entry<String, String[]> entry : entrys) {
      TreeMap<String, String> content = new TreeMap<String, String>(new MapKeyCompare());
      includeStock = entry.getValue();
      for(int i = 0; i < includeStock.length; i++) {
        content.put(String.valueOf(stockFollow_.get(includeStock[i])) + includeStock[i], includeStock[i]);
      }
      type.put(entry.getKey(), content);
    }
  }

  public static void updateMysqlData() {
    HashMap<String, TreeMap<String, String>> industry = new HashMap<String, TreeMap<String, String>>();
    HashMap<String, TreeMap<String, String>> section = new HashMap<String, TreeMap<String, String>>();
    Set<Map.Entry<String, String[]>> entrys = vindustryStock_.entrySet();
    updateStockOrder(industry, entrys);
    entrys = vconceptStock_.entrySet();
    updateStockOrder(section, entrys);
    HashMap<String, Double[]> stockEmotion = new HashMap<String, Double[]>();
    HashMap<String, Double[]> sectionEmotion = new HashMap<String, Double[]>();
    HashMap<String, Double[]> industryEmotion = new HashMap<String, Double[]>();

    try{
      MysqlHandle mysqlUpdateHandle = new MysqlHandle(mysqlPool_.getConnect());
      mysqlUpdateHandle.execEmotion("select stock, p_percent, n_percent, o_percent from stock_senti_tend", stockEmotion);
      mysqlUpdateHandle.execEmotion("select secti, p_percent, n_percent, o_percent from section_senti_tend", sectionEmotion);
      mysqlUpdateHandle.execEmotion("select indus, p_percent, n_percent, o_percent from industry_senti_tend", industryEmotion);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    }

    setMysqlData(industry, section, stockEmotion, sectionEmotion, industryEmotion);
  }
}

