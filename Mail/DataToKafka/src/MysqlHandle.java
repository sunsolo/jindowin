/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/MySqlTest.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-29 20:04
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.MapKeyCompare;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

class MysqlHandle {
  /** 连接句柄 */
  private final Connection dbConn;
  /** 一般的查询更新句柄 */
  private Statement stmt;
  /** 动态查询句柄 */
  private PreparedStatement pstmt;
  /** 调用存储过程句柄 */
  private CallableStatement cstmt;

  MysqlHandle(Connection conn) {
    dbConn = conn;
  }

  public void close() throws SQLException {
    dbConn.close();
  }

  public void execTypeInfo(String sql, HashMap<String, String> stockName) throws SQLException {
    stmt = dbConn.createStatement();
    ResultSet ret = stmt.executeQuery(sql);

    int col = ret.getMetaData().getColumnCount();
    while (ret.next()) { /**将光标当前位置向前移 */
      stockName.put(ret.getString(1), ret.getString(col));
    }
    stmt.close();
  }

  public void execEmotion(String sql, HashMap<String, Double[]> emotions) throws SQLException {
    stmt = dbConn.createStatement();
    ResultSet ret = stmt.executeQuery(sql);

    int col = ret.getMetaData().getColumnCount();
    System.out.println(col);
    while (ret.next()) { /**将光标当前位置向前移 */
      Double[] emotion = new Double[3];
      emotion[0] = ret.getDouble(2);
      emotion[1] = ret.getDouble(3);
      emotion[2] = ret.getDouble(4);
      emotions.put(ret.getString(1), emotion);
    }
    stmt.close();
  }

  public  void execTypeInfo(String sql, HashMap<String, TreeMap<String, String>> typeContent, HashMap<String, String[]> vMapStock, HashMap<String, Double> followRate) throws SQLException {
    stmt = dbConn.createStatement();
    ResultSet ret = stmt.executeQuery(sql);

    double rate = 0.0;
    String code = null;
    String type = null;
    int col = ret.getMetaData().getColumnCount();
    while (ret.next()) { /**将光标当前位置向前移 */
      type = ret.getString(1);
      TreeMap<String, String> content = new TreeMap<String, String>(new MapKeyCompare());
      String[] stockCodes = ret.getString(col).split(",");
      vMapStock.put(type, stockCodes);
      for(int i = 0; i < stockCodes.length; i++) {
        code = stockCodes[i];
        if(followRate.containsKey(code) == false) {
          rate = 0.0;
        } else {
          rate = followRate.get(code);              
        }
        content.put("" + rate + code, code);
      }
      typeContent.put(type, content);
    }
    stmt.close();
  }

  /** procName格式{call GetStockTimeEmail(?)} */
  public  void executeProc(String procName, int time, HashMap<String, String> stockName) throws SQLException {
    cstmt = dbConn.prepareCall(procName);
    cstmt.setInt(1, time);
    cstmt.execute();

    ResultSet ret = cstmt.getResultSet();
    String key = null;
    String value = null;
    int col = ret.getMetaData().getColumnCount();
    while (ret.next()) { /**将光标当前位置向前移 */
      key = ret.getString(1);
      value = ret.getString(col);
      if(stockName.containsKey(key)) {
        value = value + "," + stockName.get(key);
      } 
      stockName.put(key, value);
    }
    cstmt.close();
  }


  public Statement getStateHandle() {
    return stmt;
  }

  public PreparedStatement  getPrepareHandle() {
    return pstmt;
  }

  public CallableStatement getCallHandle() {
    return cstmt;
  }
}

