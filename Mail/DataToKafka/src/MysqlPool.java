/**
* Copyright @ 2015 ShanghaiKunyan. All rights reserved
* @author     : Sunsolo
* Email       : wukun@kunyan-inc.com
* Date        : 2016-04-08 16:52
* Description : 
*/
package com.kunyan.wokongsvc.mail; 

import com.kunyan.wokongsvc.mail.XmlHandle;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException;
import org.dom4j.DocumentException; 

import java.sql.Connection;
import java.sql.SQLException;

class MysqlPool {
  private BoneCP connPool;
  private BoneCPConfig config = new BoneCPConfig();

  MysqlPool(XmlHandle xmlHandle) throws ClassNotFoundException, SQLException, MySQLNonTransientConnectionException {
    Class.forName(xmlHandle.getElem("/config/mysql/driver"));
    createConfig(xmlHandle);
    connPool = new BoneCP(config);
  }

  public void createConfig(XmlHandle xmlHandle) {
    config.setJdbcUrl(xmlHandle.getElem("/config/mysql/url"));
    config.setUsername(xmlHandle.getElem("/config/mysql/user"));
    config.setPassword(xmlHandle.getElem("/config/mysql/password"));
    config.setMinConnectionsPerPartition(Integer.parseInt(xmlHandle.getElem("/config/mysql/minconn")));
    config.setMaxConnectionsPerPartition(Integer.parseInt(xmlHandle.getElem("/config/mysql/maxconn")));
    config.setPartitionCount(Integer.parseInt(xmlHandle.getElem("/config/mysql/partition")));
  }

  public Connection getConnect() throws SQLException {
    return connPool.getConnection();
  }

  public void close() {
    connPool.close();
  }
}

