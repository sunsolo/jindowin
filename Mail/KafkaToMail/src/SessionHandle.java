/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/kafka_java/src/main/java/com/kunyan/wokongsvc/mail/SessionHandle.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-03 17:33
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.XmlHandle;

import javax.mail.Authenticator;
import javax.mail.NoSuchProviderException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import java.util.Properties;

/**
 * singleton class 会话句柄，记住此时还没建立连接，只是保持发邮件需要的配置(属性)
 * */
class SessionHandle {
  private Properties props = new Properties();
  /**fromMail 发送邮箱 */
  private String fromMail;
  private String passWord;
  /**session 发送会话 */
  private Session session;

  private SessionHandle(XmlHandle xmlHandle) {
    init(xmlHandle);
  }

  public void init(XmlHandle xmlHandle) {

    setProps(xmlHandle);
    /**
     * 访问匿名内部类的局部变量要定义成final类型
     * */
    final String fromMail = xmlHandle.getElem("/config/mail/sendMail");
    final String passWord = xmlHandle.getElem("/config/mail/passWord");

    Authenticator authenticator = new Authenticator() {             
      @Override
      protected PasswordAuthentication getPasswordAuthentication() {

        return new PasswordAuthentication(fromMail, passWord);
      }
    };
    /** 
     * 使用环境属性和授权信息，创建邮件会话 
     * */
    session =  Session.getDefaultInstance(props, authenticator);
    this.fromMail = fromMail;
    this.passWord = passWord;
  }

  public void setProps(XmlHandle xmlHandle){
    props.put("mail.smtp.auth", xmlHandle.getElem("/config/mail/smtpAuth"));
    props.put("mail.smtp.host", xmlHandle.getElem("/config/mail/smtpHost"));
  }

  public void setFromMail(String fromMail) {
    this.fromMail = fromMail;
  }

  public void setSession(Session initSess) {
    session = initSess;
  }

  public String getFromMail() {
    return fromMail;
  }

  public Session getSession() {
    return session;
  }

  public Transport getTransport(String type) throws NoSuchProviderException {
    return session.getTransport(type);
  }

  public static SessionHandle getInstance(XmlHandle xmlHandle) {
    if(sessionHandle == null) {
      sessionHandle = new SessionHandle(xmlHandle);
    } 

    return sessionHandle;
  }

  private static SessionHandle sessionHandle;
}


