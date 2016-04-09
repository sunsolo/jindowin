/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/kafka_java/src/main/java/com/kunyan/wokongsvc/mail/TransportHandle.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-02 17:59
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail; 

import com.kunyan.wokongsvc.mail.XmlHandle; 
import com.kunyan.wokongsvc.mail.SessionHandle;

import java.util.ArrayList;
import java.util.List;
import javax.mail.internet.AddressException;
import javax.mail.Transport;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.Address;
import javax.mail.Message.RecipientType;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.AuthenticationFailedException;

/**
 * 真正的工作类，包含创建连接，断开，发邮件等     支持重连(记得先close())
 * */
class TransportHandle {
  private String host;
  private String user;
  private String passWord;
  private Transport transport;
  private SessionHandle sessionHandle;

  TransportHandle(XmlHandle xmlHandle, SessionHandle sessionHandle) {

    host = xmlHandle.getElem("/config/mail/smtpHost");
    user = xmlHandle.getElem("/config/mail/sendMail");
    passWord = xmlHandle.getElem("/config/mail/passWord");
    this.sessionHandle = sessionHandle;

    try {
      initConnect();
    } catch (NoSuchProviderException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    } catch (MessagingException e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    }
  }

  public void initConnect() throws MessagingException, NoSuchProviderException {
    connect();
  }

  public void connect() throws MessagingException, NoSuchProviderException {

    transport = sessionHandle.getTransport("smtp");
    transport.connect(host, user, passWord);
  }

  public void reconnect() throws MessagingException, NoSuchProviderException {
    if(transport.isConnected()) {
      close();
    }
    connect();
  }

  public void close() throws MessagingException {
    if(transport.isConnected()) {
      transport.close();
    }
    transport = null;
  }
  /** 
   * 将用逗号间隔的邮件地址转变成list类型 
   * @param to_emails 逗号分隔的收件箱
   * */
  public Address[] getReceive(String to_emails) throws AddressException {
    List list = new ArrayList();
    String[] median=to_emails.split(",");
    for(int i=0;i<median.length;i++){
      list.add(new InternetAddress(median[i]));
    }
    return (Address[])list.toArray(new InternetAddress[list.size()]);
  }

  /** 
   * 设置发件人，收件人(支持多个)，主题，内容=====发送
   * */
  public void sendMessage(Address[] address, 
      String subject, String content) throws MessagingException,AuthenticationFailedException {
    MimeMessage message = new MimeMessage(sessionHandle.getSession());
    InternetAddress from = new InternetAddress(sessionHandle.getFromMail());
    message.setFrom(from);
    message.setRecipients(RecipientType.TO, address);
    message.setSubject(subject);
    message.setContent(content, "text/html;charset=UTF-8");

    transport.send(message);
  }
}
