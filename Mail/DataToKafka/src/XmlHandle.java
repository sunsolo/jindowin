/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/XmlDoc.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-28 22:51
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.mail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.dom4j.Attribute;    
import org.dom4j.Document;    
import org.dom4j.DocumentException;    
import org.dom4j.Element;    
import org.dom4j.io.SAXReader;   
import org.jaxen.JaxenException;

class XmlHandle {
  private Document document;
  private SAXReader saxReader;

  private XmlHandle(String path) throws DocumentException {
    SAXReader saxReader = new SAXReader();
    document = saxReader.read(new File(path));
  }

  public String getElem(String xPath) {
    Element elem = (Element)document.selectSingleNode(xPath);
    return elem.getStringValue();
  }

  public static XmlHandle getInstance(String path) throws DocumentException {
    if(xmlHandle == null) {
      xmlHandle = new XmlHandle(path);
    }
    return xmlHandle;
  }

  private static XmlHandle xmlHandle;
}

