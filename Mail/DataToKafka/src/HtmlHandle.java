/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/HtmlTest.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-03-28 17:29
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.mail;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;

class HtmlHandle {
  private Document doc ;
  private Element  elem ;

  public HtmlHandle(String path, String charset) throws IOException {
    doc = Jsoup.parse(new File(path), charset);
    elem = doc.body();
  }

  public Elements getElem(String id) {
    return elem.getElementsByTag("h1");
  }

  public void setElem(String id, String html) {
    Elements toElem = getElem(id);
    toElem.html(html);
  }
}
