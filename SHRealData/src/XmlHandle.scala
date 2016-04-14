/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/SparkKafka/src/main/scala/XmlHandle.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-11 10:27
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import scala.xml._

class XmlHandle private(val xmlPath:String) extends Serializable{
  val xmlConfig = loadXml()

  def loadXml():Elem = {
    XML.loadFile(xmlPath)
  }

  def getElem(elemName:String):String = {
    (xmlConfig\elemName).text
  }
  
  def getElem(firstName:String, secondName:String):String = {
    (xmlConfig\firstName\secondName).text
  }

  override def toString():String = {
    this.xmlPath
  }
}

object XmlHandle {
  var xmlHandle:XmlHandle = null
  def apply(xmlPath:String):XmlHandle = {
    if(xmlHandle == null) {
      xmlHandle = new XmlHandle(xmlPath)
    }
    xmlHandle
  }
}


