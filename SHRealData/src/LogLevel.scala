/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/SparkKafka/src/main/scala/LogLevel.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-13 15:04
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

/**
 * 为了方便日志操作，整个项目中，当想打印warn以上的log日志时，则以WarnLogger类获取logger,同理对于InfoLogger
 * 因为在log4j配置中这两个类的名字被赋予了两个不同的logger
 * */
class InfoLogger {
}

class WarnLogger {
}
