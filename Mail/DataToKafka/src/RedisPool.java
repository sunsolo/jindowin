/**
* Copyright @ 2015 ShanghaiKunyan. All rights reserved
* @author     : Sunsolo
* Email       : wukun@kunyan-inc.com
* Date        : 2016-04-08 16:59
* Description : 
*/
package com.kunyan.wokongsvc.mail;

import com.kunyan.wokongsvc.mail.XmlHandle;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

class RedisPool {
  private JedisPool jedisPool;
  private JedisPoolConfig config = new JedisPoolConfig();

  RedisPool(XmlHandle xmlHandle) throws JedisDataException, JedisConnectionException {
    String host = xmlHandle.getElem("/config/redis/host");
    int port = Integer.parseInt(xmlHandle.getElem("/config/redis/port"));
    String password = xmlHandle.getElem("/config/redis/password");
    int timeout  = Integer.parseInt(xmlHandle.getElem("/config/redis/timeout"));
    config.setMaxTotal(Integer.parseInt(xmlHandle.getElem("/config/handle/num")));
    config.setMaxIdle(Integer.parseInt(xmlHandle.getElem("/config/handle/num")));
    config.setMaxWaitMillis(Integer.parseInt(xmlHandle.getElem("/config/redis/maxwait")));
    config.setTestOnBorrow(Boolean.getBoolean(xmlHandle.getElem("/config/redis/brrowtest")));

    jedisPool = new JedisPool(config, host, port, timeout, password, 0);
  }

  public Jedis getConnect() {
    return jedisPool.getResource();
  }

  public void returnConnect(RedisHandle redisHandle) {
    jedisPool.returnResource(redisHandle.get());
  }
}

