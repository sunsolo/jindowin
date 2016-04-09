// Copyright (c) 2015-2015 The restful Authors. All rights reserved.
// Created on: 2015/11/24 Author: wukun
#include "subscribe/redis_comm.h"

#include <string>

#include "logic/logic_comm.h"
#include "tools/tools.h"
#include "dic/base_dic_redis_auto.h"

namespace subscribesvc {
DbRedis::DbRedis() {
}

DbRedis::~DbRedis() {
}

void DbRedis::Dest() {
  base_dic::RedisPool::Dest();
}

void DbRedis::Init(std::list<base::ConnAddr> * const addrlist) {
  base_dic::RedisPool::Init(*addrlist);
}

base_storage::DictionaryStorageEngine* DbRedis::GetRedis() {
  base_dic::AutoDicCommEngine redis_auto_engine;
  base_storage::DictionaryStorageEngine* redis = NULL;

  redis = redis_auto_engine.GetDicEngine();

  if (NULL == redis) {
    LOG_ERROR("Redis GetConnection Error");
    exit(0);
  }

  return redis;
}

bool DbRedis::GetTopStockInfo(std::string* stock_name, double* visit_rate, \
    double* search_rate, double* follow_rate){
  base_storage::DictionaryStorageEngine* redis = DbRedis::GetRedis();
  bool ret = false;

  do {
    if (NULL == redis) {
      break;
    }

    int64 visit_count = 0, search_count = 0, follow_count = 0; 
    int64 all_visit_count = 0, all_search_count = 0, all_follow_count = 0;

    std::string current_time = tools::GetTimeKey(time(NULL));
    current_time = current_time.substr(0, 10);

    /*std::map<std::string, std::string> redis_stock;
    ret = GetRedisStock(redis, 2, "-1", "-1", 0, current_time, redis_stock);
    std::map<std::string, std::string>::iterator itr = redis_stock.begin();
    if(redis_stock.end() == itr || false == ret) {
      break;
    }
    *(stock_name) = itr->first;*/

    std::map<std::string, std::string> redis_stock;
    std::string is_del = "000001";
    ret = GetRedisStock(redis, 2, "-2", "-1", 0, current_time, redis_stock);
    if(false == ret) {
      break;
    }
    std::map<std::string, std::string>::iterator itr = redis_stock.begin();
    for(; itr != redis_stock.end(); itr++) {
      if(is_del == itr->first) {
        continue;
      }

      if(visit_count < atol((itr->second).c_str())) {
        visit_count = atol((itr->second).c_str());
        *(stock_name) = itr->first;
      }
    }

    if(0 == stock_name->size()) {
      ret = false;
      break;
    }

    bool follow_ret = GetStockProp(redis, 1, current_time, \
        *(stock_name), &all_follow_count, &follow_count);
    bool visit_ret = GetStockProp(redis, 2, current_time,  \
        *(stock_name), &all_visit_count, &visit_count);
    bool search_ret = GetStockProp(redis, 3, current_time, \
        *(stock_name), &all_search_count, &search_count);
    if(false == follow_ret || false == visit_ret || false == search_ret) {
      ret = false;
      break;
    }

    if(0 == all_visit_count) {
      *visit_rate = 0.0;
    } else {
      *visit_rate = visit_count / (all_visit_count * 1.0);
    }

    if(0 == all_follow_count) {
      *follow_rate = 0.0;
    } else {
      *follow_rate = follow_count / (all_follow_count * 1.0);
    }

    if(0 == all_search_count) {
      *search_rate = 0.0;
    } else {
      *search_rate = search_count / (all_search_count * 1.0);
    }
    ret = true;
  }while(0);

  return ret;
}

bool DbRedis::GetSingleStockInfo(std::string* stock_name, double* visit_rate, \
    double* search_rate, double* follow_rate) {
  base_storage::DictionaryStorageEngine* redis = DbRedis::GetRedis();
  bool ret = false;

  do {
    if (NULL == redis) {
      break;
    }

    int64 visit_count = 0, search_count = 0, follow_count = 0; 
    int64 all_visit_count = 0, all_search_count = 0, all_follow_count = 0;

    std::string current_time = tools::GetTimeKey(time(NULL));
    current_time = current_time.substr(0, 10);

    bool follow_ret = GetStockProp(redis, 1, current_time, \
        *(stock_name), &all_follow_count, &follow_count);
    bool visit_ret = GetStockProp(redis, 2, current_time, \
        *(stock_name), &all_visit_count, &visit_count);
    bool search_ret = GetStockProp(redis, 3, current_time, \
        *(stock_name), &all_search_count, &search_count);
    if(false == follow_ret || false == visit_ret || false == search_ret) {
      ret = false;
      break;
    }

    if(0 == all_visit_count) {
      *visit_rate = 0.0;
    } else {
      *visit_rate = (visit_count / (all_visit_count * 1.0)) * 1000;
    }

    if(0 == all_follow_count) {
      *follow_rate = 0.0;
    } else {
      *follow_rate = (follow_count / (all_follow_count * 1.0)) * 1000;
    }

    if(0 == all_search_count) {
      *search_rate = 0.0;
    } else {
      *search_rate = (search_count / (all_search_count * 1.0)) * 1000;
    }
    ret = true;
  }while(0);

  return ret;
}

bool DbRedis::GetStockProp(base_storage::DictionaryStorageEngine* redis, \
    int opt_type, const std::string& time, const std::string& stock_name,\
    int64* all_count, int64* top_count) {
  bool ret = false;

  do {
    std::map<std::string, std::string> redis_stock;

    ret = GetRedisStock(redis, opt_type, "0", "-1", 0, time, redis_stock);
    if(false == ret) {
      break;
    }
    std::map<std::string, std::string>::iterator itr = redis_stock.begin();
    for(; itr != redis_stock.end(); itr++) {
      if(itr->first == stock_name) {
        *top_count = atol((itr->second).c_str());
      }
      *all_count = *all_count + atol((itr->second).c_str());
    }
    ret = true;
  }while(0);
  return ret;
}

bool DbRedis::GetRedisStock(base_storage::DictionaryStorageEngine* redis, int opt_type, \
    const char* head, const char* tail, int order, const std::string& time,             \
    std::map<std::string, std::string>& stock_pro) {
  bool ret = false;

  do {
    std::string key_type = "";
    switch(opt_type) {
      case FOLLOW_OPT:
        key_type += "follow:";
        break;
      case VISIT_OPT:
        key_type += "visit:";
        break;
      case SEARCH_OPT:
        key_type += "search:";
        break;
      default:
        break;
    }
    std::string key = std::string("set:") + key_type + time;
    int key_len = key.length();

    ret = redis->GetSortedSet(key.c_str(), key_len, head, tail, order, stock_pro);
  }while(0);
  return ret;
}

bool DbRedis::GetStockInfo(const char* head, const char* tail, int order, \
    std::map<std::string, std::string> &stock_info) {
  base_storage::DictionaryStorageEngine* redis = DbRedis::GetRedis();
  bool ret = false;
  do {
    if (NULL == redis) {
      break;
    }

    std::string current_time = tools::GetTimeKey(time(NULL));
    current_time = current_time.substr(0, 10);
    std::string key = std::string("set:") + "visit:" + current_time;
    int key_len = key.length();

    ret = redis->GetSortedSet(key.c_str(), key_len, head, tail, order, stock_info);
  }while(0);

  return ret;
}

}
