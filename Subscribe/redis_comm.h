// Copyright (c) 2015-2015 The prism Authors. All rights reserved.
// Created on: 2015/11/25 Author: wukun
#ifndef _SUBSCRIBESVC_REDIS_COMM
#define _SUBSCRIBESVC_REDIS_COMM

#include <list>
#include <map>
#include <string>

#include "logic/logic_basic_info.h"
#include "config/config.h"
#include "basic/basic_info.h"
#include "storage/storage.h"

enum redis_opt {
  FOLLOW_OPT = 1,
  VISIT_OPT  = 2,
  SEARCH_OPT  = 3,
};

namespace subscribesvc {
class DbRedis {
 public:
  DbRedis();
  ~DbRedis();

 public:
  static void Init(std::list<base::ConnAddr> *const addrlist);
  static void Dest();

  static base_storage::DictionaryStorageEngine* GetRedis();

  static bool GetStockInfo(const char* head, \
      const char* tail,                      \
      int order,                             \
      std::map<std::string, std::string> &top_stock);

  static bool GetTopStockInfo(std::string* stock_name, \
      double* visit_rate,                              \
      double* search_rate,                             \
      double* follow_rate);

  static bool GetSingleStockInfo(std::string* stock_name, \
      double* visit_rate,                                 \
      double* search_rate,                                \
      double* follow_rate);

  static bool GetStockProp(base_storage::DictionaryStorageEngine* redis, \
      int opt_type,                                                      \
      const std::string& time,                                           \
      const std::string& stock_name,                                     \
      int64* all_count,                                                  \
      int64* top_count);

  static bool GetRedisStock(base_storage::DictionaryStorageEngine* redis, \
      int opt_type,                                                       \
      const char* head,                                                   \
      const char* tail,                                                   \
      int order,                                                          \
      const std::string& time,                                            \
      std::map<std::string, std::string>& stock_pro);
};
}  // namespace subscribesvc

#endif
