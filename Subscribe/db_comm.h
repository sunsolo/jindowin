//  Copyright (c) 2015-2015 The subscribe Authors. All rights reserved.
//  Created on: 2015/12/16 Author: Sunsolo

#ifndef __NOTICE__NOTICESVC__DB_COMM___
#define __NOTICE__NOTICESVC__DB_COMM___

#include <list>
#include <string>

#include "basic/basic_info.h"
#include "net/subscribe_comm.h"

namespace subscribesvc {

class DbSql {
 public:
  DbSql();
  virtual ~DbSql();

  static void Init(std::list<base::ConnAddr>* const addrlist);
  static void Dest();

  int cmp(const std::pair<std::pair<std::string, int>, int> &x, const std::pair<std::string, int> &y) {
    return x.second > y.second;
  }

  /* -----------------------------------------------------------------------*/
  /**
   * @Synopsis             查询所有的板块
   *
   * @Param all_section
   *
   * @Returns   
   */
  /* -----------------------------------------------------------------------*/
  static bool QuerySection(int64 user_id, netcomm_send::SendSection* all_section);

  /* -----------------------------------------------------------------------*/
  /**
   * @Synopsis             查询所有的行业
   *
   * @Param all_industry
   *
   * @Returns   
   */
  /* -----------------------------------------------------------------------*/
  static bool QueryIndustry(int64 user_id, netcomm_send::SendIndustry* all_industry);

  static bool QueryStock(int64 user_id, std::string* stock_code, netcomm_send::SendStock* all_stock);

  static bool QueryStockCodeName(std::map<std::string, std::string>& stock_code_name);

  static bool DeleteSubscribe(const int64 user_id, \
      std::string* stock_code,                      \
      std::string* section,                         \
      std::string* industry);

  static bool AddTimeEmail(const int64 user_id, \
      int64 start_time,\
      int64 end_time,  \
      int64 time_inval,\
      std::string* stock_code, \
      std::string* section,    \
      std::string* industry);

  static bool QuerySubscribe(int64 user_id, 
      netcomm_send::SendAllSubscribe* all_subscribe,
      std::map<std::string, std::string>* stock_name);
  static bool QueryStockSubscribe(int64 user_id, 
      netcomm_send::SendAllSubscribe* all_subscribe,
      std::map<std::string, std::string>* stock_name);
  static bool QuerySectionSubscribe(int64 user_id, 
      netcomm_send::SendAllSubscribe* all_subscribe);
  static bool QueryIndustrySubscribe(int64 user_id, 
      netcomm_send::SendAllSubscribe* all_subscribe);
};

}  //  namespace subscribesvc
#endif
