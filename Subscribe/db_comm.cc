//  Copyright (c) 2015-2015 The subscribe Authors. All rights reserved.
//  Created on: 2015/12/16 Author: Sunsolo

#include "subscribe/db_comm.h"
#include <mysql.h>

#include <sstream>
#include <string>

#include "basic/basic_util.h"
#include "basic/scoped_ptr.h"
#include "db/base_db_mysql_auto.h"
#include "logic/logic_comm.h"
#include "subscribe/redis_comm.h"

namespace subscribesvc {

  void DbSql::Init(std::list<base::ConnAddr>* const addrlist) {
    base_db::MysqlDBPool::Init(*addrlist);
  }

  void DbSql::Dest() {
    base_db::MysqlDBPool::Dest();
  }

  bool DbSql::QuerySection(int64 user_id, netcomm_send::SendSection* all_section) {
    bool ret = false;

    do {
      base_db::AutoMysqlCommEngine auto_engine;
      base_storage::DBStorageEngine* engine = auto_engine.GetDBEngine();
      if (NULL == engine) {
        LOG_ERROR("GetConnection Error");
        break;
      }

      std::stringstream sql_lan;
      sql_lan << "call proc_QuerySection(" << user_id << ");";
      std::string sql_str = sql_lan.str();
      ret = engine->SQLExec(sql_str.c_str());
      if (false == ret) {
        LOG_ERROR("call function query_section() is failure, "\
            "errorkey:exec sql error");
        break;
      }

      int result_num = 0;
      result_num = engine->RecordCount();
      LOG_DEBUG2("call function query_section():"\
          "The query num is [%d]", result_num);
      if (result_num > 0) {

        std::map<std::pair<std::string, int>, int> m;
        MYSQL_ROW rows;
        for (int i = 0; i < result_num; i++) {
          rows = *(reinterpret_cast<MYSQL_ROW*>(engine->FetchRows()->proc));
          std::string tmp = rows[0];
          m.insert(std::make_pair(std::make_pair(tmp, atoi(rows[2])), atoi(rows[1])));
        }

        std::vector<std::pair<std::pair<std::string,int>, int> > vec(m.begin(),m.end());
        for(int i = 0; i < 12; i++) {
          scoped_ptr<base_logic::DictionaryValue> section(\
              new base_logic::DictionaryValue());
          scoped_ptr<base_logic::StringValue> gn_name(\
              new base_logic::StringValue((vec[i].first).first));
          section->Set("gn_name", gn_name.release());
          section->SetInteger("gn_count", vec[i].second);
          section->SetInteger("is_subscribe", (vec[i].first).second);
          all_section->add_section_count(section.release());
        }
        ret = true;
      }
    }while(0);

    return ret;
  }

  bool DbSql::QueryIndustry(int64 user_id, netcomm_send::SendIndustry* all_industry) {
    bool ret = false;

    do {
      base_db::AutoMysqlCommEngine auto_engine;
      base_storage::DBStorageEngine* engine = auto_engine.GetDBEngine();
      if (NULL == engine) {
        LOG_ERROR("GetConnection Error");
        break;
      }

      std::stringstream sql_lan;
      sql_lan << "call proc_QueryIndustry(" << user_id << ");";
      std::string sql_str = sql_lan.str();
      ret = engine->SQLExec(sql_str.c_str());
      if (false == ret) {
        LOG_ERROR("call function query_industry() is failure, "\
            "errorkey:exec sql error");
        break;
      }

      int result_num = 0;
      result_num = engine->RecordCount();
      LOG_DEBUG2("call function query_industry():"\
          "The query num is [%d]", result_num);
      if (result_num > 0) {

        std::map<std::pair<std::string, int>, int> m;
        MYSQL_ROW rows;
        for (int i = 0; i < result_num; i++) {
          rows = *(reinterpret_cast<MYSQL_ROW*>(engine->FetchRows()->proc));
          std::string tmp = rows[0];
          m.insert(std::make_pair(std::make_pair(tmp, atoi(rows[2])), atoi(rows[1])));
        }

        std::vector<std::pair<std::pair<std::string,int>, int> > vec(m.begin(),m.end());
        for(int i = 0; i < 12; i++) {
          scoped_ptr<base_logic::DictionaryValue> industry(\
              new base_logic::DictionaryValue());
          scoped_ptr<base_logic::StringValue> hy_name(\
              new base_logic::StringValue((vec[i].first).first));
          industry->Set("hy_name", hy_name.release());
          industry->SetInteger("hy_count", vec[i].second);
          industry->SetInteger("is_subscribe", (vec[i].first).second);
          all_industry->add_industry_count(industry.release());
        }
        ret = true;
      }
    }while(0);

    return ret;
  }

  bool DbSql::QueryStock(int64 user_id, std::string* stock_code, netcomm_send::SendStock* all_stock) {
    bool ret = false;

    do {
      base_db::AutoMysqlCommEngine auto_engine;
      base_storage::DBStorageEngine* engine = auto_engine.GetDBEngine();
      if (NULL == engine) {
        LOG_ERROR("GetConnection Error");
        break;
      }

      std::stringstream sql_lan;
      sql_lan << "call proc_GetTopStockInfo(" << user_id << ",\'" << *stock_code << "\');";
      std::string sql_str = sql_lan.str();
      ret = engine->SQLExec(sql_str.c_str());
      if (false == ret) {
        LOG_ERROR("call function query_industry() is failure, "\
            "errorkey:exec sql error");
        break;
      }

      int result_num = 0;
      result_num = engine->RecordCount();
      LOG_DEBUG2("call function query_industry():"\
          "The query num is [%d]", result_num);
      if (result_num > 0) {

        MYSQL_ROW rows;
        for (int i = 0; i < result_num; i++) {
          rows = *(reinterpret_cast<MYSQL_ROW*>(engine->FetchRows()->proc));

          scoped_ptr<base_logic::DictionaryValue> top_stock(\
              new base_logic::DictionaryValue());
          scoped_ptr<base_logic::StringValue> stock_code(\
              new base_logic::StringValue(rows[0]));
          scoped_ptr<base_logic::StringValue> stock_name(\
              new base_logic::StringValue(rows[1]));
          top_stock->Set("stock_code", stock_code.release());
          top_stock->Set("stock_name", stock_name.release());
          top_stock->SetInteger("stock_count", atoi(rows[2]));
          top_stock->SetInteger("is_subscribe", atoi(rows[3]));
          all_stock->add_stock_count(top_stock.release());
        }

        ret = true;
      }
    }while(0);

    return ret;
  }

  bool DbSql::QueryStockCodeName(std::map<std::string, std::string>& stock_code_name) {
    bool ret = false;

    do {
      base_db::AutoMysqlCommEngine auto_engine;
      base_storage::DBStorageEngine* engine = auto_engine.GetDBEngine();
      if (NULL == engine) {
        LOG_ERROR("GetConnection Error");
        break;
      }

      std::stringstream sql_lan;
      sql_lan << "call proc_GetStockCodeName()";
      std::string sql_str = sql_lan.str();
      ret = engine->SQLExec(sql_str.c_str());
      if (false == ret) {
        LOG_ERROR("call function QueryStockCodeName() is failure,"\
            "errorkey:exec sql error");
        break;
      }

      int result_num = 0;
      result_num = engine->RecordCount();
      LOG_DEBUG2("call function QueryStockCodeName:"\
          "The query num is [%d]", result_num);
      if (result_num > 0) {
        MYSQL_ROW rows;

        for (int i = 0; i < result_num; i++) {
          rows = *(reinterpret_cast<MYSQL_ROW*>(engine->FetchRows()->proc));
          stock_code_name.insert(std::make_pair(rows[0], rows[1]));
        }
        ret = true;
      }
    }while(0);

    return ret;
  }

  bool DbSql::AddTimeEmail(const int64 user_id, \
      int64        start_time,                  \
      int64        end_time,                    \
      int64        time_inval,                  \
      std::string* stock_code,                  \
      std::string* section,                     \
      std::string* industry) {
    bool ret = false;

    do {
      base_db::AutoMysqlCommEngine auto_engine;
      base_storage::DBStorageEngine* engine = auto_engine.GetDBEngine();
      if (NULL == engine) {
        LOG_ERROR("GetConnection Error");
        break;
      }

      if (!(base::BasicUtil::UrlDecode(*stock_code, *stock_code) && \
            base::BasicUtil::UrlDecode(*section, *section) && \
            base::BasicUtil::UrlDecode(*industry, *industry))) {
        LOG_ERROR("Url decode chinese failed");
        break;
      }

      *stock_code = (*stock_code).substr(0, (*stock_code).size() - 1);
      std::stringstream sql_lan;
      sql_lan << "call proc_AddTimeEmail(" << user_id << "," <<\
        start_time << "," << end_time << "," << time_inval << ", \'" <<\
        *stock_code <<"\',\'"<< *section <<"\',\'"<< *industry <<"\');";
      std::string sql_str = sql_lan.str();
      ret = engine->SQLExec(sql_str.c_str());
      if (false == ret) {
        LOG_ERROR("call function AddTimeEmail() is failure, "\
            "errorkey:exec sql error");
        break;
      }

    }while(0);

    return ret;
  }

  bool DbSql::DeleteSubscribe(const int64 user_id, \
      std::string* stock_code,                      \
      std::string* section,                         \
      std::string* industry) {
    bool ret = false;

    do {
      base_db::AutoMysqlCommEngine auto_engine;
      base_storage::DBStorageEngine* engine = auto_engine.GetDBEngine();
      if (NULL == engine) {
        LOG_ERROR("GetConnection Error");
        break;
      }

      if (!(base::BasicUtil::UrlDecode(*stock_code, *stock_code) && \
            base::BasicUtil::UrlDecode(*section, *section) && \
            base::BasicUtil::UrlDecode(*industry, *industry))) {
        LOG_ERROR("Url decode chinese failed");
        break;
      }

      *stock_code = (*stock_code).substr(0, (*stock_code).size() - 1);
      std::stringstream sql_lan;
      sql_lan << "call proc_DeleteSubscribe(" << user_id << ", \'" <<\
        *stock_code <<"\',\'"<< *section <<"\',\'"<< *industry <<"\');";
      std::string sql_str = sql_lan.str();
      ret = engine->SQLExec(sql_str.c_str());
      if (false == ret) {
        LOG_ERROR("call function DeleteSubscribe() is failure, "\
            "errorkey:exec sql error");
        break;
      }

    }while(0);

    return ret;
  }

  bool DbSql::QuerySubscribe(int64 user_id, 
      netcomm_send::SendAllSubscribe* all_subscribe,
      std::map<std::string, std::string>* stock_name) {
    bool ret = false;

    do {
      ret = QueryStockSubscribe(user_id, all_subscribe, stock_name);
      if(ret == false) {
        break;
      }
      ret = QuerySectionSubscribe(user_id, all_subscribe);
      if(ret == false) {
        break;
      }
      ret = QueryIndustrySubscribe(user_id, all_subscribe);
    }while(0);

    return ret;
  }

  bool DbSql::QuerySectionSubscribe(int64 user_id, netcomm_send::SendAllSubscribe* all_subscribe) {
    bool ret = false;

    do {
      base_db::AutoMysqlCommEngine auto_engine;
      base_storage::DBStorageEngine* engine = auto_engine.GetDBEngine();
      if (NULL == engine) {
        LOG_ERROR("GetConnection Error");
        break;
      }

      std::stringstream sql_lan;
      sql_lan << "call proc_QuerySectionSubscribe(" << user_id << ");";
      std::string sql_str = sql_lan.str();
      ret = engine->SQLExec(sql_str.c_str());
      if (false == ret) {
        LOG_ERROR("call function query_industry() is failure, "\
            "errorkey:exec sql error");
        break;
      }

      int result_num = 0;
      result_num = engine->RecordCount();
      LOG_DEBUG2("call function query_industry():"\
          "The query num is [%d]", result_num);
      if (result_num < 0) {
        break;
      }

      MYSQL_ROW rows;
      for (int i = 0; i < result_num; i++) {
        rows = *(reinterpret_cast<MYSQL_ROW*>(engine->FetchRows()->proc));
        scoped_ptr<base_logic::DictionaryValue> section(\
            new base_logic::DictionaryValue());
        scoped_ptr<base_logic::StringValue> section_name(\
            new base_logic::StringValue(rows[0]));
        section->Set("section_name", section_name.release());
        all_subscribe->add_section(section.release());
      }

      ret = true;
    }while(0);

    return ret;
  }

  bool DbSql::QueryStockSubscribe(int64 user_id, 
      netcomm_send::SendAllSubscribe* all_subscribe,
      std::map<std::string, std::string>* name) {
    bool ret = false;

    do {
      base_db::AutoMysqlCommEngine auto_engine;
      base_storage::DBStorageEngine* engine = auto_engine.GetDBEngine();
      if (NULL == engine) {
        LOG_ERROR("GetConnection Error");
        break;
      }

      std::stringstream sql_lan;
      sql_lan << "call proc_QueryStockSubscribe(" << user_id << ");";
      std::string sql_str = sql_lan.str();
      ret = engine->SQLExec(sql_str.c_str());
      if (false == ret) {
        LOG_ERROR("call function query_industry() is failure, "\
            "errorkey:exec sql error");
        break;
      }

      int result_num = 0;
      result_num = engine->RecordCount();
      LOG_DEBUG2("call function query_industry():"\
          "The query num is [%d]", result_num);
      if (result_num < 0) {
        break;
      }

      std::map<std::string, std::string>::iterator itr = name->end();
      MYSQL_ROW rows;
      for (int i = 0; i < result_num; i++) {
        rows = *(reinterpret_cast<MYSQL_ROW*>(engine->FetchRows()->proc));
        scoped_ptr<base_logic::DictionaryValue> stock(\
            new base_logic::DictionaryValue());
        scoped_ptr<base_logic::StringValue> stock_code(\
            new base_logic::StringValue(rows[0]));
        if(name->find(rows[0]) != itr) {
          scoped_ptr<base_logic::StringValue> stock_name(\
              new base_logic::StringValue((*name)[rows[0]]));
          stock->Set("stock_name", stock_name.release());
        }
        stock->Set("stock_code", stock_code.release());
        all_subscribe->add_stock(stock.release());
      }

      ret = true;
    }while(0);

    return ret;
  }

  bool DbSql::QueryIndustrySubscribe(int64 user_id, netcomm_send::SendAllSubscribe* all_subscribe) {
    bool ret = false;

    do {
      base_db::AutoMysqlCommEngine auto_engine;
      base_storage::DBStorageEngine* engine = auto_engine.GetDBEngine();
      if (NULL == engine) {
        LOG_ERROR("GetConnection Error");
        break;
      }

      std::stringstream sql_lan;
      sql_lan << "call proc_QueryIndustrySubscribe(" << user_id << ");";
      std::string sql_str = sql_lan.str();
      ret = engine->SQLExec(sql_str.c_str());
      if (false == ret) {
        LOG_ERROR("call function query_industry() is failure, "\
            "errorkey:exec sql error");
        break;
      }

      int result_num = 0;
      result_num = engine->RecordCount();
      LOG_DEBUG2("call function query_industry():"\
          "The query num is [%d]", result_num);
      if (result_num < 0) {
        break;
      }

      MYSQL_ROW rows;
      for (int i = 0; i < result_num; i++) {
        rows = *(reinterpret_cast<MYSQL_ROW*>(engine->FetchRows()->proc));
        scoped_ptr<base_logic::DictionaryValue> industry(\
            new base_logic::DictionaryValue());
        scoped_ptr<base_logic::StringValue> industry_name(\
            new base_logic::StringValue(rows[0]));
        industry->Set("industry_name", industry_name.release());
        all_subscribe->add_industry(industry.release());
      }

      ret = true;
    }while(0);

    return ret;
  }

}  // namespace subscribesvc


