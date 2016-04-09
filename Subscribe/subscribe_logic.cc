//  Copyright (c) 2015-2015 The subscribe Authors. All rights reserved.
//  Created on: 2015/12/16 Author: Sunsolo

#include "subscribe/subscribe_logic.h"
#include <stdlib.h>
#include <string>

#include <sstream>

#include "basic/basic_util.h"
#include "config/config.h"
#include "logic/logic_unit.h"

#include "subscribe/db_comm.h"
#include "subscribe/redis_comm.h"
namespace subscribe_logic {

  Subscribelogic* Subscribelogic::instance_ = NULL;

  std::map<std::string, std::string>* Subscribelogic::Stock_Code_Name_ = NULL;

  Subscribelogic::Subscribelogic() {
    if (!Init()){
      if(NULL != Stock_Code_Name_) {
        delete Stock_Code_Name_;
        Stock_Code_Name_ = NULL;
      }

      assert(0);
    }
  }

  Subscribelogic::~Subscribelogic() {
    subscribesvc::DbSql::Dest();
    subscribesvc::DbRedis::Dest();

    if(NULL != Stock_Code_Name_) {
      delete Stock_Code_Name_;
      Stock_Code_Name_ = NULL;
    }
  }

  bool Subscribelogic::Init() {
    bool ret = false;

    do {
      config::FileConfig* config = config::FileConfig::GetFileConfig();
      if (NULL == config) {
        LOG_ERROR("Subscribelogic class initial fail, "\
            "errorkey:GetFileConfig except");
        break;
      }

      std::string path = DEFAULT_CONFIG_PATH;
      ret = config->LoadConfig(path);
      if (false == ret) {
        LOG_ERROR("Subscribelogic initial fail, errorkey:LoadConfig except");
        break;
      }

      subscribesvc::DbSql::Init(&(config->mysql_db_list_));
      subscribesvc::DbRedis::Init(&config->redis_list_);

      Stock_Code_Name_ = new std::map<std::string, std::string>();
      if(NULL == Stock_Code_Name_) {
        break;
      }
      ret = subscribesvc::DbSql::QueryStockCodeName(*Stock_Code_Name_);
    }while (0);
    return ret;
  }

  Subscribelogic* Subscribelogic::GetInstance() {
    if (NULL == instance_)
      instance_ = new Subscribelogic();

    return instance_;
  }


  void Subscribelogic::FreeInstance() {
    if (NULL != instance_) {
      delete instance_;
    }
    instance_ = NULL;
  }

  bool Subscribelogic::OnSubscribeConnect(struct server *srv, \
      const int socket) {
    return true;
  }


  bool Subscribelogic::OnSubscribeMessage(struct server *srv, \
      const int socket,                                       \
      const void *msg,                                        \
      const int len) {
    bool ret = false;

    do {
      const char* packet_stream = (const char*)(msg);
      if (NULL == packet_stream) {
        // LOG_ERROR();==>TODO
        break;
      }
      std::string http_str(packet_stream, len);

      std::string error_str;
      int error_code = 0;
      scoped_ptr<base_logic::ValueSerializer> serializer(\
          base_logic::ValueSerializer::Create(\
            base_logic::IMPL_HTTP, &http_str));
      NetBase* value = (NetBase*)(\
          serializer.get()->Deserialize(&error_code, &error_str));

      if (NULL == value) {
        error_code = STRUCT_ERROR;
        send_error(error_code, socket);
        ret = true;
        break;
      }

      scoped_ptr<RecvPacketBase> packet(\
          new RecvPacketBase(value));
      int32 type = packet->GetType();
      switch (type) {
        case USER_SUBSCRIBE:
          onAddTimeEmail(srv, socket, value);
          break;
        case QUERY_SECTION:
          OnQuerySection(srv, socket, value);
          break;
        case QUERY_INDUSTRY:
          OnQueryIndustry(srv, socket, value);
          break;
        case QUERY_STOCK:
          OnQueryStock(srv, socket, value);
          break;
        case DELETE_SUBSCRIBE:
          OnDeleteSubscribe(srv, socket, value);
          break;
        case QUERY_SUBSCRIBE:
          OnQuerySubscribe(srv, socket, value);
          break;
        default:
          return false;
      }
      ret = true;
    }while(0);

    return ret;
  }

  bool Subscribelogic::OnSubscribeClose(struct server *srv, const int socket) {
    return true;
  }


  bool Subscribelogic::OnBroadcastConnect(struct server *srv, \
      const int socket,                                       \
      const void *msg,                                        \
      const int len) {
    return true;
  }

  bool Subscribelogic::OnBroadcastMessage(struct server *srv, \
      const int socket,                                       \
      const void *msg,                                        \
      const int len) {
    return true;
  }


  bool Subscribelogic::OnBroadcastClose(struct server *srv, const int socket) {
    return true;
  }

  bool Subscribelogic::OnIniTimer(struct server *srv) {
    return true;
  }


  bool Subscribelogic::OnTimeout(struct server *srv, char *id, \
      int opcode, int time) {
    return true;
  }

  bool Subscribelogic::OnQuerySection(struct server* srv, const int socket, \
      NetBase* netbase, const void* msg , const int len ) {
    bool ret = false;

    do {
      scoped_ptr<netcomm_recv::RecvQuery> recv_section(\
          new netcomm_recv::RecvQuery(netbase));
      std::string jsonp_str = recv_section->GetJsonp();
      int json_type = ("" == jsonp_str)? 0 : 3;
      int error_code = recv_section->GetResult();
      if (error_code != 0) {
        send_error(error_code, socket, json_type, jsonp_str);
        break;
      }

      scoped_ptr<netcomm_send::SendSection> all_section(\
          new netcomm_send::SendSection());
      ret = subscribesvc::DbSql::QuerySection(recv_section->user_id(), all_section.get());
      if (true == ret) {
        if (3 == json_type) {
          all_section->set_jsonp_callback(jsonp_str);
        }

        send_message(socket, (\
              SendPacketBase*)all_section.get(), json_type);
      } else {
        send_error(QUERY_DB_FAILED, socket, json_type, jsonp_str);
      }
    }while(0);

    return ret;
  }

  bool Subscribelogic::OnQueryIndustry(struct server* srv, const int socket, \
      NetBase* netbase, const void* msg , const int len ) {
    bool ret = false;

    do {
      scoped_ptr<netcomm_recv::RecvQuery> recv_industry(\
          new netcomm_recv::RecvQuery(netbase));
      std::string jsonp_str = recv_industry->GetJsonp();
      int json_type = ("" == jsonp_str)? 0 : 3;
      int error_code = recv_industry->GetResult();
      if (error_code != 0) {
        send_error(error_code, socket, json_type, jsonp_str);
        break;
      }

      scoped_ptr<netcomm_send::SendIndustry> all_industry(\
          new netcomm_send::SendIndustry());
      ret = subscribesvc::DbSql::QueryIndustry(recv_industry->user_id(), all_industry.get());
      if (true == ret) {
        if (3 == json_type) {
          all_industry->set_jsonp_callback(jsonp_str);
        }
        send_message(socket, (\
              SendPacketBase*)all_industry.get(), json_type);

      } else {
        send_error(QUERY_DB_FAILED, socket, json_type, jsonp_str);
      }
    }while(0);
    return ret;
  }

  bool Subscribelogic::OnQueryStock(struct server* srv, const int socket, \
      NetBase* netbase, const void* msg , const int len ) {
    bool ret = false;

    do {
      scoped_ptr<netcomm_recv::RecvQuery> recv_stock(\
          new netcomm_recv::RecvQuery(netbase));
      std::string jsonp_str = recv_stock->GetJsonp();
      int json_type = ("" == jsonp_str)? 0 : 3;
      int error_code = recv_stock->GetResult();
      if (error_code != 0) {
        send_error(error_code, socket, json_type, jsonp_str);
        break;
      }

      scoped_ptr<netcomm_send::SendStock> all_stock(\
          new netcomm_send::SendStock());

      std::map<std::string, std::string> top_stock;
      std::string stock_str = "";

      ret = subscribesvc::DbRedis::GetStockInfo("-13", "-1", 0, top_stock);
      if(false == ret) {
        send_error(QUERY_REDIS_FAILED, socket, json_type, jsonp_str);
        break;
      }
      std::map<std::string, std::string>::iterator itr = top_stock.begin();
      for(; itr != top_stock.end(); itr++) {
        if(stock_str == "") {
          stock_str = stock_str + itr->first;
        } else {
          stock_str = stock_str + "," + itr->first;
        }
      }

      ret = subscribesvc::DbSql::QueryStock(recv_stock->user_id(), &stock_str, all_stock.get());
      if (true == ret) {
        if (3 == json_type) {
          all_stock->set_jsonp_callback(jsonp_str);
        }
        send_message(socket, (\
              SendPacketBase*)all_stock.get(), json_type);

      } else {
        send_error(QUERY_DB_FAILED, socket, json_type, jsonp_str);
      }
    }while(0);
    return ret;
  }

  bool Subscribelogic::onAddTimeEmail(struct server* srv, const int socket, \
      NetBase* netbase, const void* msg, const int len) {
    bool ret = false;

    do {
      scoped_ptr<netcomm_recv::RecvTimeEmail> recv_time_email(\
          new netcomm_recv::RecvTimeEmail(netbase));
      std::string jsonp_str = recv_time_email->GetJsonp();
      int json_type = ("" == jsonp_str)? 0 : 3;
      int error_code = recv_time_email->GetResult();
      if (error_code != 0) {
        send_error(error_code, socket, json_type, jsonp_str);
        break;
      }

      ret = subscribesvc::DbSql::AddTimeEmail(recv_time_email->user_id(), \
          (recv_time_email->start_time()), \
          (recv_time_email->end_time()),   \
          (recv_time_email->time_inval()),  \
          &(recv_time_email->stock_code()), \
          &(recv_time_email->section()),    \
          &(recv_time_email->industry()));

      scoped_ptr<netcomm_send::SendSubscribe> send_subscribe(\
          new netcomm_send::SendSubscribe());
      send_subscribe->set_result(ret);

      if (true == ret) {
        if (3 == json_type) {
          send_subscribe->set_jsonp_callback(jsonp_str);
        }
        send_message(socket, (SendPacketBase*)(\
              send_subscribe.get()), json_type);

      } else {
        send_error(SUBSCRIBE_DATA_IN_DB, socket, json_type, jsonp_str);
      }
    }while(0);

    return ret;
  }

  bool Subscribelogic::OnDeleteSubscribe(struct server* srv, const int socket, \
      NetBase* netbase, const void* msg, const int len) {
    bool ret = false;

    do {
      scoped_ptr<netcomm_recv::RecvAddSubscribe> delete_subscribe(\
          new netcomm_recv::RecvAddSubscribe(netbase));
      std::string jsonp_str = delete_subscribe->GetJsonp();
      int json_type = ("" == jsonp_str)? 0 : 3;
      int error_code = delete_subscribe->GetResult();
      if (error_code != 0) {
        send_error(error_code, socket, json_type, jsonp_str);
        break;
      }

      ret = subscribesvc::DbSql::DeleteSubscribe(delete_subscribe->user_id(), \
          &(delete_subscribe->stock_code()), \
          &(delete_subscribe->section()), \
          &(delete_subscribe->industry()));

      scoped_ptr<netcomm_send::SendSubscribe> send_subscribe(\
          new netcomm_send::SendSubscribe());
      send_subscribe->set_result(ret);

      if (true == ret) {
        if (3 == json_type) {
          send_subscribe->set_jsonp_callback(jsonp_str);
        }
        send_message(socket, (SendPacketBase*)(\
              send_subscribe.get()), json_type);

      } else {
        send_error(SUBSCRIBE_DATA_IN_DB, socket, json_type, jsonp_str);
      }
    }while(0);

    return ret;
  }

  bool Subscribelogic::OnQuerySubscribe(struct server* srv, const int socket, \
      NetBase* netbase, const void* msg, const int len) {
    bool ret = false;

    do {
      scoped_ptr<netcomm_recv::RecvQuery> query_subscribe(\
          new netcomm_recv::RecvQuery(netbase));
      std::string jsonp_str = query_subscribe->GetJsonp();
      int json_type = ("" == jsonp_str)? 0 : 3;
      int error_code = query_subscribe->GetResult();
      if (error_code != 0) {
        send_error(error_code, socket, json_type, jsonp_str);
        break;
      }

      scoped_ptr<netcomm_send::SendAllSubscribe> all_subscribe(\
          new netcomm_send::SendAllSubscribe());

      ret = subscribesvc::DbSql::QuerySubscribe(query_subscribe->user_id(), all_subscribe.get(), Stock_Code_Name_);
      all_subscribe->set_result(ret);
      if (true == ret) {
        if (3 == json_type) {
          all_subscribe->set_jsonp_callback(jsonp_str);
        }
        send_message(socket, (\
              SendPacketBase*)all_subscribe.get(), json_type);

      } else {
        send_error(QUERY_DB_FAILED, socket, json_type, jsonp_str);
      }
    }while(0);
    return ret;
  }

}  // namespace subscribe_logic

