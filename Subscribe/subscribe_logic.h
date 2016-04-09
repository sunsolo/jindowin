//  Copyright (c) 2015-2015 The subscribe Authors. All rights reserved.
//  Created on: 2015/12/16 Author: Sunsolo

#ifndef __SUBSCRIBE__SUBSCRIBE__SUBSCRIBE_LOGIC___
#define __SUBSCRIBE__SUBSCRIBE__SUBSCRIBE_LOGIC___
#include "core/common.h"
#include "net/subscribe_comm.h"

#define DEFAULT_CONFIG_PATH     "./plugins/subscribe/subscribe_config.xml"
namespace subscribe_logic {

class Subscribelogic {
 public:
  Subscribelogic();
  virtual ~Subscribelogic();

  static Subscribelogic *GetInstance();
  static void FreeInstance();

  bool OnSubscribeConnect(struct server *srv, const int socket);
  bool OnSubscribeMessage(struct server *srv, const int socket, \
      const void *msg, const int len);
  bool OnSubscribeClose(struct server *srv, const int socket);
  bool OnBroadcastConnect(struct server *srv, const int socket, \
      const void *data, const int len);
  bool OnBroadcastMessage(struct server *srv, const int socket, \
      const void *msg, const int len);
  bool OnBroadcastClose(struct server *srv, const int socket);
  bool OnIniTimer(struct server *srv);
  bool OnTimeout(struct server *srv, char* id, int opcode, int time);

 private:
  bool Init();

  /**
   * @synopsis      查询所有版块接口
   *
   * @param srv
   * @param socket
   * @param netbase
   * @param msg
   * @param len
   *
   * @returns   
   */
  bool OnQuerySection(struct server* srv, \
      const int socket,                   \
      NetBase* netbase,     \
      const void* msg = NULL,             \
      const int len = 0);

  /**
   * @synopsis      查询所有行业接口
   *
   * @param srv
   * @param socket
   * @param netbase
   * @param msg
   * @param len
   *
   * @returns   
   */
  bool OnQueryIndustry(struct server* srv, \
      const int socket,                    \
      NetBase* netbase,      \
      const void* msg = NULL,              \
      const int len = 0);

  /**
   * @synopsis      查询股票top20订阅信息接口
   *
   * @param srv
   * @param socket
   * @param netbase
   * @param msg
   * @param len
   *
   * @returns   
   */
  bool OnQueryStock(struct server* srv, \
      const int socket,                    \
      NetBase* netbase,      \
      const void* msg = NULL,              \
      const int len = 0);

  bool onAddTimeEmail(struct server* srv, \
      const int socket,                    \
      NetBase* netbase,      \
      const void* msg = NULL,              \
      const int len = 0);

  bool OnDeleteSubscribe(struct server* srv, \
      const int socket,                   \
      NetBase* netbase,                   \
      const void* msg = NULL,             \
      const int len = 0);

  bool OnQuerySubscribe(struct server* srv, \
      const int socket,                   \
      NetBase* netbase,                   \
      const void* msg = NULL,             \
      const int len = 0);
  static Subscribelogic *instance_;

  static std::map<std::string, std::string> *Stock_Code_Name_;
};

}  // namespace subscribe_logic

#endif

