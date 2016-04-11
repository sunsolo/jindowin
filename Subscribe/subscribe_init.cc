//  Copyright (c) 2015-2015 The subscribe Authors. All rights reserved.
//  Created on: 2015/12/16 Author: Sunsolo

#include "subscribe/subscribe_init.h"
#include <signal.h>

#include "core/common.h"
#include "core/plugins.h"

#include "subscribe/subscribe_logic.h"

struct subscribeplugin {
  char* id;
  char* name;
  char* version;
  char* provider;
};

static void *OnSubscribeStart() {
  signal(SIGPIPE, SIG_IGN);

  struct subscribeplugin* subscribe = (struct subscribeplugin*)calloc(\
      1, sizeof(struct subscribeplugin));

  subscribe->id = "subscribe";
  subscribe->name = "subscribe";
  subscribe->version = "1.0.0";
  subscribe->provider = "wukun";

  if (!subscribe_logic::Subscribelogic::GetInstance())
    assert(0);

  return subscribe;
}

static handler_t OnSubscribeShutdown(struct server* srv, void* pd) {
  subscribe_logic::Subscribelogic::FreeInstance();

  return HANDLER_GO_ON;
}

static handler_t OnSubscribeConnect(struct server *srv, \
    int fd,                                             \
    void *data,                                         \
    int len) {
  subscribe_logic::Subscribelogic::GetInstance()->OnSubscribeConnect(srv, fd);

  return HANDLER_GO_ON;
}

static handler_t OnSubscribeMessage(struct server *srv, \
    int fd,                                             \
    void *data,                                         \
    int len) {
  bool ret = false;

  ret = subscribe_logic::Subscribelogic::GetInstance()->OnSubscribeMessage(\
      srv, fd, data, len);

  if (false == ret) {
    return HANDLER_GO_ON;
  } else {
    return HANDLER_FINISHED;
  }
}

static handler_t OnSubscribeClose(struct server *srv, int fd) {
  subscribe_logic::Subscribelogic::GetInstance()->OnSubscribeClose(srv, fd);

  return HANDLER_GO_ON;
}

static handler_t OnUnknow(struct server *srv, int fd, void *data, int len) {
  return HANDLER_GO_ON;
}

static handler_t OnBroadcastConnect(struct server* srv, \
    int fd,                                             \
    void *data,                                         \
    int len) {
  subscribe_logic::Subscribelogic::GetInstance()->OnBroadcastConnect(\
      srv, fd, data, len);

  return HANDLER_GO_ON;
}

static handler_t OnBroadcastClose(struct server* srv, int fd) {
  subscribe_logic::Subscribelogic::GetInstance()->OnBroadcastClose(srv, fd);

  return HANDLER_GO_ON;
}

static handler_t OnBroadcastMessage(struct server* srv, \
    int fd,                                             \
    void *data,                                         \
    int len) {
  subscribe_logic::Subscribelogic::GetInstance()->OnBroadcastMessage(\
      srv, fd, data, len);

  return HANDLER_GO_ON;
}

static handler_t OnIniTimer(struct server* srv) {
  subscribe_logic::Subscribelogic::GetInstance()->OnIniTimer(srv);

  return HANDLER_GO_ON;
}

static handler_t OnTimeOut(struct server* srv, char* id, int opcode, int time) {
  subscribe_logic::Subscribelogic::GetInstance()->OnTimeout(\
      srv, id, opcode, time);

  return HANDLER_GO_ON;
}

int subscribe_plugin_init(struct plugin *pl) {
  pl->init = OnSubscribeStart;
  pl->clean_up = OnSubscribeShutdown;
  pl->connection = OnSubscribeConnect;
  pl->connection_close = OnSubscribeClose;
  pl->connection_close_srv = OnBroadcastClose;
  pl->connection_srv = OnBroadcastConnect;
  pl->handler_init_time = OnIniTimer;
  pl->handler_read = OnSubscribeMessage;
  pl->handler_read_srv = OnBroadcastMessage;
  pl->handler_read_other = OnUnknow;
  pl->time_msg = OnTimeOut;
  pl->data = NULL;

  return 0;
}



