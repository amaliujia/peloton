//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_server.h
//
// Identification: src/backend/message/rpc_server.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <thread>
#include <map>

#include "backend/networking/nanomsg.h"
#include "backend/networking/message_queue.h"
#include "backend/networking/rpc_method.h"

namespace peloton {
namespace networking {

class RpcServer {

  typedef std::map<uint64_t, RpcMethod*> RpcMethodMap;
  typedef struct RecvItem {
    NanoMsg*                    socket;
    RpcMethod* 					method;
    google::protobuf::Message* 	request;
  } QueueItem;

public:
  RpcServer(const char* url);
  ~RpcServer();

  // add more endpoints
  void EndPoint(const char* url);

  // start
  void Start();
  void StartSimple();

  // Multiple woker threads
  void Worker(const char* debuginfo);

//  std::thread WorkerThread(const char* debuginfo) {
//    return std::thread([=] { Worker(debuginfo); });
//  }

  // register a service
  void RegisterService(google::protobuf::Service *service);

  // remove all services
  void RemoveService();

  // close
  void Close();

private:

  NanoMsg        socket_;
  int            socket_id_;
  RpcMethodMap   rpc_method_map_;

  std::thread    worker_thread_;
  MessageQueue<RecvItem>   recv_queue_;
};

}  // namespace networking
}  // namespace peloton
