//
// Created by wendongli on 2/19/16.
//

#include "garbage_collector.h"

#include <chrono>
#include <thread>

namespace peloton {
  namespace index {
    void *Begin(void *arg) {
      GarbageCollector *gc = static_cast<GarbageCollector *>(arg);
      while (!(gc->stopped_)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(GarbageCollector::epoch_interval_));
        gc->head_ = new Epoch((gc->timer_++), gc->head_);
        gc->ReclaimGarbage();
      }
      return nullptr;
    }
    void GarbageCollector::ReclaimGarbage() {
      // reclaim at most this number of epochs every time
      static const int max_number = 5;
      Epoch *prev = head_->next_;
      Epoch *now = prev->next_;
      int i = 0;
      while(now!=nullptr&&i<max_number) {
        if(now->SafeToReclaim()) {
          ++i;
          prev->SetNext(now->next_);
          delete now;
          now = prev->next_;
        }
        else {
          prev = now;
          now = prev->next_;
        }
      }
    }
  }
}