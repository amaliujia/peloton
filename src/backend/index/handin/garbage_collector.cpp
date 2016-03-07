//
// Created by wendongli on 2/19/16.
//

#include "bwtree.h"

#include <chrono>
#include <thread>

namespace peloton {
  namespace index {
    GarbageCollector GarbageCollector::global_gc_;

    void *Begin(void *arg) {
      LOG_DEBUG("GarbageCollector::Begin()");
      GarbageCollector *gc = static_cast<GarbageCollector *>(arg);
      while (!(gc->stopped_)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(GarbageCollector::epoch_interval_));
        gc->head_ = new Epoch((gc->timer_)++, gc->head_);
        gc->ReclaimGarbage();
      }
      LOG_DEBUG("GarbageCollector::Begin() Stopped");
      return NULL;
    }

    const int GarbageCollector::epoch_interval_ = 10; //ms

    void GarbageCollector::ReclaimGarbage() {
      //LOG_DEBUG("GarbageCollector::ReclaimGarbage()");
      assert(head_!=nullptr&&head_->next_!=nullptr);
      if(last_stopped_prev_==nullptr)
        last_stopped_prev_ = head_->next_;

      // check if all the epochs beyond last_stopped_prev are safe to delete
      for(Epoch *p = last_stopped_prev_->next_;
          p!=nullptr; p = p->next_)
        if(!(p->SafeToReclaim()))
          return ;
      //LOG_DEBUG("GarbageCollector::ReclaimGarbage() safe to reclaim");
      // if so, we delete all the epochs beyond prev
      ReclaimGarbageList(last_stopped_prev_->next_);
      last_stopped_prev_->next_ = nullptr;
      last_stopped_prev_ = nullptr;
    }

    void GarbageCollector::ReclaimGarbageList(Epoch *head) {
      Epoch *next;
      while(head!=nullptr) {
        assert(head->SafeToReclaim());
        next = head->next_;
        delete head;
        head = next;
      }
    }
  }
}