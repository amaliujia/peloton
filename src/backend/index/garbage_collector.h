//
// Created by wendongli on 2/19/16.
//

#pragma once

#include <cstdint>
#include <pthread.h>

#include "bwtree.h"

namespace peloton {
  namespace index {
    typedef std::uint_fast32_t EpochTime;

    class GarbageNode {
      /*
       * Wrap a piece of garbage into a list node
       */
    public:
      GarbageNode(const BWNode *g, GarbageNode *next): next_(next), garbage_(g) { }
      ~GarbageNode() {
        const BWNode *head = garbage_;
        assert(head!=nullptr);
        while(head!=nullptr) {
          const BWNode *next = head->GetNext();
          delete head;
          head = next;
        }
      }
      // next garbage node in this garbage list
      GarbageNode *next_;
      inline void SetNext(GarbageNode *next) { next_ = next; }
    private:
      GarbageNode(const GarbageNode &) = delete;
      GarbageNode &operator=(const GarbageNode &) = delete;
      // the actual garbage bwnode
      const BWNode *garbage_;
    };

    class PIDNode {
      /*
       * Wrap a PID into a list node
       */
    public:
      PIDNode(PID pid, PIDNode *next): next_(next), pid_(pid) {}
      ~PIDNode() { /*(PIDTable::get_table()).free_PID(pid_);*/ assert(0); }
      // next PIDNode in the list
      PIDNode *next_;
      inline void SetNext(PIDNode *next) { next_ = next; }
    private:
      PIDNode(const PIDNode &) = delete;
      PIDNode &operator=(const PIDNode &) = delete;
      // actual PID needed to be reclaimed
      const PID pid_;
    };

    class Epoch {
      /*
       * Class representing an epoch
       */
    public:
      Epoch(EpochTime time, Epoch *next): next_(next), registered_number_(0), epoch_time_(time) {}

      ~Epoch() {
        assert(SafeToReclaim());
        // delete all garbage nodes, which will
        // delete the actual garbage in their destructor
        const GarbageNode *now = head_;
        const GarbageNode *next;
        while(now!=nullptr) {
          next = now->next_;
          delete now;
          now = next;
        }
        // delete all PID garbage nodes, which will
        // reclaim the actual PID in their destructor
        const PIDNode *pid_now = pid_head_;
        const PIDNode *pid_next;
        while(pid_now!=nullptr) {
          pid_next = pid_now->next_;
          delete pid_now;
          pid_now = pid_next;
        }
      }

      // try to atomically submit a garbage node, return the submission result
      inline bool SubmitGarbage(GarbageNode *new_garbage) {
        GarbageNode *head = head_;
        new_garbage->SetNext(head);
        return __sync_bool_compare_and_swap(&head_, head, new_garbage);
      }

      // try to atomically submit a PID garbage node, return the submission result
      inline bool SubmitPID(PIDNode *new_pid) {
        PIDNode *head = pid_head_;
        new_pid->SetNext(head);
        return __sync_bool_compare_and_swap(&pid_head_, head, new_pid);
      }

      // register in this epoch
      inline EpochTime Register() {
        ++registered_number_;
        return epoch_time_;
      }

      // deregister a previous registration
      inline void Deregister() { --registered_number_; }

      inline bool TimeEquals(const EpochTime t) const { return epoch_time_==t; }

      inline void SetNext(Epoch *n) { next_ = n; }

      // whether it is safe to reclaim all the garbage in this epoch
      inline bool SafeToReclaim() const { return registered_number_==0; }

      Epoch *next_;

    private:
      GarbageNode *head_ = nullptr;
      PIDNode *pid_head_ = nullptr;
      std::atomic<uint_fast32_t> registered_number_;
      const EpochTime epoch_time_;
    };

    void *Begin(void *arg);

    class GarbageCollector {
      /*
       * This is an epoch-based garbage collector
       * The epochs are chained together as a latch-free singly-linked list
       * The garbage in a specific epoch is also chained together as a latch-free singly-linked list
       */
    public:
      // daemon gc thread
      friend void *Begin(void *arg);
      static const int epoch_interval_; //ms
      // singleton
      static GarbageCollector global_gc_;

      virtual ~GarbageCollector() {
        // stop epoch generation
        Stop();
        // TODO wait till all epochs are ready to be cleaned-up
        // force delete every garbage now
        Epoch *next;
        while(head_!=nullptr) {
          assert(head_->SafeToReclaim());
          next = head_->next_;
          delete head_;
          head_ = next;
        }
      }

      // submit a new bwnode garbage
      inline void SubmitGarbage(const BWNode *garbage) {
        GarbageNode *new_garbage = new GarbageNode(garbage, nullptr);
        // loop until we successfully add this new garbage into the list
        Epoch * volatile head = head_;
        while(!head->SubmitGarbage(new_garbage))
          head = head_;
      }

      // submit a new garbage PID
      inline void SubmitPID(PID pid) {
        PIDNode *new_pid_garbage = new PIDNode(pid, nullptr);
        // loop until we successfully add this new pid garbage into the list
        Epoch * volatile head = head_;
        while(!head->SubmitPID(new_pid_garbage))
          head = head_;
      }

      // register a thread, return the EpochTime that it registered in
      inline EpochTime Register() { return head_->Register(); }

      // deregister a previous registration at "time"
      inline void Deregister(EpochTime time) {
        for(Epoch * volatile iter = head_; iter!=nullptr; iter = iter->next_) {
          if(iter->TimeEquals(time)) {
            iter->Deregister();
            return ;
          }
        }
        // should not happen
        assert(0);
      }

    private:
      Epoch * volatile head_;
      volatile EpochTime timer_;
      // the Epoch at which we stopped at in the last garbage collection iteration
      Epoch *last_stopped_prev_;
      volatile bool stopped_ = false;
      // daemon thread to do garbage collection, timer incrementation
      pthread_t clean_thread_;

      GarbageCollector(): head_(nullptr), timer_(0), last_stopped_prev_(nullptr) {
        // make sure there is at least one epoch
        head_ = new Epoch(timer_++, head_);
        // start epoch allocation thread
        pthread_create(&clean_thread_, NULL, &Begin, this);

      }
      GarbageCollector(const GarbageCollector &) = delete;
      GarbageCollector &operator=(const GarbageCollector &) = delete;

      //void *Begin(void *);
      void ReclaimGarbage();
      void Stop() {
        LOG_DEBUG("GarbageCollector::Stop()");
        stopped_ = true;
        pthread_join(clean_thread_, NULL);
      }

      // reclaim all the epochs in the list starts at "head"
      static void ReclaimGarbageList(Epoch *head);
    };
  }
}
