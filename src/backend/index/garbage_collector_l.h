//
// Created by wendongli on 2/19/16.
//

#pragma once

#include <cstdint>

#include "bwtree.h"

namespace peloton {
  namespace index {


    class GarBageCollector {
      /*
       * This is an epoch-based garbage collector
       * The epochs are chained together as a latch-free singly-linked list
       * The garbage in a specific epoch is also chained together as a latch-free singly-linked list
       */
    public:
      typedef std::uint_fast32_t EpochTime;

      // singleton
      GarBageCollector global_gc_;

      inline void SubmitGarbage(BWNode *garbage) {
        head_->SubmitGarbage(garbage);
      }
      inline EpochTime Register() {
        return head_->Register();
      }
      inline void Deregister(EpochTime time) {
        for(Epoch *iter = head_; iter!=nullptr; iter = iter->Next()) {
          if(iter->TimeEquals(time)) {
            iter->Deregister();
            return ;
          }
        }
        // should not happen
        assert(0);
      }
      void Start();
      void End();
      ~GarBageCollector();

    private:
      Epoch *head_;
      std::atomic<EpochTime> timer;

    private:
      class Epoch {
        /*
         * Class representing an epoch
         */
      public:
        Epoch(EpochTime t, Epoch *n): head_(nullptr), registered_number_(0), epoch_time_(t) {}
        ~Epoch();
        void SubmitGarbage(BWNode *g) {
          GarbageNode *head_now = head_;
          GarbageNode *new_garbage = new GarbageNode(g, head_now);
          // loop until we successfully add this new garbage into the list
          while(!__sync_bool_compare_and_swap(&head_, head_now, new_garbage)) {
            head_now = head_;
            new_garbage->SetNext(head_now);
          }
        }
        inline EpochTime Register() {
          ++registered_number_;
          return epoch_time_;
        }
        inline void Deregister() { --registered_number_; }
        inline bool TimeEquals(const EpochTime t) const { return epoch_time_==t; }
        inline Epoch *Next() const { return next_; }
        inline void SetNext(Epoch *n) { next_ = n; }
      private:
        class GarbageNode {
          /*
           * Wrap a piece of garbage into a list node
           */
        public:
          GarbageNode(BWNode *g, GarbageNode *n): garbage_(g), next_(n) { }
          ~GarbageNode() {
            // don't want to recursively delete it
            /*delete garbage; delete next;*/
          }
          inline void SetNext(GarbageNode *n) { next_ = n; }
        private:
          GarbageNode(const GarbageNode &) = delete;
          GarbageNode &operator=(const GarbageNode &) = delete;
        private:
          BWNode *garbage_;
          GarbageNode *next_;
        };
        GarbageNode *head_ = nullptr;
        std::atomic_uint_fast32_t registered_number_ = 0;
        const EpochTime epoch_time_;
        Epoch *next_;

      };
      GarBageCollector(): head_(nullptr), timer(0) {
        head_ = new Epoch(timer);
      }
      GarBageCollector(const GarBageCollector &) = delete;
      GarBageCollector &operator=(const GarBageCollector &) = delete;

    };
  }
}
