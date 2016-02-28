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
  GarbageNode(BWNode *g, GarbageNode *n) : garbage_(g), next_(n) {}
  ~GarbageNode() { delete garbage_; }
  const BWNode *garbage_;
  GarbageNode *next_;
  inline void SetNext(GarbageNode *n) { next_ = n; }

 private:
  GarbageNode(const GarbageNode &) = delete;
  GarbageNode &operator=(const GarbageNode &) = delete;
};

class Epoch {
  /*
   * Class representing an epoch
   */
 public:
  Epoch(EpochTime t, Epoch *next)
      : next_(next), head_(nullptr), registered_number_(0), epoch_time_(t) {}
  ~Epoch() {
    const GarbageNode *now = head_;
    if (now == nullptr) return;
    const GarbageNode *next;
    while (now != nullptr) {
      next = now->next_;
      delete now;
      now = next;
    }
  }
  inline bool SubmitGarbage(GarbageNode *new_garbage) {
    GarbageNode *head = head_;
    new_garbage->SetNext(head);
    return __sync_bool_compare_and_swap(&head_, head, new_garbage);
  }
  inline EpochTime Register() {
    ++registered_number_;
    return epoch_time_;
  }
  inline void Deregister() { --registered_number_; }
  inline bool TimeEquals(const EpochTime t) const { return epoch_time_ == t; }
  inline void SetNext(Epoch *n) { next_ = n; }
  inline bool SafeToReclaim() const { return registered_number_ == 0; }
  Epoch *next_;

 private:
  GarbageNode *head_ = nullptr;
  std::atomic<EpochTime> registered_number_;
  const EpochTime epoch_time_;
};

void *Begin(void *arg);

class GarbageCollector {
  /*
   * This is an epoch-based garbage collector
   * The epochs are chained together as a latch-free singly-linked list
   * The garbage in a specific epoch is also chained together as a latch-free
   * singly-linked list
   */
 public:
  // daemon gc thread
  friend void *Begin(void *arg);
  static const int epoch_interval_ = 10;  // ms
  // singleton
  static GarbageCollector global_gc_;

  virtual ~GarbageCollector() {
    // stop epoch generation
    Stop();
    // TODO wait till all epochs are ready to be cleaned-up
    // force delete every garbage now
    Epoch *next;
    while (head_ != nullptr) {
      next = head_->next_;
      delete head_;
      head_ = next;
    }
  }
  inline void SubmitGarbage(BWNode *garbage) {
    GarbageNode *new_garbage = new GarbageNode(garbage, nullptr);
    // loop until we successfully add this new garbage into the list
    Epoch *head = head_;
    while (!head->SubmitGarbage(new_garbage)) head = head_;
  }
  inline EpochTime Register() { return head_->Register(); }
  inline void Deregister(EpochTime time) {
    for (Epoch *iter = head_; iter != nullptr; iter = iter->next_) {
      if (iter->TimeEquals(time)) {
        iter->Deregister();
        return;
      }
    }
    // should not happen
    assert(0);
  }

 private:
  Epoch *head_;
  EpochTime timer_;
  volatile bool stopped_;
  pthread_t clean_thread_;

  GarbageCollector() : head_(nullptr), timer_(0) {
    // make sure there is at least one epoch
    head_ = new Epoch(timer_++, head_);
    // start epoch allocation thread
    pthread_create(&clean_thread_, NULL, &Begin, NULL);
  }
  GarbageCollector(const GarbageCollector &) = delete;
  GarbageCollector &operator=(const GarbageCollector &) = delete;

  // void *Begin(void *);
  void ReclaimGarbage();
  void Stop() {
    stopped_ = true;
    pthread_join(clean_thread_, nullptr);
  }
};
}
}
