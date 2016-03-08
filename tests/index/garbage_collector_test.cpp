//
// Created by wendongli on 2/28/16.
//
#include "gtest/gtest.h"
#include "harness.h"

#include "backend/index/bwtree.h"
#include "backend/common/types.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

#include <cstdlib>
#include <ctime>
#include <vector>
#include <algorithm>
#include <iostream>
#include <chrono>

#define TT

namespace peloton {
  namespace test {
#ifdef TT
    TEST(GarbageCollectorTest, BasicTest) {
#else
    void main() {
#endif
      static constexpr int size = 4096;
      static constexpr int max_length = 10;

      index::GarbageCollector &gc = index::GarbageCollector::global_gc_;
      srand(time(NULL));
      const index::BWBaseNode *new_garbage;
      for(int iter = 0; iter<3; ++iter) {
        index::EpochTime registered_time = gc.Register();
        for(int i = 0; i<size; ++i) {
          if(i%100==0) {
            gc.Deregister(registered_time);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            registered_time = gc.Register();
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
          }
          new_garbage = index::BWNode<index::IntsKey<1>, index::IntsComparator<1>>::GenerateRandomNodeChain((rand()%max_length)+1);
          gc.SubmitGarbage(new_garbage);
        }
        gc.Deregister(registered_time);
        //std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        dbg_msg("BASIC_TEST iteration %d finished.", iter);
      }
    }

    void SubmitGarbage(std::atomic<std::uint_least8_t> *no_gen) {
      static constexpr int max_duration = 50;
      static constexpr int max_length = 10;
      static constexpr int size = 4096;
      int no = (*no_gen)++;
      index::GarbageCollector &gc = index::GarbageCollector::global_gc_;
      index::EpochTime registered_time = gc.Register();
      const index::BWBaseNode *new_garbage;
      for(int i = 0; i<size; ++i) {
        if(i%100==0) {
          gc.Deregister(registered_time);
          std::this_thread::sleep_for(std::chrono::milliseconds(rand()%max_duration));
          registered_time = gc.Register();
        }
        new_garbage = index::BWNode<index::IntsKey<1>, index::IntsComparator<1>>::GenerateRandomNodeChain((rand()%max_length)+1);
        gc.SubmitGarbage(new_garbage);
      }
      gc.Deregister(registered_time);
      dbg_msg("thread %d submit garbage done.", no);
    }

#ifdef TT
    TEST(GarbageCollectorTest, ConcurrentSubmitGarbageTest) {
#else
    void main2() {
#endif
      static constexpr int total = 6;
      std::atomic<std::uint_least8_t> no_gen(0);
      srand(time(NULL));
      for(int iter = 0; iter<3; ++iter) {
        LaunchParallelTest(total, SubmitGarbage, &no_gen);
        //std::this_thread::sleep_for(std::chrono::milliseconds(10000));
        dbg_msg("CONCURRENT_SUBMIT_GARBAGE_TEST iteration %d finished.", iter);
      }
    }
  }
}