//
// Created by wendongli on 2/28/16.
//
#include "gtest/gtest.h"
#include "harness.h"

#include "backend/common/logger.h"
#include "backend/index/index_factory.h"
#include "backend/storage/tuple.h"
#include "backend/index/bwtree.h"

#include <cstdlib>
#include <ctime>
#include <vector>
#include <algorithm>
#include <iostream>
#include <chrono>

#define TT

namespace peloton {
  namespace test {

    void VerifyAndFree(index::PIDTable &table,
                       const std::vector<index::Address> &addresses,
                       const std::vector<index::PID> &pids) {
      assert(addresses.size()==pids.size());
      // verify
      for(size_t i = 0; i<addresses.size(); ++i) {
        EXPECT_EQ(table.get(pids[i]), addresses[i]);
      }
      // free
      for(size_t i = 0; i<addresses.size(); ++i)
        table.free_PID(pids[i]);
    }

#ifdef TT
    TEST(PIDTableTests, BasicTest) {
#else
    void main() {
#endif
      static constexpr int N = 4097;
      // PIDTable
      index::PIDTable table;
      std::vector<index::Address> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(N);
      pids.reserve(N);
      // test
      for(int iter = 0; iter<4; ++iter) {
        // allocate
        for(int i = 0; i<N; ++i) {
          addresses.push_back((index::Address)(unsigned long) rand());
          pids.push_back(table.allocate_PID(addresses[i]));
        }
        // verify
        VerifyAndFree(table, addresses, pids);
        addresses.clear();
        pids.clear();
      }
    }

    // Allocate helper function
    void Allocate(std::atomic<std::uint_least8_t> *no_gen, int size_each,
                  const std::vector<index::Address> *addresses,
                  std::vector<index::PID> *pids,
                  index::PIDTable *table) {
      //assert(addresses.size()>=size&&pids.size()>=size);
      int no = (*no_gen)++;
      for(int i = 0; i<size_each; ++i) {
        (*pids)[size_each*no+i] = table->allocate_PID((*addresses)[size_each*no+i]);
        if(i%100==0)
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      LOG_DEBUG("allocate thread %d done.", no);
    }

#ifdef TT
    TEST(PIDTableTests, ConcurrentAllocationTest) {
#else
    void main2() {
#endif
      static constexpr int size = 4096;
      static constexpr int total = 4;
      static constexpr int size_each = size/total;
      std::atomic<std::uint_least8_t> no_gen(0);
      // PIDTable
      index::PIDTable table;
      std::vector<index::Address> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(size);
      pids.reserve(size);
      for(int iter=0; iter<5; ++iter) {
        for(int i = 0; i<size; ++i) {
          addresses.push_back((index::Address)(unsigned long) rand());
          pids.push_back(index::PIDTable::PID_NULL);
        }

        LaunchParallelTest(total, Allocate, &no_gen, size_each, &addresses, &pids, &table);

        std::this_thread::sleep_for(std::chrono::milliseconds(5000));

        // verify
        VerifyAndFree(table, addresses, pids);

        no_gen = 0;
        addresses.clear();
        pids.clear();
      }
    }

    void AllocateOrFreeAndTest(std::atomic<std::uint_least8_t> *no_gen,
                               int size_each,
                               const std::vector<index::Address> *addresses,
                               std::vector<index::PID> *pids,
                               index::PIDTable *table) {
      //assert(addresses.size()>=size&&pids.size()>=size);
      auto no = (*no_gen)++;
      // even allocate
      if(no%2==0) {
        for(int i = 0; i<size_each; ++i) {
          (*pids)[size_each*no+i] = table->allocate_PID((*addresses)[size_each*no+i]);
          if(i%100==0)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        LOG_DEBUG("allocate thread %d allocation done.", no);

        for(int i = 0; i<size_each; ++i) {
          EXPECT_EQ(table->get((*pids)[size_each*no+i]), (*addresses)[size_each*no+i]);
        }

        LOG_DEBUG("allocate thread %d verification done.", no);

        for(int i = 0; i<size_each; ++i) {
          table->free_PID((*pids)[size_each*no+i]);
        }

        LOG_DEBUG("allocate thread %d free done.", no);
      }
        // odd free
      else {
        for(int i=0; i<size_each; ++i)
          table->free_PID((*pids)[size_each*no+i]);
        LOG_DEBUG("free thread %d done.", no);
      }
    }
#ifdef TT
    TEST(PIDTableTests, ConcurrentAllocationFreeTest) {
#else
    void main3() {
#endif
      static constexpr int size = 4096;
      static constexpr int total = 4;
      static constexpr int size_each = size/total;
      std::atomic<std::uint_least8_t> no_gen(0);
      // PIDTable
      index::PIDTable table;
      std::vector<index::Address> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(size);
      pids.reserve(size);

      for(int iter=0; iter<5; ++iter) {
        for(int i = 0; i<size; ++i) {
          addresses.push_back((index::Address)(unsigned long) rand());
          pids.push_back(index::PIDTable::PID_NULL);
        }

        for(int i = 0; i<total; ++i) {
          if(i%2==1) {
            for(int j = 0; j<size_each; ++j)
              pids[i*size_each+j] = table.allocate_PID(addresses[i*size_each+j]);
          }
        }

        LaunchParallelTest(total, AllocateOrFreeAndTest, &no_gen, size_each, &addresses, &pids, &table);

        std::this_thread::sleep_for(std::chrono::milliseconds(5000));

        no_gen = 0;
        addresses.clear();
        pids.clear();
      }
    }

    void CompareAndSwap(std::atomic<std::uint_least8_t> *no_gen, int size_each,
                        const std::vector<index::Address> *original_addresses,
                        const std::vector<index::Address> *to_addresses,
                        std::vector<bool> *success,
                        std::vector<index::PID> *pids,
                        index::PIDTable *table) {
      auto no = (*no_gen)++;
      for(int i = 0; i<size_each; ++i) {
        (*success)[size_each*no+i] =
                table->bool_compare_and_swap((*pids)[size_each*no+i],
                                             (*original_addresses)[size_each*no+i],
                                             (*to_addresses)[size_each*no+i]);
      }
      LOG_DEBUG("CAS thread %d done.", no);
    }

#ifdef TT
    TEST(PIDTableTests, CompareAndSwapTest) {
#else
    void main4() {
#endif
      static constexpr int size = 4096;
      static constexpr int total = 4;
      static constexpr int size_each = size/total;
      std::atomic<std::uint_least8_t> no_gen(0);
      // PIDTable
      index::PIDTable table;
      std::vector<index::Address> original_addresses;
      std::vector<index::Address> to_addresses;
      std::vector<index::PID> pids;
      std::vector<bool> success;

      srand(time(NULL));
      original_addresses.reserve(size);
      to_addresses.reserve(size);
      pids.reserve(size);

      for(int iter=0; iter<5; ++iter) {
        // initialize
        for(int i = 0; i<size; ++i) {
          original_addresses.push_back((index::Address) (unsigned long) rand());
          to_addresses.push_back(nullptr);
          pids.push_back(table.allocate_PID(original_addresses[i]));
          success.push_back(false);
        }

        // launch test
        LaunchParallelTest(total, CompareAndSwap, &no_gen,
                           size_each, &original_addresses,
                           &to_addresses, &success, &pids, &table);

        // wait finish
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));

        // verify
        for(int i = 0; i<size; ++i) {
          if(success[i]) {
            EXPECT_EQ(table.get(pids[i]), to_addresses[i]);
          }
          else {
            EXPECT_EQ(table.get(pids[i]), original_addresses[i]);
          }
        }

        // clean up
        for(int i=0; i<size; ++i)
          table.free_PID(pids[i]);
        no_gen = 0;
        original_addresses.clear();
        to_addresses.clear();
        pids.clear();
        success.clear();
      }
    }

    void ConcurrentCompareAndSwap(std::atomic<std::uint_least8_t> *no_gen, int size,
                        const std::vector<index::Address> *original_addresses,
                        const std::vector<index::Address> *to_addresses,
                        std::vector<bool> *success,
                        std::vector<index::PID> *pids,
                        index::PIDTable *table) {
      auto no = (*no_gen)++;
      for(int i = 0; i<size; ++i) {
        success[no][i] =
                table->bool_compare_and_swap((*pids)[i],
                                             (*original_addresses)[i],
                                             to_addresses[no][i]);
      }
      LOG_DEBUG("concurrent CAS thread %d done.", no);
    }

#ifdef TT
    TEST(PIDTableTests, ConcurrentCompareAndSwapTest) {
#else
    void main5() {
#endif
      static constexpr int size = 4096;
      static constexpr int total = 4;
      std::atomic<std::uint_least8_t> no_gen(0);
      // PIDTable
      index::PIDTable table;
      std::vector<index::Address> original_addresses;
      std::vector<index::Address> to_addresses[total];
      std::vector<index::PID> pids;
      std::vector<bool> success[total];

      srand(time(NULL));
      original_addresses.reserve(size);
      pids.reserve(size);
      for(int i=0; i<total; ++i) {
        to_addresses[i].reserve(size);
        success[i].reserve(size);
      }

      for(int iter=0; iter<5; ++iter) {
        // initialize
        for(int i = 0; i<size; ++i) {
          original_addresses.push_back((index::Address) (unsigned long) rand());
          pids.push_back(table.allocate_PID(original_addresses[i]));
          for(int j=0; j<total; ++j) {
            to_addresses[j].push_back((index::Address) (unsigned long) rand());
            success[j].push_back(false);
          }
        }

        // launch test
        LaunchParallelTest(total, ConcurrentCompareAndSwap, &no_gen,
                           size, &original_addresses,
                           to_addresses, success, &pids, &table);

        // wait finish
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));

        // verify
        for(int i = 0; i<size; ++i) {
          bool only_one = false;
          for(int j=0; j<total; ++j) {
            EXPECT_TRUE((success[j][i]&&table.get(pids[i])==to_addresses[j][i])||!success[j][i]);
            if(success[j][i])
              only_one = !only_one;
          }
          EXPECT_TRUE(only_one);
        }

        // clean up
        for(int i=0; i<size; ++i)
          table.free_PID(pids[i]);
        no_gen = 0;
        original_addresses.clear();
        pids.clear();
        for(int i=0; i<total; ++i) {
          to_addresses[i].clear();
          success[i].clear();
        }
      }
    }
  }
}