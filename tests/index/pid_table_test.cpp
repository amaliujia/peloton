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
#include <vector>
#include <algorithm>

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

    TEST(PIDTableTests, BasicTest) {
    //void main() {
      static constexpr int N = 4097;
      // PIDTable
      index::PIDTable table;
      std::vector<index::Address> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(N);
      pids.reserve(N);
      std::generate_n(addresses.begin(), N, []() { return (index::Address)(unsigned long) rand(); });
      // test
      for(int iter = 0; iter<2; ++iter) {
        // allocate
        for(int i = 0; i<N; ++i)
          pids.push_back(table.allocate_PID(addresses[i]));
        VerifyAndFree(table, addresses, pids);
        /*
        // verify
        for(size_t i = 0; i<addresses.size(); ++i) {
          EXPECT_EQ(table.get(pids[i]), addresses[i]);
        }
        // free
        for(size_t i = 0; i<addresses.size(); ++i)
          table.free_PID(pids[i]);
        */
      }
    }

    // Allocate helper function
    void Allocate(std::atomic<std::uint_least8_t> &no_gen, int size_each,
                  const std::vector<index::Address> &addresses,
                  std::vector<index::PID> &pids,
                  index::PIDTable &table) {
      //assert(addresses.size()>=size&&pids.size()>=size);
      int no = no_gen++;
      for(int i = 0; i<size_each; ++i)
        pids[size_each*no+i] = table.allocate_PID(addresses[size_each*no+i]);
      LOG_DEBUG("allocate thread %d done.", no);
    }

    Test(PIDTableTests, ConcurrentAllocationTest) {
    //void main2() {
      static constexpr int size = 4096;
      static constexpr int total = 4;
      static constexpr int size_each = size/total;
      std::atomic<std::uint_least8_t> no_gen = 0;
      // PIDTable
      index::PIDTable table;
      std::vector<index::Address> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(size);
      pids.reserve(size);
      std::generate_n(addresses.begin(), size, []() { return (index::Address)(unsigned long)  rand(); });
      std::fill_n(pids.begin(), size, index::PIDTable::PID_NULL);

      LaunchParallelTest(total, Allocate, no_gen, size_each, addresses, pids, table);

      // TODO lanuch will wait till all done?
      VerifyAndFree(table, addresses, pids);
      /*
      // verify
      for(size_t i = 0; i<addresses.size(); ++i) {
        EXPECT_EQ(table.get(pids[i]), addresses[i]);
      }
      // free
      for(size_t i = 0; i<addresses.size(); ++i)
        table.free_PID(pids[i]);
      */
    }

    void AllocateOrFreeAndTest(std::atomic<std::uint_least8_t> &no_gen,
                               int total, int size_each,
                               const std::vector<index::Address> &addresses,
                               std::vector<index::PID> &pids,
                               index::PIDTable &table) {
      //assert(addresses.size()>=size&&pids.size()>=size);
      auto no = no_gen++;
      // even allocate
      if(no%2==0) {
        for(int i = 0; i<size_each; ++i)
          pids[size_each*no+i] = table.allocate_PID(addresses[size_each*no+i]);
        LOG_DEBUG("allocate thread %d allocation done.", no);

        for(int i = 0; i<size_each; ++i) {
          EXPECT_EQ(table.get(pids[size_each*no+i]), addresses[size_each*no+i]);
        }

        LOG_DEBUG("allocate thread %d verification done.", no);
      }
        // odd free
      else {
        for(int i=0; i<size_each; ++i)
          table.free_PID(pids[size_each*no+i]);
        LOG_DEBUG("free thread %d done.", no);
      }
    }

    TEST(PIDTableTests, ConcurrentAllocationFreeTest) {
    //void main3() {
      static constexpr int size = 4096;
      static constexpr int total = 4;
      static constexpr int size_each = size/total;
      std::atomic<std::uint_least8_t> no_gen = 0;
      // PIDTable
      index::PIDTable table;
      std::vector<index::Address> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(size);
      pids.reserve(size);
      std::generate_n(addresses.begin(), size, []() { return (index::Address)(unsigned long) rand(); });
      std::fill_n(pids.begin(), size, index::PIDTable::PID_NULL);

      for(int i=0; i<total; ++i) {
        if(i%2==1) {
          for(int j=0; j<size_each; ++j)
            pids[i*size_each+j] = table.allocate_PID(address[i*size_each+j]);
        }
      }

      LaunchParallelTest(total, AllocateOrFreeAndTest, no_gen, size_each, addresses, pids, table);
      /*
      for(int i=0; i<total; ++i) {
        if(i%2==0) {
          for(int j=0; j<size_each; ++j) {
            EXPECT_EQ(table.get(pids[size_each*i+j]), addresses[size_each*i+j]);
          }
        }
      }
      */
    }

    void CompareAndSwap(std::atomic<std::uint_least8_t> &no_gen, int size_each,
                        const std::vector<index::Address> &original_addresses,
                        const std::vector<index::Address> &to_addresses,
                        std::vector<bool> &success,
                        std::vector<index::PID> &pids,
                        index::PIDTable &table) {
      //assert(original_addresses.size()>=size&&
      //       to_addresses.size()>=size&&
      //       pids.size()>=size);
      auto no = no_gen++;
      for(int i = 0; i<size_each; ++i) {
        success[size_each*no+i] =
                table.bool_compare_and_swap(pid[size_each*no+i],
                                            original_addresses[size_each*no+i],
                                            to_addresses[size_each*no+i]);
      }
      LOG_DEBUG("CAS thread %d done.", no);
    }

    TEST(PIDTableTests, CompareAndSwapTest) {
    //void main4() {
      static constexpr int size = 4096;
      static constexpr int total = 4;
      static constexpr int size_each = size/total;
      std::atomic<std::uint_least8_t> no_gen = 0;
      // PIDTable
      index::PIDTable table;
      std::vector<index::Address> original_addresses;
      std::vector<index::Address> to_addresses;
      std::vector<index::PID> pids;
      std::vector<bool> success;

      srand(time(NULL));
      addresses.reserve(size);
      to_addresses.reserve(size);
      pids.reserve(size);
      std::generate_n(original_addresses.begin(), size, []() { return (index::Address)(unsigned long) rand(); });
      std::generate_n(to_addresses.begin(), size, []() { return (index::Address)(unsigned long) rand(); });
      std::fill_n(success.begin(), size, false);
      for(int i=0; i<size; ++i) {
        pids[i] = table.allocate_PID(original_addresses[i]);
        EXPECT_EQ(table.get(pids[i]), original_addresses[i]);
      }

      LaunchParallelTest(total, CompareAndSwap, no_gen,
                         size_each, original_addresses,
                         to_addresses, success, pids, table);

      for(int i=0; i<size; ++i) {
        if(success[i]) {
          EXPECT_EQ(table.get(pids[i]), to_addresses[i]);
        }
        else {
          EXPECT_EQ(table.get(pids[i]), original_addresses[i]);
        }
      }
    }
  }
}