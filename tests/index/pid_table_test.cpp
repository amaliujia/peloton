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
                       const std::vector<const index::BWNode *> &addresses,
                       const std::vector<PID> &pids) {
      assert(addresses.size()==pids.size());
      // verify
      for(int i = 0; i<addresses.size(); ++i) {
        EXPECT_EQ(table.get[pids[i]], addresses[i]);
      }
      // free
      for(int i = 0; i<addresses.size(); ++i)
        table.free_PID(pids[i]);
    }

    TEST(PIDTableTests, BasicTest) {
    //int main() {
      static constexpr size_t N = 4097;
      // PIDTable
      index::PIDTable table;
      std::vector<const index::BWNode *> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(N);
      pids.reserve(N);
      std::generate_n(addresses.begin(), N, []() { return (const index::BWNode *) rand(); });
      // test
      for(int iter = 0; iter<2; ++iter) {
        // allocate
        for(int i = 0; i<N; ++i)
          pids.push_back(table.allocate_PID(addresses[i]));
        VerifyAndFree(table, addresses, pids);
      }
    }

    // Allocate helper function
    void Allocate(std::atomic<std::uint_least8_t> &no_gen, int total, int size_each,
                  const std::vector<const index::BWnode *> &addresses,
                  std::vector<index::PID> &pids,
                  index::PIDTable &table) {
      assert(addresses.size()>=size&&pids.size()>=size);
      int no = no_gen++;
      for(int i = 0; i<size_each; ++i)
        pids[size_each*no+i] = table.allocate_PID(addresses[size_each*no+i]);
      LOG_DEBUG("allocate thread %d done.", no);
    }

    Test(PIDTableTests, ConcurrentAllocationTest) {
    //int main2() {
      static constexpr size_t size = 4096;
      static constexpr size_t total = 4;
      std::atomic<std::uint_least8_t> no_gen = 0;
      // PIDTable
      index::PIDTable table;
      std::vector<const index::BWNode *> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(size);
      pids.reserve(size);
      std::generate_n(addresses.begin(), size, []() { return (const index::BWNode *) rand(); });
      std::fill_n(pids.begin(), size, index::PIDTable::PID_NULL);

      LaunchParallelTest(total, Allocate, no_gen, total, size, addresses, pids, table);

      // TODO lanuch will wait till all done?
      VerifyAndFree(table, addresses, pids);
    }

    void AllocateOrFreeAndTest(std::atomic<std::uint_least8_t> &no_gen, int total, int size_each,
                  const std::vector<const index::BWnode *> &addresses,
                  std::vector<index::PID> &pids,
                  index::PIDTable &table) {
      assert(addresses.size()>=size&&pids.size()>=size);
      int no = no_gen++;
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
    //int main3() {
      static constexpr size_t size = 4096;
      static constexpr size_t total = 4;
      std::atomic<std::uint_least8_t> no_gen = 0;
      // PIDTable
      index::PIDTable table;
      std::vector<const index::BWNode *> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(size);
      pids.reserve(size);
      std::generate_n(addresses.begin(), size, []() { return (const index::BWNode *) rand(); });
      std::fill_n(pids.begin(), size, index::PIDTable::PID_NULL);
      for(int i=0; i<size; ++i)
        if(i%4096==1)
          pids[i] = table.allocate_PID(addresses[i]);
      LaunchParallelTest(total, AllocateOrFreeAndTest, no_gen, total, size, addresses, pids, table);
    }
  }
}