//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index_test.cpp
//
// Identification: tests/index/index_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/common/logger.h"
#include "backend/index/index_factory.h"
#include "backend/storage/tuple.h"
#include "backend/index/bwtree.h"
#include "backend/common/types.h"
#include "backend/index/index_key.h"

#include <string>
#include <cstdlib>
#include <ctime>
#include <vector>
#include <algorithm>
#include <iostream>
#include <chrono>


// for IDE formatting, since IDE can't recognize when we use TEST(...)
#define TT

namespace peloton {
  namespace test {

// epoch-based garbage collector test
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
          new_garbage = index::BWBaseNode::GenerateRandomNodeChain((rand()%max_length)+1);
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
        new_garbage = index::BWBaseNode::GenerateRandomNodeChain((rand()%max_length)+1);
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








// pid table test
    void VerifyAndFree(index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>> &table,
                       const std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> &addresses,
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
      index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>> table;
      std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(N);
      pids.reserve(N);
      // test
      for(int iter = 0; iter<4; ++iter) {
        // allocate
        for(int i = 0; i<N; ++i) {
          addresses.push_back((index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address)(unsigned long) rand());
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
                  const std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> *addresses,
                  std::vector<index::PID> *pids,
                  index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>> *table) {
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
      index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>> table;
      std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(size);
      pids.reserve(size);
      for(int iter=0; iter<5; ++iter) {
        for(int i = 0; i<size; ++i) {
          addresses.push_back((index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address)(unsigned long) rand());
          pids.push_back(index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::PID_NULL);
        }

        LaunchParallelTest(total, Allocate, &no_gen, size_each, &addresses, &pids, &table);

        //std::this_thread::sleep_for(std::chrono::milliseconds(5000));

        // verify
        VerifyAndFree(table, addresses, pids);

        no_gen = 0;
        addresses.clear();
        pids.clear();
      }
    }

    void AllocateOrFreeAndTest(std::atomic<std::uint_least8_t> *no_gen,
                               int size_each,
                               const std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> *addresses,
                               std::vector<index::PID> *pids,
                               index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>> *table) {
      //assert(addresses.size()>=size&&pids.size()>=size);
      auto no = (*no_gen)++;
      // even allocate
      if(no%2==0) {
        for(int i = 0; i<size_each; ++i) {
          (*pids)[size_each*no+i] = table->allocate_PID((*addresses)[size_each*no+i]);
          if(i%100==0)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        dbg_msg("allocate thread %d allocation done.", no);

        for(int i = 0; i<size_each; ++i) {
          EXPECT_EQ(table->get((*pids)[size_each*no+i]), (*addresses)[size_each*no+i]);
        }

        dbg_msg("allocate thread %d verification done.", no);

        for(int i = 0; i<size_each; ++i) {
          table->free_PID((*pids)[size_each*no+i]);
        }

        dbg_msg("allocate thread %d free done.", no);
      }
        // odd free
      else {
        for(int i=0; i<size_each; ++i)
          table->free_PID((*pids)[size_each*no+i]);
        dbg_msg("free thread %d done.", no);
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
      index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>> table;
      std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> addresses;
      std::vector<index::PID> pids;
      srand(time(NULL));
      addresses.reserve(size);
      pids.reserve(size);

      for(int iter=0; iter<5; ++iter) {
        for(int i = 0; i<size; ++i) {
          addresses.push_back((index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address)(unsigned long) rand());
          pids.push_back(index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::PID_NULL);
        }

        for(int i = 0; i<total; ++i) {
          if(i%2==1) {
            for(int j = 0; j<size_each; ++j)
              pids[i*size_each+j] = table.allocate_PID(addresses[i*size_each+j]);
          }
        }

        LaunchParallelTest(total, AllocateOrFreeAndTest, &no_gen, size_each, &addresses, &pids, &table);

        //std::this_thread::sleep_for(std::chrono::milliseconds(5000));

        no_gen = 0;
        addresses.clear();
        pids.clear();
      }
    }

    void CompareAndSwap(std::atomic<std::uint_least8_t> *no_gen, int size_each,
                        const std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> *original_addresses,
                        const std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> *to_addresses,
                        std::vector<bool> *success,
                        std::vector<index::PID> *pids,
                        index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>> *table) {
      auto no = (*no_gen)++;
      for(int i = 0; i<size_each; ++i) {
        (*success)[size_each*no+i] =
                table->bool_compare_and_swap((*pids)[size_each*no+i],
                                             (*original_addresses)[size_each*no+i],
                                             (*to_addresses)[size_each*no+i]);
      }
      dbg_msg("CAS thread %d done.", no);
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
      index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>> table;
      std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> original_addresses;
      std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> to_addresses;
      std::vector<index::PID> pids;
      std::vector<bool> success;

      srand(time(NULL));
      original_addresses.reserve(size);
      to_addresses.reserve(size);
      pids.reserve(size);

      for(int iter=0; iter<5; ++iter) {
        // initialize
        for(int i = 0; i<size; ++i) {
          original_addresses.push_back((index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address) (unsigned long) rand());
          to_addresses.push_back(nullptr);
          pids.push_back(table.allocate_PID(original_addresses[i]));
          success.push_back(false);
        }

        // launch test
        LaunchParallelTest(total, CompareAndSwap, &no_gen,
                           size_each, &original_addresses,
                           &to_addresses, &success, &pids, &table);

        // wait finish
        //std::this_thread::sleep_for(std::chrono::milliseconds(5000));

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
                                  const std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> *original_addresses,
                                  const std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> *to_addresses,
                                  std::vector<bool> *success,
                                  std::vector<index::PID> *pids,
                                  index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>> *table) {
      auto no = (*no_gen)++;
      for(int i = 0; i<size; ++i) {
        success[no][i] =
                table->bool_compare_and_swap((*pids)[i],
                                             (*original_addresses)[i],
                                             to_addresses[no][i]);
      }
      dbg_msg("concurrent CAS thread %d done.", no);
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
      index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>> table;
      std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> original_addresses;
      std::vector<index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address> to_addresses[total];
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
          original_addresses.push_back((index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address) (unsigned long) rand());
          pids.push_back(table.allocate_PID(original_addresses[i]));
          for(int j=0; j<total; ++j) {
            to_addresses[j].push_back((index::PIDTable<index::IntsKey<1>, index::IntsComparator<1>>::Address) (unsigned long) rand());
            success[j].push_back(false);
          }
        }

        // launch test
        LaunchParallelTest(total, ConcurrentCompareAndSwap, &no_gen,
                           size, &original_addresses,
                           to_addresses, success, &pids, &table);

        // wait finish
        //std::this_thread::sleep_for(std::chrono::milliseconds(5000));

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








// BWtree index test
    catalog::Schema *key_schema = nullptr;
    catalog::Schema *tuple_schema = nullptr;

    ItemPointer item0(120, 5);
    ItemPointer item1(120, 7);
    ItemPointer item2(123, 19);

    index::Index *BuildIndex(bool unique_keys=false) {
      // Build tuple and key schema
      std::vector<std::vector<std::string>> column_names;
      std::vector<catalog::Column> columns;
      std::vector<catalog::Schema *> schemas;
      IndexType index_type = INDEX_TYPE_BWTREE;

      catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                              "A", true);
      catalog::Column column2(VALUE_TYPE_VARCHAR, 1024, "B", true);
      catalog::Column column3(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE),
                              "C", true);
      catalog::Column column4(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                              "D", true);

      columns.push_back(column1);
      columns.push_back(column2);

      // INDEX KEY SCHEMA -- {column1, column2}
      key_schema = new catalog::Schema(columns);
      key_schema->SetIndexedColumns({0, 1});

      columns.push_back(column3);
      columns.push_back(column4);

      // TABLE SCHEMA -- {column1, column2, column3, column4}
      tuple_schema = new catalog::Schema(columns);

      index::IndexMetadata *index_metadata = new index::IndexMetadata(
              "test_index", 125, index_type, INDEX_CONSTRAINT_TYPE_DEFAULT,
              tuple_schema, key_schema, unique_keys);

      // Build index
      index::Index *index = index::IndexFactory::GetInstance(index_metadata);
      EXPECT_TRUE(index!=NULL);

      return index;
    }

#ifdef TT
    TEST(BWTreeIndexTests, BasicTest) {
#else
    void main() {
#endif
      auto pool = TestingHarness::GetInstance().GetTestingPool();
      std::vector<ItemPointer> locations;

      // INDEX
      std::unique_ptr<index::Index> index(BuildIndex());

      std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));

      key0->SetValue(0, ValueFactory::GetIntegerValue(100), pool);

      key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);

      // INSERT
      index->InsertEntry(key0.get(), item0);

      locations = index->ScanKey(key0.get());
      EXPECT_EQ(locations.size(), 1);
      EXPECT_EQ(locations[0].block, item0.block);

      // DELETE
      index->DeleteEntry(key0.get(), item0);

      locations = index->ScanKey(key0.get());
      EXPECT_EQ(locations.size(), 0);

      delete tuple_schema;
    }

    void GenerateKeyValues(VarlenPool *pool,
                           std::vector<std::pair<std::unique_ptr<storage::Tuple>, ItemPointer>> &pairs,
                           int size, int key_value_repeat_factor, int key_repeat_factor, bool shuffle) {
      assert(key_repeat_factor>=key_value_repeat_factor);
      assert(key_value_repeat_factor>0);
      pairs.reserve(size);
      srand(time(NULL));

      int key_number = 0;
      int key_first_half;
      std::string key_second_half;
      oid_t value_number = 0;
      oid_t value_first_half;
      oid_t value_second_half;

      for(int i=0, j=0; i<size; ++i) {
        if(j==0) {
          key_first_half = key_number++;
          key_second_half = std::to_string(rand());
          value_first_half = value_number++;
          value_second_half = value_first_half;
        }
        else if(j<key_value_repeat_factor) {

        }
        else if(j<key_repeat_factor) {
          value_first_half = value_number++;
          value_second_half = value_number++;
        }
        pairs.push_back(std::pair<std::unique_ptr<storage::Tuple>, ItemPointer>());
        std::pair<std::unique_ptr<storage::Tuple>, ItemPointer> &p = pairs.back();
        p.first.reset(new storage::Tuple(key_schema, true));
        p.second.block = value_first_half;
        p.second.offset = value_second_half;
        p.first->SetValue(0, ValueFactory::GetIntegerValue(key_first_half), pool);
        p.first->SetValue(1, ValueFactory::GetStringValue(key_second_half), pool);
        ++j;
        if(j==key_repeat_factor)
          j = 0;
      }

      if(shuffle) {
        std::random_shuffle(pairs.begin(), pairs.end());
      }
    }

    void InsertFunction(std::atomic<std::uint_least8_t> *no_gen, index::Index *index, const size_t size_each,
                        std::vector<std::pair<std::unique_ptr<storage::Tuple>, ItemPointer>> *pairs,
                        std::vector<bool> *result) {
      const unsigned int no = (*no_gen)++;
      assert(result->size()==pairs->size());
      assert(size_each*(no+1)<=pairs->size());
      for(auto i = no*size_each; i<(no+1)*size_each; ++i) {
        (*result)[i] = index->InsertEntry((*pairs)[i].first.get(), (*pairs)[i].second);
      }
      dbg_msg("insert thread %d done.", no);
    }

    void DeleteFunction(std::atomic<std::uint_least8_t> *no_gen, index::Index *index, const size_t size_each,
                        std::vector<std::pair<std::unique_ptr<storage::Tuple>, ItemPointer>> *pairs,
                        std::vector<bool> *result) {
      const int no = (*no_gen)++;
      assert(result->size()==pairs->size());
      assert(size_each*(no+1)<=pairs->size());
      for(auto i = no*size_each; i<(no+1)*size_each; ++i) {
        (*result)[i] = index->DeleteEntry((*pairs)[i].first.get(), (*pairs)[i].second);
      }
      dbg_msg("delete thread %d done.", no);
    }

#ifdef TT
    TEST(BWTreeIndexTests, SingleThreadTest) {
#else
    void main2() {
#endif
      auto pool = TestingHarness::GetInstance().GetTestingPool();
      // INDEX
      std::unique_ptr<index::Index> index(BuildIndex());

      constexpr size_t size(2000);
      std::vector<std::pair<std::unique_ptr<storage::Tuple>, ItemPointer>> pairs;
      std::atomic<std::uint_least8_t> no_gen(0);
      std::vector<bool> result;
      result.reserve(size);

      for(int iter=0; iter<1; ++iter) {
        // initialize variables
        GenerateKeyValues(pool, pairs, size, 1, 1, true);
        for(size_t i = 0; i<size; ++i) {
          result.push_back(false);
        }
        // try insertion
        LaunchParallelTest(1, InsertFunction, &no_gen, index.get(), size, &pairs, &result);

        // check insertion result
        std::vector<ItemPointer> locations;
        for(size_t i = 0; i<size; ++i) {
          EXPECT_TRUE(result[i]);
          locations = index->ScanKey(pairs[i].first.get());
          EXPECT_EQ(locations.size(), 1);
        }

        // check insertion result
        locations = index->ScanAllKeys();
        EXPECT_EQ(locations.size(), size);

        index->Cleanup();
        LOG_DEBUG("MemoryFootprint after insertion: %lu", (unsigned long)index->GetMemoryFootprint());

        // try deletion
        no_gen = 0;
        LaunchParallelTest(1, DeleteFunction, &no_gen, index.get(), size, &pairs, &result);

        // check deletion result
        for(size_t i = 0; i<size; ++i) {
          EXPECT_TRUE(result[i]);
          locations = index->ScanKey(pairs[i].first.get());
          EXPECT_EQ(locations.size(), 0);
        }

        // check deletion result
        locations = index->ScanAllKeys();
        EXPECT_EQ(locations.size(), 0);

        index->Cleanup();
        LOG_DEBUG("MemoryFootprint after deletion: %lu", (unsigned long)index->GetMemoryFootprint());

        // clear up
        pairs.clear();
        no_gen = 0;
        result.clear();
        dbg_msg("iteration %d done.", iter);
      }

      delete tuple_schema;
    }

#ifdef TT
    TEST(BWTreeIndexTests, SingleThreadUniqueKeyTest) {
#else
    void main3() {
#endif
      auto pool = TestingHarness::GetInstance().GetTestingPool();
      // INDEX
      std::unique_ptr<index::Index> index(BuildIndex(true));

      if(!index->HasUniqueKeys()) {
        LOG_TRACE("Index is built in duplicate mode, unique key test will not be done.");
        return ;
      }

      constexpr size_t size(2000);
      std::vector<std::pair<std::unique_ptr<storage::Tuple>, ItemPointer>> pairs;
      std::atomic<std::uint_least8_t> no_gen(0);
      std::vector<bool> result;
      result.reserve(size);

      for(int iter=0; iter<1; ++iter) {
        // initialize variables
        GenerateKeyValues(pool, pairs, size, 1, 1, true);
        for(size_t i = 0; i<size; ++i) {
          result.push_back(false);
        }

        // try insertion
        LaunchParallelTest(1, InsertFunction, &no_gen, index.get(), size, &pairs, &result);

        // check insertion result
        std::vector<ItemPointer> locations;
        for(size_t i = 0; i<size; ++i) {
          EXPECT_TRUE(result[i]);
          locations = index->ScanKey(pairs[i].first.get());
          EXPECT_EQ(locations.size(), 1);
        }

        // check insertion result
        locations = index->ScanAllKeys();
        EXPECT_EQ(locations.size(), size);

        index->Cleanup();
        LOG_DEBUG("MemoryFootprint after first insertion: %lu", (unsigned long)index->GetMemoryFootprint());

        // check unique constraint
        no_gen = 0;
        LaunchParallelTest(1, InsertFunction, &no_gen, index.get(), size, &pairs, &result);
        for(size_t i = 0; i<size; ++i) {
          EXPECT_FALSE(result[i]);
        }

        index->Cleanup();
        LOG_DEBUG("MemoryFootprint after second insertion: %lu", (unsigned long)index->GetMemoryFootprint());

        // try deletion
        no_gen = 0;
        LaunchParallelTest(1, DeleteFunction, &no_gen, index.get(), size, &pairs, &result);

        // check deletion result
        for(size_t i = 0; i<size; ++i) {
          EXPECT_TRUE(result[i]);
          locations = index->ScanKey(pairs[i].first.get());
          EXPECT_EQ(locations.size(), 0);
        }

        // check deletion result
        locations = index->ScanAllKeys();
        EXPECT_EQ(locations.size(), 0);

        index->Cleanup();
        LOG_DEBUG("MemoryFootprint after deletion: %lu", (unsigned long)index->GetMemoryFootprint());

        // clear up
        pairs.clear();
        no_gen = 0;
        result.clear();
        dbg_msg("iteration %d done.", iter);
      }

      delete tuple_schema;
    }

#ifdef TT
    TEST(BWTreeIndexTests, MultipleThreadTest) {
#else
    void main4() {
#endif
      auto pool = TestingHarness::GetInstance().GetTestingPool();
      // INDEX
      std::unique_ptr<index::Index> index(BuildIndex());

      constexpr size_t size(2000);
      constexpr size_t thread_number(4);
      constexpr size_t size_each(size/thread_number);

      std::vector<std::pair<std::unique_ptr<storage::Tuple>, ItemPointer>> pairs;
      std::atomic<std::uint_least8_t> no_gen(0);
      std::vector<bool> result;
      result.reserve(size);

      for(int iter=0; iter<1; ++iter) {
        // initialize variables
        GenerateKeyValues(pool, pairs, size, 1, 1, true);
        for(size_t i = 0; i<size; ++i) {
          result.push_back(false);
        }
        // try insertion
        LaunchParallelTest(thread_number, InsertFunction, &no_gen, index.get(), size_each, &pairs, &result);

        // check insertion result
        std::vector<ItemPointer> locations;
        for(size_t i = 0; i<size; ++i) {
          EXPECT_TRUE(result[i]);
          locations = index->ScanKey(pairs[i].first.get());
          EXPECT_EQ(locations.size(), 1);
        }

        // check insertion result
        locations = index->ScanAllKeys();
        EXPECT_EQ(locations.size(), size);

        index->Cleanup();
        LOG_DEBUG("MemoryFootprint after insertion: %lu", (unsigned long)index->GetMemoryFootprint());

        // try deletion
        no_gen = 0;
        LaunchParallelTest(thread_number, DeleteFunction, &no_gen, index.get(), size_each, &pairs, &result);

        // check deletion result
        for(size_t i = 0; i<size; ++i) {
          EXPECT_TRUE(result[i]);
          locations = index->ScanKey(pairs[i].first.get());
          EXPECT_EQ(locations.size(), 0);
        }

        // check deletion result
        locations = index->ScanAllKeys();
        EXPECT_EQ(locations.size(), 0);

        index->Cleanup();
        LOG_DEBUG("MemoryFootprint after deletion: %lu", (unsigned long)index->GetMemoryFootprint());

        // clear up
        pairs.clear();
        no_gen = 0;
        result.clear();
        dbg_msg("iteration %d done.", iter);
      }

      delete tuple_schema;
    }









// original index test

    TEST(IndexTests, BasicTest) {
    auto pool = TestingHarness::GetInstance().GetTestingPool();
    std::vector<ItemPointer> locations;

    // INDEX
    std::unique_ptr<index::Index> index(BuildIndex());

    std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));

    key0->SetValue(0, ValueFactory::GetIntegerValue(100), pool);

    key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);

    // INSERT
    index->InsertEntry(key0.get(), item0);

    locations = index->ScanKey(key0.get());
    EXPECT_EQ(locations.size(), 1);
    EXPECT_EQ(locations[0].block, item0.block);

    // DELETE
    index->DeleteEntry(key0.get(), item0);

    locations = index->ScanKey(key0.get());
    EXPECT_EQ(locations.size(), 0);

    delete tuple_schema;
  }

// INSERT HELPER FUNCTION
  void InsertTest(index::Index *index, VarlenPool *pool, size_t scale_factor){

    // Loop based on scale factor
    for(size_t scale_itr = 1; scale_itr <= scale_factor; scale_itr++) {
      // Insert a bunch of keys based on scale itr
      std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
      std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
      std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
      std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));
      std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));
      std::unique_ptr<storage::Tuple> keynonce(new storage::Tuple(key_schema, true));

      key0->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
      key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);
      key1->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
      key1->SetValue(1, ValueFactory::GetStringValue("b"), pool);
      key2->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
      key2->SetValue(1, ValueFactory::GetStringValue("c"), pool);
      key3->SetValue(0, ValueFactory::GetIntegerValue(400 * scale_itr), pool);
      key3->SetValue(1, ValueFactory::GetStringValue("d"), pool);
      key4->SetValue(0, ValueFactory::GetIntegerValue(500 * scale_itr), pool);
      key4->SetValue(1, ValueFactory::GetStringValue(
              "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"), pool);
      keynonce->SetValue(0, ValueFactory::GetIntegerValue(1000 * scale_itr), pool);
      keynonce->SetValue(1, ValueFactory::GetStringValue("f"), pool);

      // INSERT
      index->InsertEntry(key0.get(), item0);
      index->InsertEntry(key1.get(), item1);
      index->InsertEntry(key1.get(), item2);
      index->InsertEntry(key1.get(), item1);
      index->InsertEntry(key1.get(), item1);
      index->InsertEntry(key1.get(), item0);

      index->InsertEntry(key2.get(), item1);
      index->InsertEntry(key3.get(), item1);
      index->InsertEntry(key4.get(), item1);

    }
    LOG_TRACE("InsertTest() done.");
  }

// DELETE HELPER FUNCTION
  void DeleteTest(index::Index *index, VarlenPool *pool, size_t scale_factor){

    // Loop based on scale factor
    for(size_t scale_itr = 1; scale_itr <= scale_factor; scale_itr++) {
      // Delete a bunch of keys based on scale itr
      std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
      std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
      std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
      std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));
      std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));

      key0->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
      key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);
      key1->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
      key1->SetValue(1, ValueFactory::GetStringValue("b"), pool);
      key2->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
      key2->SetValue(1, ValueFactory::GetStringValue("c"), pool);
      key3->SetValue(0, ValueFactory::GetIntegerValue(400 * scale_itr), pool);
      key3->SetValue(1, ValueFactory::GetStringValue("d"), pool);
      key4->SetValue(0, ValueFactory::GetIntegerValue(500 * scale_itr), pool);
      key4->SetValue(1, ValueFactory::GetStringValue(
              "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"), pool);

      // DELETE
      index->DeleteEntry(key0.get(), item0);
      index->DeleteEntry(key1.get(), item1);
      index->DeleteEntry(key2.get(), item2);
      index->DeleteEntry(key3.get(), item1);
      index->DeleteEntry(key4.get(), item1);
    }
    LOG_TRACE("DeleteTest() done.");
  }

  TEST(IndexTests, DeleteTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, InsertTest, index.get(), pool, scale_factor);
  LaunchParallelTest(1, DeleteTest, index.get(), pool, scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);
  key1->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, ValueFactory::GetStringValue("b"), pool);
  key2->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, ValueFactory::GetStringValue("c"), pool);

  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 0);

  locations = index->ScanKey(key1.get());
  EXPECT_EQ(locations.size(), 2);

  locations = index->ScanKey(key2.get());
  EXPECT_EQ(locations.size(), 1);
  EXPECT_EQ(locations[0].block, item1.block);

  delete tuple_schema;
}

TEST(IndexTests, MultiThreadedInsertTest) {
auto pool = TestingHarness::GetInstance().GetTestingPool();
std::vector<ItemPointer> locations;

// INDEX
std::unique_ptr<index::Index> index(BuildIndex());

// Parallel Test
size_t num_threads = 4;
size_t scale_factor = 1;
LaunchParallelTest(num_threads, InsertTest, index.get(), pool, scale_factor);

locations = index->ScanAllKeys();
EXPECT_EQ(locations.size(), 9 * num_threads);

std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
std::unique_ptr<storage::Tuple> keynonce(new storage::Tuple(key_schema, true));

keynonce->SetValue(0, ValueFactory::GetIntegerValue(1000), pool);
keynonce->SetValue(1, ValueFactory::GetStringValue("f"), pool);

key0->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);

locations = index->ScanKey(keynonce.get());
EXPECT_EQ(locations.size(), 0);

locations = index->ScanKey(key0.get());
EXPECT_EQ(locations.size(), num_threads);
EXPECT_EQ(locations[0].block, item0.block);

delete tuple_schema;
}

}  // End test namespace
}  // End peloton namespace

