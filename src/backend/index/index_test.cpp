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

#include <string>
#include <cstdlib>
#include <ctime>
#include <vector>
#include <algorithm>
#include <iostream>
#include <chrono>

namespace peloton {
  namespace test {

//===--------------------------------------------------------------------===//
// Index Tests
//===--------------------------------------------------------------------===//
// for IDE formatting, since IDE cant recognize when we use TEST(...)
#define TT





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

        //std::this_thread::sleep_for(std::chrono::milliseconds(5000));

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







    
#ifdef TT
    TEST(GarbageCollectorTest, BasicTest) {
#else
    void main() {
#endif
      static constexpr int size = 4096;
      static constexpr int max_length = 10;

      index::GarbageCollector &gc = index::GarbageCollector::global_gc_;
      srand(time(NULL));
      const index::BWNode *new_garbage;
      for(int iter = 0; iter<3; ++iter) {
        index::EpochTime registered_time = gc.Register();
        for(int i = 0; i<size; ++i) {
          if(i%100==0) {
            gc.Deregister(registered_time);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            registered_time = gc.Register();
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
          }
          new_garbage = index::BWNode::GenerateRandomNodeChain((rand()%max_length)+1);
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
      const index::BWNode *new_garbage;
      for(int i = 0; i<size; ++i) {
        if(i%100==0) {
          gc.Deregister(registered_time);
          std::this_thread::sleep_for(std::chrono::milliseconds(rand()%max_duration));
          registered_time = gc.Register();
        }
        new_garbage = index::BWNode::GenerateRandomNodeChain((rand()%max_length)+1);
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








    catalog::Schema *key_schema = nullptr;
    catalog::Schema *tuple_schema = nullptr;

    ItemPointer item0(120, 5);
    ItemPointer item1(120, 7);
    ItemPointer item2(123, 19);

    index::Index *BuildIndex() {
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

      // Build index metadata
      const bool unique_keys = true;

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
    TEST(IndexTests, SingleThreadTest) {
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

        // clear up
        pairs.clear();
        no_gen = 0;
        result.clear();
        dbg_msg("iteration %d done.", iter);
      }

      delete tuple_schema;
    }

#ifdef TT
    TEST(IndexTests, SingleThreadUniqueKeyTest) {
#else
    void main3() {
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

        // check unique constraint
        no_gen = 0;
        LaunchParallelTest(1, InsertFunction, &no_gen, index.get(), size, &pairs, &result);
        for(size_t i = 0; i<size; ++i) {
          EXPECT_FALSE(result[i]);
        }

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

        // clear up
        pairs.clear();
        no_gen = 0;
        result.clear();
        dbg_msg("iteration %d done.", iter);
      }

      delete tuple_schema;
    }

#ifdef TT
    TEST(IndexTests, MultipleThreadTest) {
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

        // clear up
        pairs.clear();
        no_gen = 0;
        result.clear();
        dbg_msg("iteration %d done.", iter);
      }

      delete tuple_schema;
    }
  }  // End test namespace
}  // End peloton namespace
