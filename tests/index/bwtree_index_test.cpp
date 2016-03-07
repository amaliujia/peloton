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
#include "backend/index/bwtree.h"
#include "backend/storage/tuple.h"

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

#define TT

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
