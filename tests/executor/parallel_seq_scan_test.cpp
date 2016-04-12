//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// seq_scan_test.cpp
//
// Identification: tests/executor/seq_scan_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>
#include <ctime>

#include "harness.h"

#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/concurrency/transaction.h"
#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/exchange_seq_scan_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/exchange_seq_scan_plan.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_factory.h"
#include "backend/storage/tuple.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

#include "backend/common/cycle_timer.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class ExchangeSeqScanTests : public PelotonTest {};

namespace {

/**
 * @brief Set of tuple_ids that will satisfy the predicate in our test cases.
 */
const std::set<oid_t> g_tuple_ids({0, 3});
const int tuple_count = 100;
const int tile_group_count = 20000;
const size_t tuples = tuple_count * tile_group_count;


void SelectPopulateTiles(
  std::shared_ptr<storage::TileGroup> tile_group, int num_rows) {
  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
    catalog::Schema::AppendSchemaList(tile_schemas));

  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();
  const txn_id_t txn_id = txn->GetTransactionId();
  const cid_t commit_id = txn->GetCommitId();
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  for (int col_itr = 0; col_itr < num_rows; col_itr++) {
    storage::Tuple tuple(schema.get(), allocate);
    tuple.SetValue(0, ValueFactory::GetIntegerValue(col_itr % 10),
                   testing_pool);
    tuple.SetValue(1, ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(col_itr, 1)),
                   testing_pool);
    tuple.SetValue(2, ValueFactory::GetDoubleValue(ExecutorTestsUtil::PopulatedValue(col_itr, 2)),
                   testing_pool);
    Value string_value = ValueFactory::GetStringValue(
      std::to_string(ExecutorTestsUtil::PopulatedValue(col_itr, 3)));
    tuple.SetValue(3, string_value, testing_pool);

    oid_t tuple_slot_id = tile_group->InsertTuple(txn_id, &tuple);
    tile_group->CommitInsertedTuple(tuple_slot_id, txn_id, commit_id);
  }

  txn_manager.CommitTransaction();
}

/**
 * @brief Convenience method to create table for test.
 *
 * @return Table generated for test.
 */
storage::DataTable *CreateTable() {
  // const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable(tuple_count, false));

  // Schema for first tile group. Vertical partition is 2, 2.
  std::vector<catalog::Schema> schemas1(
    {catalog::Schema({ExecutorTestsUtil::GetColumnInfo(0),
                      ExecutorTestsUtil::GetColumnInfo(1)}),
     catalog::Schema({ExecutorTestsUtil::GetColumnInfo(2),
                      ExecutorTestsUtil::GetColumnInfo(3)})});

  // Schema for second tile group. Vertical partition is 1, 3.
  std::vector<catalog::Schema> schemas2(
    {catalog::Schema({ExecutorTestsUtil::GetColumnInfo(0)}),
     catalog::Schema({ExecutorTestsUtil::GetColumnInfo(1),
                      ExecutorTestsUtil::GetColumnInfo(2),
                      ExecutorTestsUtil::GetColumnInfo(3)})});

  TestingHarness::GetInstance().GetNextTileGroupId();

  std::map<oid_t, std::pair<oid_t, oid_t>> column_map1;
  column_map1[0] = std::make_pair(0, 0);
  column_map1[1] = std::make_pair(0, 1);
  column_map1[2] = std::make_pair(1, 0);
  column_map1[3] = std::make_pair(1, 1);

  std::map<oid_t, std::pair<oid_t, oid_t>> column_map2;
  column_map2[0] = std::make_pair(0, 0);
  column_map2[1] = std::make_pair(1, 0);
  column_map2[2] = std::make_pair(1, 1);
  column_map2[3] = std::make_pair(1, 2);

  for (auto i = 0; i < tile_group_count; i++) {
    if (i % 2 == 0) {
      // Create tile groups.
      table->AddTileGroup(std::shared_ptr<storage::TileGroup>(
        storage::TileGroupFactory::GetTileGroup(
          INVALID_OID, INVALID_OID,
          TestingHarness::GetInstance().GetNextTileGroupId(), table.get(),
          schemas1, column_map1, tuple_count)));
    } else {
      table->AddTileGroup(std::shared_ptr<storage::TileGroup>(
        storage::TileGroupFactory::GetTileGroup(
          INVALID_OID, INVALID_OID,
          TestingHarness::GetInstance().GetNextTileGroupId(), table.get(),
          schemas2, column_map2, tuple_count)));
    }
  }

  // ExecutorTestsUtil::PopulateTiles(table->GetTileGroup(0), tuple_count);
  for (auto i = 0; i <= tile_group_count; i++) {
    ExecutorTestsUtil::PopulateTiles(table->GetTileGroup(i), tuple_count);
  }
  // ExecutorTestsUtil::PopulateTiles(table->GetTileGroup(2), tuple_count);

  return table.release();
}

/**
 * @brief Convenience method to create predicate for test.
 * @param tuple_ids Set of tuple ids that we want the predicate to match with.
 *
 * The predicate matches any tuple with ids in the specified set.
 * This assumes that the table was populated with PopulatedValue() in
 * ExecutorTestsUtil.
 *
 * Each OR node has an equality node to its right and another OR node to
 * its left. The leftmost leaf is a FALSE constant value expression.
 *
 * In each equality node, we either use (arbitrarily taking reference from the
 * parity of the loop iteration) the first field or last field of the tuple.
 */
expression::AbstractExpression *CreatePredicate(
  const std::set<oid_t> &tuple_ids,
  oid_t select) {
  assert(tuple_ids.size() >= 1);

  expression::AbstractExpression *predicate =
    expression::ExpressionUtil::ConstantValueFactory(Value::GetFalse());

  for (oid_t i = 0; i < select; i++) {
    // Create equality expression comparison tuple value and constant value.
    // First, create tuple value expression.
    expression::AbstractExpression *tuple_value_expr = nullptr;
    // I think basically this means take a look at column 0.
    tuple_value_expr = expression::ExpressionUtil::TupleValueFactory(0, 0);

    // Second, create constant value expression.
    Value constant_value = ValueFactory::GetIntegerValue(i);


    expression::AbstractExpression *constant_value_expr =
      expression::ExpressionUtil::ConstantValueFactory(constant_value);

    // Finally, link them together using an equality expression.
    expression::AbstractExpression *equality_expr =
      expression::ExpressionUtil::ComparisonFactory(EXPRESSION_TYPE_COMPARE_EQUAL,
                                                    tuple_value_expr, constant_value_expr);

    // Join equality expression to other equality expression using ORs.
    predicate = expression::ExpressionUtil::ConjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_OR,
                                                               predicate, equality_expr);
  }

  return predicate;
}

/**
 * @brief Convenience method to extract next tile from executor.
 * @param executor Executor to be tested.
 *
 * @return Logical tile extracted from executor.
 */
executor::LogicalTile *GetNextTile(executor::AbstractExecutor &executor) {
  EXPECT_TRUE(executor.Execute());
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_THAT(result_tile, NotNull());
  return result_tile.release();
}

/**
 * @brief Runs actual test used by some or all of the test cases below.
 * @param executor Sequential scan executor to be tested.
 * @param expected_num_tiles Expected number of output tiles.
 * @param expected_num_cols Expected number of columns in the output
 *        logical tile(s).
 *
 * There are a lot of contracts between this function and the test cases
 * that use it (especially the part that verifies values). Please be mindful
 * if you're making changes.
 */
void RunTest(executor::ExchangeSeqScanExecutor &executor, int expected_num_tiles,
             __attribute__((unused))   int expected_num_cols) {
  EXPECT_TRUE(executor.Init());
  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  for (int i = 0; i < expected_num_tiles; i++) {
    result_tiles.emplace_back(GetNextTile(executor));
  }
  EXPECT_FALSE(executor.Execute());

  // Check correctness of result tiles.
  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_EQ(expected_num_cols, result_tiles[i]->GetColumnCount());

    // Only two tuples per tile satisfy our predicate.
    EXPECT_EQ(g_tuple_ids.size(), result_tiles[i]->GetTupleCount());

    // Verify values.
    std::set<oid_t> expected_tuples_left(g_tuple_ids);
    for (oid_t new_tuple_id : *(result_tiles[i])) {
      // We divide by 10 because we know how PopulatedValue() computes.
      // Bad style. Being a bit lazy here...

      int old_tuple_id =
          result_tiles[i]->GetValue(new_tuple_id, 0).GetIntegerForTestsOnly() /
          10;

      EXPECT_EQ(1, expected_tuples_left.erase(old_tuple_id));

      int val1 = ExecutorTestsUtil::PopulatedValue(old_tuple_id, 1);
      EXPECT_EQ(
          val1,
          result_tiles[i]->GetValue(new_tuple_id, 1).GetIntegerForTestsOnly());
      int val2 = ExecutorTestsUtil::PopulatedValue(old_tuple_id, 3);

      // expected_num_cols - 1 is a hacky way to ensure that
      // we are always getting the last column in the original table.
      // For the tile group test case, it'll be 2 (one column is removed
      // during the scan as part of the test case).
      // For the logical tile test case, it'll be 3.
      Value string_value(ValueFactory::GetStringValue(std::to_string(val2)));
      EXPECT_EQ(string_value,
                result_tiles[i]->GetValue(new_tuple_id, expected_num_cols - 1));
    }
    EXPECT_EQ(0, expected_tuples_left.size());
  }
}

// Sequential scan of table with predicate.
// The table being scanned has more than one tile group. i.e. the vertical
// partitioning changes midway.
TEST_F(SeqScanTests, TwoTileGroupsWithPredicateTest) {
  // Create table.
  std::unique_ptr<storage::DataTable> table(CreateTable());

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 3});

  // Create plan node.
  // planner::SeqScanPlan tnode(table.get(), CreatePredicate(g_tuple_ids),
  //                          column_ids);
  planner::ExchangeSeqScanPlan node(table.get(), nullptr,
                           column_ids);

  //std::clock_t start;
  //double* duration = new double(0);
  //start = std::clock();
  double startTime = CycleTimer::currentSeconds();

  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
  new executor::ExecutorContext(txn));

  executor::ExchangeSeqScanExecutor executor(&node, context.get());
  RunTest(executor, table->GetTileGroupCount(), column_ids.size());

  txn_manager.CommitTransaction();
  // *duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
  //LOG_INFO("Duration: %f", *duration);
  double dt = CycleTimer::currentSeconds() - startTime;
  std::cout << "Duration: " << dt << std::endl;
  //delete duration;
}
}

}
}  // namespace test
}  // namespace peloton
