//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// planner_test.cpp
//
// Identification: tests/planner/planner_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#include "executor/executor_tests_util.h"
#include "backend/common/value_factory.h"
#include "gmock/gtest/gtest.h"
#include "backend/storage/data_table.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/seq_scan_plan.h"

#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_factory.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Planner Tests
//===--------------------------------------------------------------------===//

class PlannerTests : public PelotonTest {};

TEST_F(PlannerTests, BasicTest) {}

/*expression::AbstractExpression *CreatePredicate(
  const std::set<oid_t> &tuple_ids) {
  assert(tuple_ids.size() >= 1);

  expression::AbstractExpression *predicate =
    expression::ExpressionUtil::ConstantValueFactory(Value::GetFalse());

  bool even = false;
  for (oid_t tuple_id : tuple_ids) {
    even = !even;

    // Create equality expression comparison tuple value and constant value.
    // First, create tuple value expression.
    expression::AbstractExpression *tuple_value_expr = nullptr;

    tuple_value_expr =
      even ? expression::ExpressionUtil::TupleValueFactory(0, 0)
           : expression::ExpressionUtil::TupleValueFactory(0, 3);

    // Second, create constant value expression.
    Value constant_value =
      even ? ValueFactory::GetIntegerValue(
        ExecutorTestsUtil::PopulatedValue(tuple_id, 0))
           : ValueFactory::GetStringValue(std::to_string(
        ExecutorTestsUtil::PopulatedValue(tuple_id, 3)));

    expression::AbstractExpression *constant_value_expr =
      expression::ExpressionUtil::ConstantValueFactory(constant_value);

    // Finally, link them together using an equality expression.
    expression::AbstractExpression *equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
        EXPRESSION_TYPE_COMPARE_EQUAL, tuple_value_expr,
        constant_value_expr);

    // Join equality expression to other equality expression using ORs.
    predicate = expression::ExpressionUtil::ConjunctionFactory(
      EXPRESSION_TYPE_CONJUNCTION_OR, predicate, equality_expr);
  }

  return predicate;
}

TEST_F(PlannerTests, SeqScanPlanCopyTest) {
  // const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
  std::unique_ptr<storage::DataTable> table(nullptr);

  const std::vector<oid_t> column_ids({0, 1, 3});
  const std::set<oid_t> g_tuple_ids({0, 3}); 
  expression::AbstractExpression *ex = CreatePredicate(g_tuple_ids);

  planner::SeqScanPlan *old_plan = new planner::SeqScanPlan(table.get(), ex, column_ids);
  const planner::SeqScanPlan *new_plan = dynamic_cast<const planner::SeqScanPlan *>(old_plan->Copy());
  EXPECT_EQ(old_plan->IfEqual(new_plan), true);
  delete new_plan;
  delete old_plan;
} */

}  // End test namespace
}  // End peloton namespace
