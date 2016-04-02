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

#include "executor/executor_tests_util.h"
#include "backend/common/value_factory.h"
#include <gmock/gtest/gtest.h>
#include "backend/storage/data_table.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/seq_scan_plan.h"

#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Planner Tests
//===--------------------------------------------------------------------===//

class PlannerTests : public PelotonTest {};

TEST_F(PlannerTests, BasicTest) {}


expression::AbstractExpression *CreatePredicate(
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
  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable());

  const std::vector<oid_t> column_ids({0, 1, 3});
  expression::AbstractExpression *ex = CreatePredicate(column_ids);

  SeqScanPlan *old_plan = new SeqScanPlan(table.get(), ex, column_ids);
  SeqScanPlan *new_plan = static_cast<SeqScanPlan *>(old_plan->Copy());
  EXPECT_EQ(old_plan->IfEqual(new_plan), true);
}

}  // End test namespace
}  // End peloton namespace
