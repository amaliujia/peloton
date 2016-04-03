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

}  // End test namespace
}  // End peloton namespace
