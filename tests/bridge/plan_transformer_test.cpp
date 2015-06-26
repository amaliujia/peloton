/*-------------------------------------------------------------------------
 *
 * plan_transformer_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * Author: Ming Fang
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"


#include "harness.h"
#include "backend/bridge/bridge.h"
#include "backend/bridge/plan_transformer.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Plan Transformer Tests
//===--------------------------------------------------------------------===//

TEST(PlanTransformerTests, PrintPlanTest) {
  std::string query = "SELECT * FROM pg_class;";
  PlanState *planstate = PlanSimpleQuery(query.c_str());
  bridge::PlanTransformer::GetInstance().printPostgresPlanStateTree(planstate);
}

} // End test namespace
} // End nstore namespace

