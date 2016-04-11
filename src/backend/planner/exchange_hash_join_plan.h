//
// Created by 王 瑞 on 16-4-11.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_join_plan.h"
#include "backend/planner/hash_join_plan.h"
#include "backend/planner/project_info.h"

namespace peloton {
namespace planner {

class ExchangeHashJoinPlan : public AbstractJoinPlan {
public:
  ExchangeHashJoinPlan(const ExchangeHashJoinPlan &) = delete;
  ExchangeHashJoinPlan &operator=(const ExchangeHashJoinPlan &) = delete;
  ExchangeHashJoinPlan(ExchangeHashJoinPlan &&) = delete;
  ExchangeHashJoinPlan &operator=(ExchangeHashJoinPlan &&) = delete;

  ExchangeHashJoinPlan(PelotonJoinType join_type,
               const expression::AbstractExpression *predicate,
               const ProjectInfo *proj_info, const catalog::Schema *proj_schema)
    : AbstractJoinPlan(join_type, predicate, proj_info, proj_schema) {}

  ExchangeHashJoinPlan(PelotonJoinType join_type,
               const expression::AbstractExpression *predicate,
               const ProjectInfo *proj_info, const catalog::Schema *proj_schema,
               const std::vector<oid_t> &
               outer_hashkeys)  // outer_hashkeys is added for IN-subquery
    : AbstractJoinPlan(join_type, predicate, proj_info, proj_schema) {
    outer_column_ids_ = outer_hashkeys;  // added for IN-subquery
  }

  ExchangeHashJoinPlan(const HashJoinPlan *plan)  // outer_hashkeys is added for IN-subquery
    : AbstractJoinPlan(plan->GetJoinType(), plan->GetPredicate(), plan->GetProjInfo(), plan->GetSchema()) {
    outer_column_ids_ = plan->GetOuterHashIds();  // added for IN-subquery
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_EXCHANGE_HASHJOIN;
  }

  const std::string GetInfo() const { return "ExchangeHashJoin"; }

  const std::vector<oid_t> &GetOuterHashIds() const {
    return outer_column_ids_;
  }

private:
  std::vector<oid_t> outer_column_ids_;
};

}  // namespace planner
}  // namespace peloton
