#include <backend/planner/hash_plan.h>
#include "backend/planner/exchange_seq_scan_plan.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

static planner::AbstractPlan *BuildParallelHashPlan(const planner::AbstractPlan *old_plan) {
  LOG_TRACE("Mapping hash plan to parallel seq scan plan (add exchange hash operator)");
  const planner::HashPlan *plan = dynamic_cast<const planner::HashPlan *>(old_plan);
//  auto hashkeys = const_cast<std::vector<std::unique_ptr<const expression::AbstractExpression>> *>(old_plan->GetHashKeysPtr());
//  planner::AbstractPlan *exchange_hash_plan = new planner::ExchangeHashPlan(hashkeys);
  return nullptr;
}

static planner::AbstractPlan *BuildParallelSeqScanPlan(const planner::AbstractPlan *seq_scan_plan) {
  /* Grab the target table */
  LOG_TRACE("Mapping seq scan plan to parallel seq scan plan (add exchange seq scan operator)");
  const planner::SeqScanPlan *plan = dynamic_cast<const planner::SeqScanPlan *>(seq_scan_plan);
  planner::AbstractPlan * exchange_seq_scan_plan = new planner::ExchangeSeqScanPlan(plan);
  return exchange_seq_scan_plan;
}

static planner::AbstractPlan *BuildParallelPlanUtil(const planner::AbstractPlan *old_plan) {
  planner::AbstractPlan *ret_ptr = nullptr;
  switch (old_plan->GetPlanNodeType()) {
    case PLAN_NODE_TYPE_SEQSCAN:
      return BuildParallelSeqScanPlan(old_plan);
    case PLAN_NODE_TYPE_HASH:
      return BuildParallelHashPlan(old_plan);
    case PLAN_NODE_TYPE_HASHJOIN:
      // TODO: HashJoin
    default:
      return const_cast<planner::AbstractPlan *>(old_plan);
  }
}

/**
 * There are two ways to do such mapping.
 * 1. Plan level parallelism. For each type of plan, has one function to do mapping.
 * 2. Plan node level parallelism. For each type of node, has one function to do mapping.
 *
 * Here second solution is adopted.
 */
const planner::AbstractPlan *PlanTransformer::BuildParallelPlan(const planner::AbstractPlan *old_plan) {
  LOG_TRACE("Mapping single-threaded plan to parallel plan");

  // Base case: leaf plan node
  if (old_plan->GetChildren().size() == 0) {
    return BuildParallelPlanUtil(old_plan);
  } else {
    planner::AbstractPlan *ret_ptr = nullptr;
    std::vector<const planner::AbstractPlan *> child_plan_vec;

    for (const planner::AbstractPlan *child : old_plan->GetChildren()) {
      child_plan_vec.push_back(BuildParallelPlanUtil(child));
    }

    ret_ptr = BuildParallelPlanUtil(old_plan);
    ret_ptr->ClearChild();
    for (auto child_plan : child_plan_vec) {
      ret_ptr->AddChild(child_plan);
    }

    return ret_ptr;
  }
}

}  // namespace bridge
}  // namespace peloton