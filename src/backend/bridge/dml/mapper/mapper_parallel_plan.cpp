#include <backend/planner/seq_scan_plan.h>
#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

/**
 * There are two ways to do such mapping.
 * 1. Plan level parallelism. For each type of plan, has one function to do mapping.
 * 2. Plan node level parallelism. For each type of node, has one function to do mapping.
 *
 * Here second solution is adopted.
 */
const planner::AbstractPlan *PlanTransformer::BuildParallelPlan(const planner::AbstractPlan *old_plan) {
  LOG_TRACE("Mapping single-threaded plan to parallel plan");

  switch (old_plan->GetPlanNodeType()) {
    case PLAN_NODE_TYPE_SEQSCAN:
      return BuildParallelSeqScanPlan(old_plan);
    case PLAN_NODE_TYPE_HASH:
      return BuildParallelHashPlan(old_plan);
    default:
      return old_plan;
  }
}

}  // namespace bridge
}  // namespace peloton