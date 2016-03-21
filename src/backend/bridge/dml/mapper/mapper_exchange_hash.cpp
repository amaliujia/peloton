#include "backend/planner/hash_plan.h"
#include "backend/planner/exchange_hash_plan.h"
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/common/cuckoohash_map.h"

namespace peloton {
namespace bridge {

static const planner::AbstractPlan *BuildParallelHashPlanUtil(const planner::HashPlan *old_plan) {
  auto hashkeys = const_cast<std::vector<std::unique_ptr<const expression::AbstractExpression>> *>(old_plan->GetHashKeysPtr());
  const planner::AbstractPlan *exchange_hash_plan = new planner::ExchangeHashPlan(hashkeys);
  return exchange_hash_plan;
}

const planner::AbstractPlan *PlanTransformer::BuildParallelHashPlan(const planner::AbstractPlan *old_plan) {
  LOG_TRACE("Mapping hash plan to parallel seq scan plan (add exchange hash operator)");
  const planner::HashPlan *plan = dynamic_cast<const planner::HashPlan *>(old_plan);
  const planner::AbstractPlan *ret_ptr = BuildParallelHashPlanUtil(plan);
  return ret_ptr;
}

}  // namespace bridge
}  // namespace peloton
