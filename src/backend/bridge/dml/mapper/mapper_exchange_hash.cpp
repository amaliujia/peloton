#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/common/cuckoohash_map.h"

namespace peloton {
namespace bridge {

const planner::AbstractPlan *PlanTransformer::BuildParallelHashPlan( __attribute__((unused)) const planner::AbstractPlan *old_plan) {
  LOG_TRACE("Mapping hash plan to parallel seq scan plan (add exchange hash operator)");
  return nullptr;
}

}  // namespace bridge
}  // namespace peloton
