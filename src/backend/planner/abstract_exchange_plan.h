#pragma once

#include "abstract_plan.h"

namespace peloton {
namespace planner {

class AbstractExchangePlan {
protected:
  std::unique_ptr<const planner::AbstractPlan> old_plan;
};

}  // namespace planner
}  // namespace peloton