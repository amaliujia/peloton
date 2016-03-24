#pragma once

#include "abstract_plan.h"

namespace peloton {
namespace planner {

class AbstractExchangePlan {
public:
  AbstractExchangePlan(const planner::AbstractPlan *plan): old_plan_(plan) {}  

protected:
  std::unique_ptr<const planner::AbstractPlan> old_plan_;
};

}  // namespace planner
}  // namespace peloton
