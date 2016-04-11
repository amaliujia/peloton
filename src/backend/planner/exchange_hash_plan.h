#pragma once

#include "backend/planner/abstract_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/hash_plan.h"
#include "backend/planner/abstract_exchange_plan.h"

namespace peloton {
namespace planner {

class ExchangeHashPlan : public AbstractPlan, public AbstractExchangePlan {
public:
  ExchangeHashPlan() = delete;
  ExchangeHashPlan(const ExchangeHashPlan &) = delete;
  ExchangeHashPlan &operator=(const ExchangeHashPlan &) = delete;
  ExchangeHashPlan(ExchangeHashPlan &&) = delete;
  ExchangeHashPlan &operator=(ExchangeHashPlan &&) = delete;

  typedef const expression::AbstractExpression HashKeyType;
  typedef std::unique_ptr<HashKeyType> HashKeyPtrType;
 
  ExchangeHashPlan(const planner::HashPlan *hash_plan): AbstractExchangePlan(hash_plan)
  {
    for (const auto& hashkey : hash_plan->GetHashKeys()) {
      hash_keys_.push_back(hashkey.get());
    }
  }
  
  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_EXCHANGE_HASH; }

  const std::string GetInfo() const { return "ExchangeHash"; }

  inline const std::vector<HashKeyType *> &GetHashKeys() const {
    return this->hash_keys_;
  }

private:
  std::vector<HashKeyType *> hash_keys_;
};

}  // namespace planner
}  // namespace peloton
