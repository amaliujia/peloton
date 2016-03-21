#pragma once

#include "backend/planner/abstract_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {

namespace planner {

class ExchangeHashPlan : public AbstractPlan {
public:
  ExchangeHashPlan(const ExchangeHashPlan &) = delete;
  ExchangeHashPlan &operator=(const ExchangeHashPlan &) = delete;
  ExchangeHashPlan(ExchangeHashPlan &&) = delete;
  ExchangeHashPlan &operator=(ExchangeHashPlan &&) = delete;

  typedef const expression::AbstractExpression HashKeyType;
  typedef std::unique_ptr<HashKeyType> HashKeyPtrType;
 
  ExchangeHashPlan(std::vector<std::unique_ptr<const expression::AbstractExpression>> *hashkeys)
    : hash_keys_(std::move(*hashkeys)) {}
  
  /*ExchangeHashPlan(const std::vector<HashKeyPtrType> &hashkeys) {
    for (size_t i = 0; i < hashkeys.size(); i++) {
      hash_keys_.push_back(std::unique_ptr<HashKeyType>(hashkeys[i].release()));
    }
  }*/

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_HASH; }

  const std::string GetInfo() const { return "ExchangeHash"; }

  inline const std::vector<HashKeyPtrType> &GetHashKeys() const {
    return this->hash_keys_;
  }

private:
  std::vector<HashKeyPtrType> hash_keys_;
};

}  // namespace planner
}  // namespace peloton
