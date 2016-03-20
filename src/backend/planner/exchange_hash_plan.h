#pragma once

#include "backend/planner/abstract_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {

namespace planner {

class ExchangeHashPlan : public AbstractPlan {
  ExchangeHashPlan(const ExchangeHashPlan &) = delete;
  ExchangeHashPlan &operator=(const ExchangeHashPlan &) = delete;
  ExchangeHashPlan(ExchangeHashPlan &&) = delete;
  ExchangeHashPlan &operator=(ExchangeHashPlan &&) = delete;

  typedef const expression::AbstractExpression HashKeyType;
  typedef std::unique_ptr<HashKeyType> HashKeyPtrType;

  ExchangeHashPlan(std::vector<HashKeyPtrType> &hashkeys)
  : hash_keys_(std::move(hashkeys)) {}

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