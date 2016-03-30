//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_plan.h
//
// Identification: src/backend/planner/hash_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {
namespace planner {

/**
 * @brief
 *
 */
class HashPlan : public AbstractPlan {
 public:
  HashPlan(const HashPlan &) = delete;
  HashPlan &operator=(const HashPlan &) = delete;
  HashPlan(const HashPlan &&) = delete;
  HashPlan &operator=(const HashPlan &&) = delete;

  typedef const expression::AbstractExpression HashKeyType;
  typedef std::unique_ptr<HashKeyType> HashKeyPtrType;

  HashPlan(std::vector<HashKeyPtrType> &hashkeys)
      : hash_keys_(std::move(hashkeys)) {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_HASH; }

  const std::string GetInfo() const { return "Hash"; }

  inline const std::vector<HashKeyPtrType> &GetHashKeys() const {
    return this->hash_keys_;
  }

  const AbstractPlan *Copy() const {
    std::vector<HashKeyPtrType> copied_hash_keys(hash_keys_);
    return new HashPlan(copied_hash_keys);
  }

  bool IfEqual(const HashPlan *plan) {
     auto keys = plan->GetHashKeys();
     std::set<const expression::AbstractExpression *> plan_set;
     for (const auto& key : keys) {
       plan_set.insert(key.get());
     }

    std::set<const expression::AbstractExpression *> self_set;
    for (const auto& key : hash_keys_) {
      self_set.insert(key.get());
    }

    return self_set == plan_set;
  }

 private:
  std::vector<HashKeyPtrType> hash_keys_;
};
}
}
