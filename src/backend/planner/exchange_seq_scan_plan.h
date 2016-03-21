#pragma once

#include "backend/storage/database.h"
#include "backend/expression/abstract_expression.h"
#include "abstract_plan.h"

namespace peloton {
namespace planner {

class ExchangeSeqScanPlan : public AbstractPlan {
public:
  ExchangeSeqScanPlan(const ExchangeSeqScanPlan &) = delete;
  ExchangeSeqScanPlan &operator=(const ExchangeSeqScanPlan &) = delete;
  ExchangeSeqScanPlan(ExchangeSeqScanPlan &&) = delete;
  ExchangeSeqScanPlan &operator=(ExchangeSeqScanPlan &&) = delete;

  ExchangeSeqScanPlan() {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_EXCHANGE_SEQSCAN; }

  const std::string GetInfo() const { return "ExchangeSeqScan"; }

};

}  // namespace planner
}  // namespace peloton
