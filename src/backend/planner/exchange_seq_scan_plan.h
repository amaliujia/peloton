#pragma once

#include "backend/storage/database.h"
#include "backend/expression/abstract_expression.h"
#include "abstract_plan.h"

namespace peloton {
namespace planner {

class ExchangeSeqScanPlan : public AbstractScan {
public:
  ExchangeSeqScanPlan(const ExchangeSeqScanPlan &) = delete;
  ExchangeSeqScanPlan &operator=(const ExchangeSeqScanPlan &) = delete;
  ExchangeSeqScanPlan(ExchangeSeqScanPlan &&) = delete;
  ExchangeSeqScanPlan &operator=(ExchangeSeqScanPlan &&) = delete;


  ExchangeSeqScanPlan(const SeqScanPlan *seq_scan_plan)
    : AbstractScan(seq_scan_plan->GetTable(),
                   const_cast<expression::AbstractExpression *>(seq_scan_plan->GetPredicate()),
                   seq_scan_plan->GetColumnIds()),
      old_plan(seq_scan_plan) {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_EXCHANGE_SEQSCAN; }

  const std::string GetInfo() const { return "ExchangeSeqScan"; }

private:
  std::unique_ptr<const planner::AbstractPlan> old_plan;
};

}  // namespace planner
}  // namespace peloton
