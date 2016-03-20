#include "backend/planner/seq_scan_plan.h"
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/exchange_seq_scan_plan.h"

namespace peloton {
namespace bridge {

static std::vector<planner::SeqScanPlan *> BuildParallelSeqScanPlanUtil(const planner::SeqScanPlan *seq_scan_plan) {
  storage::DataTable *target_table = seq_scan_plan->GetTable();

  // TODO: it is possbile target_table is nullptr, aka seq_scan_plan has child plan (just a guess). Need handle
  // This case.
  assert(target_table);
  LOG_INFO("ExchangeSeqScan: database table: %s", target_table->GetName().c_str());
  expression::AbstractExpression *predicate = const_cast<expression::AbstractExpression *>(seq_scan_plan->GetPredicate());
  std::vector<oid_t> column_ids(seq_scan_plan->GetColumnIds());

  oid_t current_tile_group_offset = START_OID;
  oid_t total_tile_group_count = target_table->GetTileGroupCount();

  std::vector<planner::SeqScanPlan *> ret_vec;
  while (current_tile_group_offset < total_tile_group_count) {
    ret_vec.push_back(new planner::SeqScanPlan(target_table, predicate, column_ids, current_tile_group_offset));
    current_tile_group_offset++;
  }

  return ret_vec;
}

const planner::AbstractPlan *PlanTransformer::BuildParallelSeqScanPlan(const planner::AbstractPlan *seq_scan_plan) {
  /* Grab the target table */
  LOG_TRACE("Mapping seq scan plan to parallel seq scan plan (add exchange seq scan operator)");
  const planner::SeqScanPlan *plan = dynamic_cast<const planner::SeqScanPlan *>(seq_scan_plan);
  auto sub_plans = BuildParallelSeqScanPlanUtil(plan);

  // Creates a bunch of seq_scan_plan nodes.
  // auto exchange_seq_scan_plan = new planner::ExchangeSeqScanPlan(target_table, predicate, column_ids);
  auto exchange_seq_scan_plan = new planner::ExchangeSeqScanPlan();

  for (auto sub_plan : sub_plans) {
    exchange_seq_scan_plan->AddChild(sub_plan);
  }

  return exchange_seq_scan_plan;
}

}  // namespace bridge
}  // namespace peloton
