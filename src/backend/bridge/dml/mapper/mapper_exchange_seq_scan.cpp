#include <backend/planner/seq_scan_plan.h>
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/exchange_seq_scan_plan.h"

namespace peloton {
namespace bridge {

const planner::AbstractPlan *BuildParallelSeqScanPlan(const planner::AbstractPlan *seq_scan_plan) {
  /* Grab the target table */
  LOG_TRACE("Mapper seq scan plan to parallel seq scan plan");
  const planner::SeqScanPlan *plan = dynamic_cast<const planner::SeqScanPlan *>(seq_scan_plan); 
  storage::DataTable *target_table = plan->GetTable();

  // TODO: it is possbile target_table is nullptr, aka seq_scan_plan has child plan (just a guess). Need handle
  // This case.
  assert(target_table);
  LOG_INFO("ExchangeSeqScan: database table: %s", target_table->GetName().c_str());

  const expression::AbstractExpression *predicate =
    plan->GetPredicate();
  std::vector<oid_t> column_ids(plan->GetColumnIds());

  // Creates a bunch of seq_scan_plan nodes.
  // auto exchange_seq_scan_plan = new planner::ExchangeSeqScanPlan(target_table, predicate, column_ids);
  auto exchange_seq_scan_plan = new planner::ExchangeSeqScanPlan();

  oid_t current_tile_group_offset = START_OID;
  oid_t total_tile_group_count = target_table->GetTileGroupCount();

  while (current_tile_group_offset < total_tile_group_count) {
    exchange_seq_scan_plan->AddChild(new planner::SeqScanPlan(target_table, predicate, column_ids, current_tile_group_offset));
    current_tile_group_offset++;
  }

  return exchange_seq_scan_plan;
}

}  // namespace bridge
}  // namespace peloton
