//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// seq_scan_node.h
//
// Identification: src/backend/planner/seq_scan_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_scan_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class SeqScanPlan : public AbstractScan {
 public:
  SeqScanPlan(const SeqScanPlan &) = delete;
  SeqScanPlan &operator=(const SeqScanPlan &) = delete;
  SeqScanPlan(SeqScanPlan &&) = delete;
  SeqScanPlan &operator=(SeqScanPlan &&) = delete;

  SeqScanPlan(storage::DataTable *table,
              expression::AbstractExpression *predicate,
              const std::vector<oid_t> &column_ids)
      : AbstractScan(table, predicate, column_ids) {}

  SeqScanPlan(storage::DataTable *table,
              expression::AbstractExpression *predicate,
              const std::vector<oid_t> &column_ids,
              oid_t tile_gourp_offset)
    : AbstractScan(table, predicate, column_ids), assigned_tile_group_offset_(tile_gourp_offset){}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_SEQSCAN; }

  const std::string GetInfo() const { return "SeqScan"; }

  const oid_t GetAssignedTileGroupOffset() const {return assigned_tile_group_offset_;}

private:
  oid_t assigned_tile_group_offset_ = INVALID_OID;
};

}  // namespace planner
}  // namespace peloton
