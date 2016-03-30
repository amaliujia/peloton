//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_plan.h
//
// Identification: src/backend/planner/index_scan_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <backend/common/vector_comparator.h>

#include "backend/planner/abstract_scan_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {
class Tuple;
}

namespace planner {

class IndexScanPlan : public AbstractScan {
 public:
  IndexScanPlan(const IndexScanPlan &) = delete;
  IndexScanPlan &operator=(const IndexScanPlan &) = delete;
  IndexScanPlan(IndexScanPlan &&) = delete;
  IndexScanPlan &operator=(IndexScanPlan &&) = delete;

  struct IndexScanDesc {
    IndexScanDesc() : index(nullptr) {}

    IndexScanDesc(
        index::Index *index, const std::vector<oid_t> &column_ids,
        const std::vector<ExpressionType> &expr_types,
        const std::vector<Value> &values,
        const std::vector<expression::AbstractExpression *> &runtime_keys)
        : index(index),
          key_column_ids(column_ids),
          expr_types(expr_types),
          values(values),
          runtime_keys(runtime_keys) {}

    index::Index *index = nullptr;

    std::vector<oid_t> key_column_ids;

    std::vector<ExpressionType> expr_types;

    std::vector<Value> values;

    std::vector<expression::AbstractExpression *> runtime_keys;
  };

  IndexScanPlan(storage::DataTable *table,
                expression::AbstractExpression *predicate,
                const std::vector<oid_t> &column_ids,
                const IndexScanDesc &index_scan_desc)
      : AbstractScan(table, predicate, column_ids),
        index_(index_scan_desc.index),
        column_ids_(column_ids),
        key_column_ids_(std::move(index_scan_desc.key_column_ids)),
        expr_types_(std::move(index_scan_desc.expr_types)),
        values_(std::move(index_scan_desc.values)),
        runtime_keys_(std::move(index_scan_desc.runtime_keys)) {}

  ~IndexScanPlan() {
    for (auto expr : runtime_keys_) {
      delete expr;
    }
  }

  index::Index *GetIndex() const { return index_; }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  const std::vector<oid_t> &GetKeyColumnIds() const { return key_column_ids_; }

  const std::vector<ExpressionType> &GetExprTypes() const {
    return expr_types_;
  }

  const std::vector<Value> &GetValues() const { return values_; }

  const std::vector<expression::AbstractExpression *> &GetRunTimeKeys() const {
    return runtime_keys_;
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_INDEXSCAN;
  }

  const std::string GetInfo() const { return "IndexScan"; }

  const AbstractPlan *Copy() const {
    IndexScanDesc desc(index_, key_column_ids_, expr_types_, values_, runtime_keys_);
    IndexScanPlan *new_plan = new IndexScanPlan(GetTable(),
                                                  const_cast<expression::AbstractExpression *>(GetPredicate()),
                                                   GetColumnIds(), desc);
    return new_plan;
  }

  bool IfEqual(const IndexScanPlan *plan) {
    VectorComparator<oid_t> oid_comparator;
    VectorComparator<ExpressionType> expr_type_comparator;
    // VectorComparator<Value> value_comparator;
    VectorComparator<expression::AbstractExpression *> expr_ptr_comparator;

    return expr_ptr_comparator.Compare(plan->GetRunTimeKeys(), runtime_keys_) &&
           oid_comparator.Compare(plan->GetColumnIds(), column_ids_) &&
           oid_comparator.Compare(key_column_ids_, plan->GetKeyColumnIds()) &&
           expr_type_comparator.Compare(plan->GetExprTypes(), expr_types_) &&
//           value_comparator.Compare(plan->GetValues(), values_) &&
           index_ == plan->GetIndex() &&
            AbstractScan::IfEqual(plan);
  }

 private:
  /** @brief index associated with index scan. */
  index::Index *index_;

  const std::vector<oid_t> column_ids_;

  const std::vector<oid_t> key_column_ids_;

  const std::vector<ExpressionType> expr_types_;

  const std::vector<Value> values_;

  const std::vector<expression::AbstractExpression *> runtime_keys_;
};

}  // namespace planner
}  // namespace peloton
