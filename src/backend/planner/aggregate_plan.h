//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_plan.h
//
// Identification: src/backend/planner/aggregate_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <backend/common/vector_comparator.h>
#include "backend/planner/abstract_plan.h"
#include "backend/planner/project_info.h"
#include "backend/common/types.h"

namespace peloton {

namespace expression {
class AbstractExpression;
}

namespace planner {

class AggregatePlan : public AbstractPlan {
 public:
  AggregatePlan() = delete;
  AggregatePlan(const AggregatePlan &) = delete;
  AggregatePlan &operator=(const AggregatePlan &) = delete;
  AggregatePlan(AggregatePlan &&) = delete;

  class AggTerm {
   public:
    ExpressionType aggtype;
    const expression::AbstractExpression *expression;
    bool distinct;

    bool operator<( const AggTerm& other ) const {
       if(aggtype < other.aggtype) {
         return true;
       } else if (aggtype > other.aggtype){
         return false;
       } 

       if (expression < other.expression) {
         return true;
       } else if (expression > other.expression) {
         return false;
       }

       if (distinct == true && other.distinct == false) {
         return true;
       }

       return false;
    }

    AggTerm(ExpressionType et, expression::AbstractExpression *expr,
            bool distinct = false)
        : aggtype(et), expression(expr), distinct(distinct) {}
  };

  AggregatePlan(const planner::ProjectInfo *project_info,
                const expression::AbstractExpression *predicate,
                const std::vector<AggTerm> &&unique_agg_terms,
                const std::vector<oid_t> &&groupby_col_ids,
                const catalog::Schema *output_schema,
                PelotonAggType aggregate_strategy)
      : project_info_(project_info),
        predicate_(predicate),
        unique_agg_terms_(unique_agg_terms),
        groupby_col_ids_(groupby_col_ids),
        output_schema_(output_schema),
        agg_strategy_(aggregate_strategy) {}

  const std::vector<oid_t> &GetGroupbyColIds() const {
    return groupby_col_ids_;
  }

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  const std::vector<AggTerm> &GetUniqueAggTerms() const {
    return unique_agg_terms_;
  }

  const catalog::Schema *GetOutputSchema() const {
    return output_schema_.get();
  }

  PelotonAggType GetAggregateStrategy() const { return agg_strategy_; }

  inline PlanNodeType GetPlanNodeType() const {
    return PlanNodeType::PLAN_NODE_TYPE_AGGREGATE_V2;
  }

  ~AggregatePlan() {
    for (auto term : unique_agg_terms_) {
      delete term.expression;
    }
  }

  void SetColumnIds(const std::vector<oid_t> &column_ids) {
    column_ids_ = column_ids;
  }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  const AbstractPlan *Copy() const {
    AggregatePlan *new_plan = new AggregatePlan(project_info_.get(), predicate_.get(),
                                                std::vector<AggTerm>(unique_agg_terms_),
                                                std::vector<oid_t> (groupby_col_ids_),
                                                output_schema_.get(), agg_strategy_);
    return new_plan;
  }

  bool IfEqual(const AggregatePlan *plan) {
    VectorComparator<AggTerm> aggterm_comp;
    VectorComparator<oid_t> oid_comp;

    return plan->GetProjectInfo() == project_info_.get() &&
           plan->GetPredicate() == predicate_.get() &&
           aggterm_comp.Compare(plan->GetUniqueAggTerms(), unique_agg_terms_) &&
           oid_comp.Compare(plan->GetGroupbyColIds(), groupby_col_ids_) &&
           oid_comp.Compare(plan->GetColumnIds(), column_ids_) &&
           output_schema_.get() == plan->GetOutputSchema() &&
           plan->GetAggregateStrategy() == agg_strategy_;
  }

 private:
  /* For projection */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  /* For HAVING clause */
  std::unique_ptr<const expression::AbstractExpression> predicate_;

  /* Unique aggregate terms */
  const std::vector<AggTerm> unique_agg_terms_;

  /* Group-by Keys */
  const std::vector<oid_t> groupby_col_ids_;

  /* Output schema */
  std::unique_ptr<const catalog::Schema> output_schema_;

  /* Aggregate Strategy */
  const PelotonAggType agg_strategy_;

  /** @brief Columns involved */
  std::vector<oid_t> column_ids_;
};
}
}
