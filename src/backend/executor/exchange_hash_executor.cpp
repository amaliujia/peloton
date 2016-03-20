#include "backend/planner/exchange_hash_plan.h"
#include "backend/executor/exchange_hash_executor.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
HExchangeHashExecutor::ExchangeHashExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context)
  : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool ExchangeHashExecutor::DInit() {
  assert(children_.size() == 1);

  // Initialize executor state
  done_ = false;
  result_itr = 0;

  return true;
}

/*
 * exchange_hash_executor has only one child, which should be exchange_seq_scan (assume no index).
 * exchange_hash_executor creates parallel task only when it gets logical tiles for exechagne_seq_scan.
 */
bool ExchangeHashExecutor::DExecute() {
  LOG_INFO("Exchange Hash Executor");

  if (done_ == false) {
    const planner::ExchangeHashPlan *node = GetPlanNode<planner::ExchangeHashPlan>();

    // First, get all the input logical tiles
    while (children_[0]->Execute()) {
      child_tiles_.emplace_back(children_[0]->GetOutput());
      // TODO: creates parallel tasks here.
    }

    if (child_tiles_.size() == 0) {
      LOG_TRACE("Hash Executor : false -- no child tiles ");
      return false;
    }

    /* *
     * HashKeys is a vector of TupleValue expr
     * from which we construct a vector of column ids that represent the
     * attributes of the underlying table.
     * The hash table is built on top of these hash key attributes
     * */
    auto &hashkeys = node.GetHashKeys();

    // Construct a logical tile
    for (auto &hashkey : hashkeys) {
      assert(hashkey->GetExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE);
      auto tuple_value =
        reinterpret_cast<const expression::TupleValueExpression *>(
          hashkey.get());
      column_ids_.push_back(tuple_value->GetColumnId());
    }

    // Construct the hash table by going over each child logical tile and
    // hashing
    for (size_t child_tile_itr = 0; child_tile_itr < child_tiles_.size();
         child_tile_itr++) {
      auto tile = child_tiles_[child_tile_itr].get();

      // Go over all tuples in the logical tile
      for (oid_t tuple_id : *tile) {
        // Key : container tuple with a subset of tuple attributes
        // Value : < child_tile offset, tuple offset >
        hash_table_[HashMapType::key_type(tile, tuple_id, &column_ids_)].insert(
          std::make_pair(child_tile_itr, tuple_id));
      }
    }

    done_ = true;
  }

  // Return logical tiles one at a time
  while (result_itr < child_tiles_.size()) {
    if (child_tiles_[result_itr]->GetTupleCount() == 0) {
      result_itr++;
      continue;
    } else {
      SetOutput(child_tiles_[result_itr++].release());
      LOG_TRACE("Hash Executor : true -- return tile one at a time ");
      return true;
    }
  }

  LOG_TRACE("Hash Executor : false -- done ");
  return false;

}

}  // namespace executor
}  // namespace peloton