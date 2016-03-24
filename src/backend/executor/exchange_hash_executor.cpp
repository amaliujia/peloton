#include <utility>
#include <vector>
#include <backend/common/thread_manager.h>

#include "backend/planner/exchange_hash_plan.h"
#include "backend/executor/exchange_hash_executor.h"
#include "backend/common/logger.h"
#include "backend/common/value.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/hash_executor.h"
#include "backend/planner/hash_plan.h"
#include "backend/expression/tuple_value_expression.h"
#include "parallel_seq_scan_task_response.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
ExchangeHashExecutor::ExchangeHashExecutor(const planner::AbstractPlan *node,
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

void ExchangeHashExecutor::BuildHashTableThreadMain(HashMapType *table, LogicalTile *tile,
                                                    const std::vector<const expression::AbstractExpression *> &hashkeys,
                                                    BlockingQueue<AbstractParallelTaskResponse *> *queue) {
  /* *
    * HashKeys is a vector of TupleValue expr
    * from which we construct a vector of column ids that represent the
    * attributes of the underlying table.
    * The hash table is built on top of these hash key attributes
    * */
  // Construct a logical tile
  for (auto &hashkey : hashkeys) {
    assert(hashkey->GetExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE);
    auto tuple_value =
      reinterpret_cast<const expression::TupleValueExpression *>(
        hashkey);
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
      // TODO: test cuckoohash_map
      table->insert(HashMapType::key_type(tile, tuple_id, &column_ids_), std::make_pair(child_tile_itr, tuple_id));
    }
  }

  auto response = new ParallelSeqScanTaskResponse(NoRetValue, nullptr);
  queue->PutTask(response);
}

/*
 * exchange_hash_executor has only one child, which should be exchange_seq_scan (assume no index).
 * exchange_hash_executor creates parallel task only when it gets logical tiles for exechagne_seq_scan.
 */
bool ExchangeHashExecutor::DExecute() {
  LOG_INFO("Exchange Hash Executor");

  if (done_ == false) {
    const planner::ExchangeHashPlan& node = GetPlanNode<planner::ExchangeHashPlan>();

    // First, get all the input logical tiles
    while (children_[0]->Execute()) {
      auto tile = children_[0]->GetOutput();
      child_tiles_.emplace_back(tile);
      std::function<void()> f_build_hash_table = std::bind(&ExchangeHashExecutor::BuildHashTableThreadMain, this,
                                                   &hash_table_, tile, node.GetHashKeys(), &queue_);
      ThreadManager::GetQueryExecutionPool().AddTask(f_build_hash_table);
    }

    if (child_tiles_.size() == 0) {
      LOG_TRACE("Hash Executor : false -- no child tiles ");
      return false;
    }
    done_ = true;
  }

  // make sure building hashmap is done before return any child tiles.
  int task_count = 0;
  while (task_count < child_tiles_.size()) {
    queue_.GetTask();
    task_count++;
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
