#include <backend/storage/tile_group_header.h>
#include <backend/expression/container_tuple.h>
#include <backend/planner/seq_scan_plan.h>
#include "backend/common/thread_manager.h"
#include "backend/executor/exchange_seq_scan_executor.h"
#include "backend/executor/parallel_seq_scan_task_response.h"
#include "logical_tile_factory.h"

namespace peloton {
namespace executor {

ExchangeSeqScanExecutor::ExchangeSeqScanExecutor(const planner::AbstractPlan *node,
                                           ExecutorContext *executor_context)
  : AbstractScanExecutor(node, executor_context) {}


bool ExchangeSeqScanExecutor::DInit() {
  parallelize_done_ = false;

  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  // Grab data from plan node.
  const planner::SeqScanPlan &node = GetPlanNode<planner::SeqScanPlan>();

  target_table_ = node.GetTable();

  current_tile_group_offset_ = START_OID;

  if (target_table_ != nullptr) {
    table_tile_group_count_ = target_table_->GetTileGroupCount();

    if (column_ids_.empty()) {
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }

  return true;
}

bool ExchangeSeqScanExecutor::ThreadExecute(oid_t current_tile_group_offset_) {
  auto tile_group =
    target_table_->GetTileGroup(current_tile_group_offset_);

  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

  auto transaction_ = executor_context_->GetTransaction();
  txn_id_t txn_id = transaction_->GetTransactionId();
  cid_t commit_id = transaction_->GetLastCommitId();
  oid_t active_tuple_count = tile_group->GetNextTupleSlot();

  // Print tile group visibility
  // tile_group_header->PrintVisibility(txn_id, commit_id);

  // Construct logical tile.
  std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
  logical_tile->AddColumns(tile_group, column_ids_);

  // Construct position list by looping through tile group
  // and applying the predicate.
  std::vector<oid_t> position_list;
  for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
    if (tile_group_header->IsVisible(tuple_id, txn_id, commit_id) ==
        false) {
      continue;
    }

    expression::ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                         tuple_id);
    if (predicate_ == nullptr) {
      position_list.push_back(tuple_id);
    } else {
      auto eval =
        predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
      if (eval == true) position_list.push_back(tuple_id);
    }
  }

  logical_tile->AddPositionList(std::move(position_list));

  // Don't return empty tiles
  if (0 == logical_tile->GetTupleCount()) {
    return false;
  }

  SetOutput(logical_tile.release());
  return true;
}

bool ExchangeSeqScanExecutor::DExecute() {
  LOG_INFO("Exchange Seq Scan executor:: start execute, children_size %lu", children_.size());

  if (!parallelize_done_) {
    // When exchange_seq_scan_executor reads from a table, just parallelize each part of this table.
    if (children_.size() == 0) {
      assert(target_table_ != nullptr);
      assert(column_ids_.size() != 0);

      while (current_tile_group_offset_ <  table_tile_group_count_) {
        LOG_TRACE("ExchangeSeqScanExecutor :: submit task to thread pool, dealing with %lu tile.",
                  current_tile_group_offset_);
        std::function<void()> f_seq_scan = std::bind(&ExchangeSeqScanExecutor::SeqScanThreadMain, this,
                                                     this, current_tile_group_offset_, &queue_);
        ThreadManager::GetQueryExecutionPool().AddTask(f_seq_scan);

        current_tile_group_offset_++;
      }

      parallelize_done_ = true;
    } else {
       // When exchange_seq_scan_executor, should use a single thread to create parallel tasks.
       // TODO: Use one single thead here to keep fetching logical tiles from child_, ignore it now.
        assert(children_.size() == 1);
        LOG_TRACE("Exchange Seq Scan executor :: 1 child ");
        assert(target_table_ == nullptr);
        assert(column_ids_.size() == 0);

        while (children_[0]->Execute()) {
          std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

          if (predicate_ != nullptr) {
          // Invalidate tuples that don't satisfy the predicate.
            for (oid_t tuple_id : *tile) {
              expression::ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
              if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
                .IsFalse()) {
                tile->RemoveVisibility(tuple_id);
              }
            }
          }

          if (0 == tile->GetTupleCount()) {  // Avoid returning empty tiles
            continue;
          }

          SetOutput(tile.release());
          return true;
      }
      return false;
    }
  }

  // if (children_.size() == 0
  current_tile_group_offset_ = START_OID;

  while (current_tile_group_offset_ < table_tile_group_count_) {
    std::unique_ptr<AbstractParallelTaskResponse> response_ptr(queue_.GetTask());
    current_tile_group_offset_++;
    if (response_ptr->GetStatus() == HasRetValue) {
      SetOutput(response_ptr->GetOutput());
      return true;
    }
  }
  return false;

  // else {
}

void ExchangeSeqScanExecutor::SeqScanThreadMain(oid_t current_tile_group_offset_) {
  LOG_INFO("Parallel worker :: ExchangeSeqScanExecutor :: SeqScanThreadMain, executor: %s", GetRawNode()->GetInfo().c_str());
  bool ret = ThreadExecute(current_tile_group_offset_);
  // bool ret = executor->ThreadExecute(current_tile_group_offset_);
  AbstractParallelTaskResponse *response = nullptr;

  if (ret) {
    LogicalTile *logical_tile = GetOutput();
    response = new ParallelSeqScanTaskResponse(HasRetValue, logical_tile) ;
  } else {
    response = new ParallelSeqScanTaskResponse(NoRetValue, nullptr);
  }
  queue_.PutTask(response);
}

}  // executor
}  // peloton
