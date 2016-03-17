//
// Created by 王 瑞 on 16-3-16.
//

#include "exchange_seq_scan_executor.h"

namespace peloton {
namespace executor {


ExchangeSeqScanExecutor::ExchangeSeqScanExecutor(const planner::AbstractPlan *node,
                                           ExecutorContext *executor_context)
  : AbstractExecutor(node, executor_context) {}


bool ExchangeSeqScanExecutor::DInit() {
  assert(children_.size() != 0);
  current_tile_offset_ = 0;
  total_tile_count_ = children_.size();


  // Init thread pool
  // TODO: it should not here, but for now let's just put it here
  return true;
}

bool ExchangeSeqScanExecutor::DExecute() {
  LOG_INFO("Exchange Seq Scan executor:: start execute, children_size %lu", children_.size());
  while (current_tile_offset_ < total_tile_count_) {
    bool ret = children_[current_tile_offset_++]->Execute();

    if (ret) {
      std::unique_ptr<LogicalTile> logical_tile(children_[current_tile_offset_ - 1]->GetOutput());
      SetOutput(logical_tile.release());
      return true;
    }
  }
  return false;
}

}  // executor
}  // peloton
