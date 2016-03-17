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
  return true;
}

bool ExchangeSeqScanExecutor::DExecute() {
  for (auto child : children_) {
    bool ret = child->Execute();

    if (ret) {
      std::unique_ptr<LogicalTile> logical_tile(child->GetOutput());
      SetOutput(logical_tile.release());
      return ret;
    } else {
      continue;
    }
  }
  return false;
}

}  // executor
}  // peloton
