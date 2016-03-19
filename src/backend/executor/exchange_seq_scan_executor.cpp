#include "backend/common/thread_manager.h"
#include "backend/executor/exchange_seq_scan_executor.h"
#include "backend/executor/parallel_seq_scan_task_response.h"

namespace peloton {
namespace executor {

ExchangeSeqScanExecutor::ExchangeSeqScanExecutor(const planner::AbstractPlan *node,
                                           ExecutorContext *executor_context)
  : AbstractExecutor(node, executor_context) {}


bool ExchangeSeqScanExecutor::DInit() {
  assert(children_.size() != 0);

  current_tile_offset_ = 0;
  total_tile_count_ = children_.size();
  parallelize_done_ = false;
  current_tile_count_ = 0;

  return true;
}

bool ExchangeSeqScanExecutor::DExecute() {
  LOG_INFO("Exchange Seq Scan executor:: start execute, children_size %lu", children_.size());

  if (!parallelize_done_) {
    while (current_tile_offset_ < total_tile_count_) {
      LOG_TRACE("ExchangeSeqScanExecutor :: submit task to thread pool, dealing with %lu tile.", current_tile_offset_);
      std::function<void()> f_seq_scan = std::bind(&ExchangeSeqScanExecutor::SeqScanThreadMain, this, children_[current_tile_offset_], &queue_); 
      ThreadManager::GetServerThreadPool().AddTask(f_seq_scan);

      current_tile_offset_++;
    }
    parallelize_done_ = true;
  }

  if (current_tile_count_ == total_tile_count_) {
    return false;
  }

  while (current_tile_count_ < total_tile_count_) {
    std::unique_ptr<AbstractParallelTaskResponse> response_ptr(queue_.GetTask());
    current_tile_count_++;
    if (response_ptr->GetStatus() == HasRetValue) {
      SetOutput(response_ptr->GetOutput());
      return true;
    }
  }
  return false;
}

void  ExchangeSeqScanExecutor::SeqScanThreadMain(
                            AbstractExecutor *executor,
                            BlockingQueue<AbstractParallelTaskResponse *> *queue) {
  LOG_INFO("Parallel worker :: ExchangeSeqScanExecutor :: SeqScanThreadMain, executor: %s", executor->GetRawNode()->GetInfo().c_str());
  bool ret = executor->Execute();
  AbstractParallelTaskResponse *response = nullptr;

  if (ret) {
    LogicalTile *logical_tile = executor->GetOutput();
    response = new ParallelSeqScanTaskResponse(HasRetValue, logical_tile) ;
  } else {
    response = new ParallelSeqScanTaskResponse(NoRetValue, nullptr);
  }
  queue->PutTask(response);
}

}  // executor
}  // peloton
