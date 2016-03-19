#include "backend/expression/abstract_expression.h"
#include "backend/storage/data_table.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/abstract_scan_executor.h"
#include "backend/executor/abstract_parallel_task_response.h"
#include "backend/executor/thread_pool.h"

namespace peloton {
namespace executor {

class ExchangeSeqScanExecutor : public AbstractExecutor {
public:
  ExchangeSeqScanExecutor(const ExchangeSeqScanExecutor &) = delete;
  ExchangeSeqScanExecutor &operator=(const ExchangeSeqScanExecutor &) = delete;
  ExchangeSeqScanExecutor(ExchangeSeqScanExecutor &&) = delete;
  ExchangeSeqScanExecutor &operator=(ExchangeSeqScanExecutor &&) = delete;

  explicit ExchangeSeqScanExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context);
protected:
  bool DInit();

  bool DExecute();

private:
  void SeqScanThreadMain(AbstractExecutor *executor, BlockingQueue<AbstractParallelTaskResponse *> queue);

private:
  oid_t current_tile_offset_;
  oid_t total_tile_count_;

  oid_t current_tile_count_;
  bool parallelize_done_;

  BlockingQueue<AbstractParallelTaskResponse *> queue_;

};

}  // namespace executor
}  // namespace peloton

