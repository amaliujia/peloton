#pragma once

#include "backend/executor/abstract_executor.h"
#include "backend/executor/abstract_parallel_task_response.h"
#include "backend/executor/thread_pool.h"

namespace planner {
namespace executor {

class AbstractExchangeExecutor : public AbstractExecutor {
public:
  AbstractExchangeExecutor(const AbstractExchangeExecutor &) = delete;
  AbstractExchangeExecutor &operator=(const AbstractExchangeExecutor&) = delete;
  AbstractExchangeExecutor(AbstractExchangeExecutor &&) = delete;
  AbstractExchangeExecutor &operator=(AbstractExchangeExecutor &&) = delete;

  explicit AbstractExchangeExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context);

protected:
  bool DInit() = 0;

  bool DExecute() = 0;

protected:
  BlockingQueue<AbstractParallelTaskResponse *> queue_;
};

}
}
