
#include "abstract_exchange_executor.h"

namespace planner {

namespace executor {

AbstractExchangeExecutor::AbstractExchangeExecutor(const planner::AbstractPlan *node,
                                    ExecutorContext *executor_context)
  :AbstractExecutor(node, executor_context) {}


}
}
