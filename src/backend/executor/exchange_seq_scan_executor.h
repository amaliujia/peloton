#include "backend/expression/abstract_expression.h"
#include "backend/storage/data_table.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/abstract_scan_executor.h"

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

// private:
//  /** @brief Pointer to table to scan from. */
//  storage::DataTable *target_table_ = nullptr;
//
//  /** @brief Selection predicate. */
//  const expression::AbstractExpression *predicate_ = nullptr;
//
//  /** @brief Columns from tile group to be added to logical tile output. */
//  std::vector<oid_t> column_ids_;



};

}  // namespace executor
}  // namespace peloton

