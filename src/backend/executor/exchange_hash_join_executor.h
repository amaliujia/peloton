#include <deque>
#include <vector>

#include "backend/executor/abstract_join_executor.h"
#include "backend/planner/exchange_hash_join_plan.h"
#include "backend/executor/exchange_hash_executor.h"

namespace peloton {
namespace executor {

class ExchangeHashJoinExecutor : public AbstractJoinExecutor {
  ExchangeHashJoinExecutor(const ExchangeHashJoinExecutor &) = delete;
  ExchangeHashJoinExecutor &operator=(const ExchangeHashJoinExecutor &) = delete;

public:
  explicit ExchangeHashJoinExecutor(const planner::AbstractPlan *node,
                                    ExecutorContext *executor_context);

protected:
  bool DInit();

  bool DExecute();

private:
  typedef std::unordered_set<std::pair<size_t, oid_t>, boost::hash<std::pair<size_t, oid_t>>> MapValueType;

  ExchangeHashExecutor *hash_executor_ = nullptr;

  bool hashed_ = false;

  std::deque<LogicalTile *> buffered_output_tiles;
  std::vector<std::unique_ptr<LogicalTile>> right_tiles_;

  // logical tile iterators
  size_t left_logical_tile_itr_ = 0;
  size_t right_logical_tile_itr_ = 0;

};

}  // namespace executor
}  // namespace peloton