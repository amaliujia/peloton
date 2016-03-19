#pragma once

#include "backend/executor/logical_tile.h"

namespace peloton {
namespace executor {

enum ParallelTaskStatus {
  Unknown,
  HasRetValue,
  NoRetValue
};


class AbstractParallelTaskResponse {
public:
  AbstractParallelTaskResponse();
  AbstractParallelTaskResponse(ParallelTaskStatus status);

  ParallelTaskStatus GetStatus();


  virtual LogicalTile *GetOutput() = 0;
private:
  ParallelTaskStatus  status_;
};


}  // namespace executor
}  // namespace peloton
