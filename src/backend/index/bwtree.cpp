//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.cpp
//
// Identification: src/backend/index/bwtree.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// !!!
#include "backend/index/bwtree.h"
#include "backend/index/index_key.h"
#include "backend/index/pid_table.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {
  PIDTable PIDTable::global_table_;
}  // End index namespace
}  // End peloton namespace
