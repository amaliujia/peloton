//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_join_executor.cpp
//
// Identification: src/backend/executor/hash_join_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>
#include <unordered_map>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/hash_join_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for hash join executor.
 * @param node Hash join node corresponding to this executor.
 */
HashJoinExecutor::HashJoinExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context)
    : AbstractJoinExecutor(node, executor_context) {}

bool HashJoinExecutor::DInit() {
  assert(children_.size() == 2);

  auto status = AbstractJoinExecutor::DInit();
  if (status == false) return status;

  assert(children_[1]->GetRawNode()->GetPlanNodeType() == PLAN_NODE_TYPE_HASH);

  hash_executor_ = reinterpret_cast<HashExecutor *>(children_[1]);

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool HashJoinExecutor::DExecute() {
  // Loop until we have non-empty result join logical tile or exit
  // Build outer join output when done
  if (left_child_done_ && right_child_done_) {
    return BuildOuterJoinOutput();
  }

  //===--------------------------------------------------------------------===//
  // Pick right and left tiles
  //===--------------------------------------------------------------------===//

  // Get all the logical tiles from RIGHT child
  if (right_child_done_ == false) {
    LOG_TRACE("Retrieve all tiles from Right child.");
    while (children_[1]->Execute()) {
      LOG_TRACE("Retrieve a new tile from right child");
      BufferRightTile(children_[1]->GetOutput());
    }

    LOG_TRACE("Right child is exhausted.");
    assert(!right_result_tiles_.empty());
    right_child_done_ = true;
  }

  // Get next logical tile from LEFT child when needed
  if (right_logical_tile_itr_ >= right_result_tiles_.size() ||
      left_result_tiles_.empty()) {
    if (children_[0]->Execute()) {
      LOG_TRACE("Retrieve a new title from the left child.");
      BufferLeftTile(children_[0]->GetOutput());
      right_logical_tile_itr_ = 0;
    } else {
      LOG_TRACE("Left child is exhausted.");
      assert(!left_result_tiles_.empty());
      left_child_done_ = true;
      return BuildOuterJoinOutput();
    }
  }

  //===--------------------------------------------------------------------===//
  // Build Join Tile
  //===--------------------------------------------------------------------===//

  LogicalTile *left_tile = left_result_tiles_.back().get();
  LogicalTile *right_tile = right_result_tiles_[right_logical_tile_itr_].get();

  // Build output join logical tile
  // This step builds the scheme of joined table
  auto output_tile = BuildOutputLogicalTile(left_tile, right_tile);

  // position builder
  LogicalTile::PositionListsBuilder pos_lists_builder(left_tile, right_tile);

  // Get the hash table from the hash executor
  const HashExecutor::HashMapType hash_table = hash_executor_->GetHashTable();
  const std::vector<oid_t> hash_key_ids = hash_executor_->GetHashKeyIds();

  // Go over the left logical tile
  // For each tuple, find matching tuples in the hash table built on top of the
  // right table

  // Go over the matching right tuples
  for (auto left_tile_row_itr : *left_tile) {
    // tuple with only columns in join clauses.
    expression::ContainerTuple<executor::LogicalTile> left_tuple(
        left_tile, left_tile_row_itr, &hash_key_ids);

    if (hash_table.find(left_tuple) == hash_table.end()) {
      continue;
    }

    // add full mathcing left tuple to position list.
    expression::ContainerTuple<executor::LogicalTile> left_tuple_full(
        left_tile, left_tile_row_itr);
    for (const auto &matching_tuple : hash_table.find(left_tuple)->second) {
      if (matching_tuple.first != right_logical_tile_itr_) {
        continue;
      }

      // construct right tuple
      expression::ContainerTuple<executor::LogicalTile> right_tuple(
          right_result_tiles_[right_logical_tile_itr_].get(),
          matching_tuple.second);

      if (predicate_ != nullptr &&
          predicate_->Evaluate(&left_tuple, &right_tuple, executor_context_)
              .IsFalse()) {
        // Join predicate is false. Skip pair and continue.
        continue;
      }

      // Insert joined tuple into right position list.
      pos_lists_builder.AddRow(left_tile_row_itr, matching_tuple.second);

      RecordMatchedLeftRow(left_result_tiles_.size() - 1, left_tile_row_itr);
      RecordMatchedRightRow(matching_tuple.first, matching_tuple.second);
    }
  }

  right_logical_tile_itr_++;

  if (pos_lists_builder.Size() > 0) {
    output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
    SetOutput(output_tile.release());
    return true;
  } else {
    return DExecute();
  }
}

}  // namespace executor
}  // namespace peloton
