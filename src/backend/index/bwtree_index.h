//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// btree_index.h
//
// Identification: src/backend/index/btree_index.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <map>

#include "backend/catalog/manager.h"
#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/index/bwtree.h"
#include "backend/index/item_pointer_comparator.h"
#include "backend/index/item_pointer_equality_checker.h"


namespace peloton {
namespace index {

/**
 * BW tree-based index implementation.
 *
 * @see Index
 */
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class BWTreeIndex : public Index {
  friend class IndexFactory;

  typedef BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ItemPointerComparator, ItemPointerEqualityChecker, true> MapTypeDuplicate;
  typedef BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ItemPointerComparator, ItemPointerEqualityChecker, false> MapTypeUnique;
  public:
  BWTreeIndex(IndexMetadata *metadata);

  virtual ~BWTreeIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer location);

  std::vector<ItemPointer> Scan(const std::vector<Value> &values,
                                const std::vector<oid_t> &key_column_ids,
                                const std::vector<ExpressionType> &expr_types,
                                const ScanDirectionType& scan_direction);

  std::vector<ItemPointer> ScanAllKeys();

  std::vector<ItemPointer> ScanKey(const storage::Tuple *key);

  std::string GetTypeName() const;

  // TODO: Implement this
  bool Cleanup() {
    dbg_msg("not implemented BWTreeIndex::Cleanup being called");
    return true;
  }

  // TODO: Implement this
  size_t GetMemoryFootprint() {
    dbg_msg("not implemented BWTreeIndex::GetMemoryFootprint being called");
    return 0;
  }

 protected:
  // container
  MapTypeDuplicate container_duplicate;
  MapTypeUnique container_unique;
};

}  // End index namespace
}  // End peloton namespace
