//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// btree_index.cpp
//
// Identification: src/backend/index/btree_index.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/index/bwtree_index.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"
#include "backend/common/types.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      container_duplicate(metadata),
      container_unique(metadata) {
  dbg_msg("BWTreeIndex::BWTreeIndex() unique=%s",
          HasUniqueKeys() ? "true" : "false");
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator,
            KeyEqualityChecker>::~BWTreeIndex() {
  dbg_msg("BWTreeIndex::~BWTreeIndex() unique=%s",
          HasUniqueKeys() ? "true" : "false");
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                                  const ItemPointer location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  if (HasUniqueKeys())
    return container_unique.InsertEntry(index_key, location);
  else
    return container_duplicate.InsertEntry(index_key, location);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                  const ItemPointer location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  if (HasUniqueKeys())
    return container_unique.DeleteEntry(index_key, location);
  else
    return container_duplicate.DeleteEntry(index_key, location);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values, const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types,
    const ScanDirectionType &scan_direction) {
  dbg_msg("BWTreeIndex::Scan being called");
  std::vector<ItemPointer> result;

  // Check if we have leading (leftmost) column equality
  // refer : http://www.postgresql.org/docs/8.2/static/indexes-multicolumn.html
  oid_t leading_column_id = 0;
  auto key_column_ids_itr = std::find(key_column_ids.begin(),
                                      key_column_ids.end(), leading_column_id);

  // SPECIAL CASE : leading column id is one of the key column ids
  // and is involved in a equality constraint
  bool special_case = false;
  if (key_column_ids_itr != key_column_ids.end()) {
    auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
    if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      special_case = true;
    }
  }

  LOG_TRACE("Special case : %d ", special_case);

  if (special_case) {
    // If it is a special case, we can figure out the range to scan in the index
    std::unique_ptr<storage::Tuple> start_key;
    start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));
    ConstructLowerBoundTuple(start_key.get(), values, key_column_ids,
                             expr_types);
    KeyType index_key;
    index_key.SetFromKey(start_key.get());
    if (HasUniqueKeys()) {
      auto iterator = container_unique.GetIterator(index_key);
      iterator->Register();
      ExtractAllTuples(values, key_column_ids, expr_types, iterator, result);
      iterator->Deregister();
      delete iterator;
    } else {
      auto iterator = container_duplicate.GetIterator(index_key);
      iterator->Register();
      ExtractAllTuples(values, key_column_ids, expr_types, iterator, result);
      iterator->Deregister();
      delete iterator;
    }
  } else {
    if (HasUniqueKeys()) {
      auto iterator = container_unique.GetIterator();
      iterator->Register();
      ExtractAllTuples(values, key_column_ids, expr_types, iterator, result);
      iterator->Deregister();
      delete iterator;
    } else {
      auto iterator = container_duplicate.GetIterator();
      iterator->Register();
      ExtractAllTuples(values, key_column_ids, expr_types, iterator, result);
      iterator->Deregister();
      delete iterator;
    }
  }

  if (scan_direction == SCAN_DIRECTION_TYPE_BACKWARD)
    std::reverse(result.begin(), result.end());
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::vector<ItemPointer> BWTreeIndex<KeyType, ValueType, KeyComparator,
                                     KeyEqualityChecker>::ScanAllKeys() {
  std::vector<ItemPointer> result;
  if (HasUniqueKeys())
    container_unique.ScanAllKeys(result);
  else
    container_duplicate.ScanAllKeys(result);
  return result;
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key) {
  std::vector<ItemPointer> result;
  KeyType index_key;
  index_key.SetFromKey(key);
  if (HasUniqueKeys())
    container_unique.ScanKey(index_key, result);
  else
    container_duplicate.ScanKey(index_key, result);
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::string BWTreeIndex<KeyType, ValueType, KeyComparator,
                        KeyEqualityChecker>::GetTypeName() const {
  return "BWTree";
}

// Explicit template instantiation
template class BWTreeIndex<IntsKey<1>, ItemPointer, IntsComparator<1>,
                           IntsEqualityChecker<1>>;
template class BWTreeIndex<IntsKey<2>, ItemPointer, IntsComparator<2>,
                           IntsEqualityChecker<2>>;
template class BWTreeIndex<IntsKey<3>, ItemPointer, IntsComparator<3>,
                           IntsEqualityChecker<3>>;
template class BWTreeIndex<IntsKey<4>, ItemPointer, IntsComparator<4>,
                           IntsEqualityChecker<4>>;

template class BWTreeIndex<GenericKey<4>, ItemPointer, GenericComparator<4>,
                           GenericEqualityChecker<4>>;
template class BWTreeIndex<GenericKey<8>, ItemPointer, GenericComparator<8>,
                           GenericEqualityChecker<8>>;
template class BWTreeIndex<GenericKey<12>, ItemPointer, GenericComparator<12>,
                           GenericEqualityChecker<12>>;
template class BWTreeIndex<GenericKey<16>, ItemPointer, GenericComparator<16>,
                           GenericEqualityChecker<16>>;
template class BWTreeIndex<GenericKey<24>, ItemPointer, GenericComparator<24>,
                           GenericEqualityChecker<24>>;
template class BWTreeIndex<GenericKey<32>, ItemPointer, GenericComparator<32>,
                           GenericEqualityChecker<32>>;
template class BWTreeIndex<GenericKey<48>, ItemPointer, GenericComparator<48>,
                           GenericEqualityChecker<48>>;
template class BWTreeIndex<GenericKey<64>, ItemPointer, GenericComparator<64>,
                           GenericEqualityChecker<64>>;
template class BWTreeIndex<GenericKey<96>, ItemPointer, GenericComparator<96>,
                           GenericEqualityChecker<96>>;
template class BWTreeIndex<GenericKey<128>, ItemPointer, GenericComparator<128>,
                           GenericEqualityChecker<128>>;
template class BWTreeIndex<GenericKey<256>, ItemPointer, GenericComparator<256>,
                           GenericEqualityChecker<256>>;
template class BWTreeIndex<GenericKey<512>, ItemPointer, GenericComparator<512>,
                           GenericEqualityChecker<512>>;

template class BWTreeIndex<TupleKey, ItemPointer, TupleKeyComparator,
                           TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
