//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// BWTree.h
//
// Identification: src/backend/index/BWTree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/manager.h"
#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/common/value.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

#include <utility>
#include <mutex>
#include <boost/lockfree/stack.hpp>
#include <cstdint>
#include <vector>
#include <map>
#include <set>
#include <chrono>

// Debug mesasge for autolab, since LOG... won't be printed

//#define P2DEBUG
#ifdef P2DEBUG
#define dbg_msg(...)                                     \
  do {                                                   \
    fprintf(stderr, "%s line %d: ", __FILE__, __LINE__); \
    fprintf(stderr, __VA_ARGS__);                        \
    fprintf(stderr, "\n");                               \
    fflush(stderr);                                      \
  } while (0)
#else
#define dbg_msg(...)
#endif

#ifdef P2DEBUG
// Customized assert, since an assertion failure on autolab won't be printed
#define myassert(e)                \
  do {                             \
    if (!(e)) {                    \
      dbg_msg("assertion failed"); \
    }                              \
    assert(e);                     \
  } while (0)
#else
#define myassert(e)
#endif

namespace peloton {
namespace index {

struct ItemPointerComparator {
  inline bool operator()(const ItemPointer &lhs, const ItemPointer &rhs) const {
    return (lhs.block < rhs.block) ||
           ((lhs.block == rhs.block) && (lhs.offset < rhs.offset));
  }
};

struct ItemPointerEqualityChecker {
  inline bool operator()(const ItemPointer &lhs, const ItemPointer &rhs) const {
    return lhs.block == rhs.block && lhs.offset == rhs.offset;
  }
};

typedef std::uint_fast32_t PID;
typedef size_t SizeType;
typedef uint_fast8_t VersionNumber;
constexpr int max_chain_len = 10;               // for consolidation
constexpr int compact_chain_len_threshold = 3;  // for compaction
constexpr int max_node_size = 200;
constexpr int min_node_size = max_node_size / 2;
enum NodeType {
  NInner = 0,
  NLeaf = 1,
  NInsert = 2,
  NDelete = 3,
  NSplit = 4,
  NSplitEntry = 5,
  NMerge = 6,
  NRemove = 7,
  NMergeEntry = 8
};

template <typename KeyType, typename KeyComparator>
class PIDTable;

class BWBaseNode {
  /*
   * BWBaseNode
   * Use this node type so that a global garbage collector can be made
   */
 public:
  static const BWBaseNode *GenerateRandomNodeChain(int length);
  virtual NodeType GetType() const = 0;
  virtual const BWBaseNode *GetNext() const = 0;
  virtual size_t GetMemoryFootprint() const = 0;
  virtual ~BWBaseNode() {}
};

template <typename KeyType, typename KeyComparator>
class BWNode : public BWBaseNode {
 public:
  // Generate a random delta node chain of length 'length'. For test garbage
  // collection
  static const BWNode<KeyType, KeyComparator> *GenerateRandomNodeChain(
      int length);
  // Generate a random node of type 'type'
  static const BWNode<KeyType, KeyComparator> *GenerateRandomNode(
      NodeType type, const BWNode *next);

  virtual NodeType GetType() const = 0;

  virtual const BWNode<KeyType, KeyComparator> *GetNext() const = 0;

  // Print self
  virtual void Print(const PIDTable<KeyType, KeyComparator> &,
                     int indent) const = 0;

  inline bool IfLeafNode() const { return if_leaf_; }

  inline bool IfInnerNode() const { return !IfLeafNode(); }

  inline bool IfChainTooLong() const { return chain_length_ > max_chain_len; }

  inline bool IfOverflow() const { return slot_usage_ > max_node_size; }

  inline bool IfUnderflow() const { return slot_usage_ < min_node_size; }

  // If the given key is in the key range of this node
  inline bool IfInRange(const KeyType &key,
                        const KeyComparator key_comparator_) const {
    return (!has_low_key_ || !key_comparator_(key, low_key_)) &&
           (!has_high_key_ || key_comparator_(key, high_key_));
  }

  inline SizeType GetSlotUsage() const { return slot_usage_; }

  inline SizeType GetChainLength() const { return chain_length_; }

  inline bool HasLowKey() const { return has_low_key_; }

  inline bool HasHighKey() const { return has_high_key_; }

  inline const KeyType &GetLowKey() const { return low_key_; }

  inline const KeyType &GetHighKey() const { return high_key_; }

  virtual size_t GetMemoryFootprint() const {
    return sizeof(slot_usage_) + sizeof(chain_length_) + sizeof(if_leaf_) +
           sizeof(has_low_key_) + sizeof(has_high_key_) + sizeof(low_key_) +
           sizeof(high_key_);
  }

  virtual ~BWNode(){};

 protected:
  const SizeType slot_usage_;
  const SizeType chain_length_;
  const bool if_leaf_;
  // has_low_key_==false <-> low_key_==-infinity
  const bool has_low_key_, has_high_key_;
  const KeyType low_key_, high_key_;

  BWNode(const SizeType &slot_usage, const SizeType &chain_length, bool if_leaf,
         const PID &, const PID &, const KeyType &low_key,
         const KeyType &high_key)
      : slot_usage_(slot_usage),
        chain_length_(chain_length),
        if_leaf_(if_leaf),
        has_low_key_(true),
        has_high_key_(true),
        low_key_(low_key),
        high_key_(high_key) {}

  BWNode(const SizeType &slot_usage, const SizeType &chain_length, bool if_leaf,
         const PID &, const PID &, const KeyType &low_key, const bool &)
      : slot_usage_(slot_usage),
        chain_length_(chain_length),
        if_leaf_(if_leaf),
        has_low_key_(true),
        has_high_key_(false),
        low_key_(low_key),
        high_key_(low_key) {}

  BWNode(const SizeType &slot_usage, const SizeType &chain_length, bool if_leaf,
         const PID &, const PID &, const bool &, const KeyType &high_key)
      : slot_usage_(slot_usage),
        chain_length_(chain_length),
        if_leaf_(if_leaf),
        has_low_key_(false),
        has_high_key_(true),
        low_key_(high_key),
        high_key_(high_key) {}

  BWNode(const SizeType &slot_usage, const SizeType &chain_length, bool if_leaf,
         const PID &, const PID &, const bool &, const bool &)
      : slot_usage_(slot_usage),
        chain_length_(chain_length),
        if_leaf_(if_leaf),
        has_low_key_(false),
        has_high_key_(false),
        low_key_(),
        high_key_() {}

  BWNode(const SizeType &slot_usage, const SizeType &chain_length, bool if_leaf,
         const PID &, const PID &, const KeyType &low_key,
         const KeyType &high_key, const bool &has_low_key,
         const bool &has_high_key)
      : slot_usage_(slot_usage),
        chain_length_(chain_length),
        if_leaf_(if_leaf),
        has_low_key_(has_low_key),
        has_high_key_(has_high_key),
        low_key_(low_key),
        high_key_(high_key) {}

 private:
  BWNode(const BWNode &) = delete;

  BWNode &operator=(const BWNode &) = delete;
};

template <typename KeyType, typename KeyComparator>
class BWNormalNode : public BWNode<KeyType, KeyComparator> {
 public:
  virtual NodeType GetType() const = 0;

  // Normal nodes don't have a next
  const BWNode<KeyType, KeyComparator> *GetNext() const { return nullptr; };

  inline const PID &GetLeft() const { return left_; }

  inline const PID &GetRight() const { return right_; }

  virtual size_t GetMemoryFootprint() const {
    return BWNode<KeyType, KeyComparator>::GetMemoryFootprint() +
           sizeof(left_) + sizeof(right_);
  }

 protected:
  BWNormalNode(const SizeType &slot_usage, const SizeType &chain_length,
               bool if_leaf, const PID &left, const PID &right,
               const KeyType &low_key, const KeyType &high_key)
      : BWNode<KeyType, KeyComparator>(slot_usage, chain_length, if_leaf, left,
                                       right, low_key, high_key),
        left_(left),
        right_(right) {}

  BWNormalNode(const SizeType &slot_usage, const SizeType &chain_length,
               bool if_leaf, const PID &left, const PID &right,
               const bool &has_low_key, const KeyType &high_key)
      : BWNode<KeyType, KeyComparator>(slot_usage, chain_length, if_leaf, left,
                                       right, has_low_key, high_key),
        left_(left),
        right_(right) {
    myassert(!has_low_key);
  }

  BWNormalNode(const SizeType &slot_usage, const SizeType &chain_length,
               bool if_leaf, const PID &left, const PID &right,
               const KeyType &low_key, const bool &has_high_key)
      : BWNode<KeyType, KeyComparator>(slot_usage, chain_length, if_leaf, left,
                                       right, low_key, has_high_key),
        left_(left),
        right_(right) {
    myassert(!has_high_key);
  }

  BWNormalNode(const SizeType &slot_usage, const SizeType &chain_length,
               bool if_leaf, const PID &left, const PID &right,
               const bool &has_low_key, const bool &has_high_key)
      : BWNode<KeyType, KeyComparator>(slot_usage, chain_length, if_leaf, left,
                                       right, has_low_key, has_high_key),
        left_(left),
        right_(right) {
    myassert(!has_low_key && !has_high_key);
  }

  BWNormalNode(const SizeType &slot_usage, const SizeType &chain_length,
               bool if_leaf, const PID &left, const PID &right,
               const KeyType &low_key, const KeyType &high_key,
               const bool &has_low_key, const bool &has_high_key)
      : BWNode<KeyType, KeyComparator>(slot_usage, chain_length, if_leaf, left,
                                       right, low_key, high_key, has_low_key,
                                       has_high_key),
        left_(left),
        right_(right) {}

  const PID left_, right_;
};

template <typename KeyType, typename KeyComparator>
class BWInnerNode : public BWNormalNode<KeyType, KeyComparator> {
 public:
  BWInnerNode(const std::vector<KeyType> &keys,
              const std::vector<PID> &children, const PID &left,
              const PID &right, const KeyType &low_key, const KeyType &high_key)
      : BWNormalNode<KeyType, KeyComparator>(keys.size(), 0, false, left, right,
                                             low_key, high_key),
        keys_(keys),
        children_(children) {}

  BWInnerNode(const std::vector<KeyType> &keys,
              const std::vector<PID> &children, const PID &left,
              const PID &right, const KeyType &low_key,
              const bool &has_high_key)
      : BWNormalNode<KeyType, KeyComparator>(keys.size(), 0, false, left, right,
                                             low_key, has_high_key),
        keys_(keys),
        children_(children) {
    myassert(!has_high_key);
  }

  BWInnerNode(const std::vector<KeyType> &keys,
              const std::vector<PID> &children, const PID &left,
              const PID &right, const bool &has_low_key,
              const KeyType &high_key)
      : BWNormalNode<KeyType, KeyComparator>(keys.size(), 0, false, left, right,
                                             has_low_key, high_key),
        keys_(keys),
        children_(children) {
    myassert(!has_low_key);
  }

  BWInnerNode(const std::vector<KeyType> &keys,
              const std::vector<PID> &children, const PID &left,
              const PID &right, const bool &has_low_key,
              const bool &has_high_key)
      : BWNormalNode<KeyType, KeyComparator>(keys.size(), 0, false, left, right,
                                             has_low_key, has_high_key),
        keys_(keys),
        children_(children) {
    myassert(!has_low_key && !has_high_key);
  }

  BWInnerNode(std::vector<KeyType> &&keys, std::vector<PID> &&children,
              const PID &left, const PID &right, const KeyType &low_key,
              const KeyType &high_key, const bool &has_low_key,
              const bool &has_high_key)
      : BWNormalNode<KeyType, KeyComparator>(keys.size(), 0, false, left, right,
                                             low_key, high_key, has_low_key,
                                             has_high_key),
        keys_(keys),
        children_(children) {}

  inline NodeType GetType() const { return NInner; }

  inline const std::vector<KeyType> &GetKeys() const { return keys_; }

  inline const std::vector<PID> &GetChildren() const { return children_; }

  virtual size_t GetMemoryFootprint() const {
    myassert(keys_.size() + 1 == children_.size());
    return BWNormalNode<KeyType, KeyComparator>::GetMemoryFootprint() +
           sizeof(keys_) + sizeof(children_) + sizeof(KeyType) * keys_.size() +
           sizeof(PID) * children_.size();
  }

  void Print(const PIDTable<KeyType, KeyComparator> &, int indent) const;

 protected:
  const std::vector<KeyType> keys_;
  const std::vector<PID> children_;
};

template <typename KeyType, typename KeyComparator, typename ValueType>
class BWLeafNode : public BWNormalNode<KeyType, KeyComparator> {
 public:
  BWLeafNode(const std::vector<KeyType> &keys,
             const std::vector<ValueType> &values, const PID &left,
             const PID &right, const KeyType &low_key, const KeyType &high_key)
      : BWNormalNode<KeyType, KeyComparator>(keys.size(), 0, true, left, right,
                                             low_key, high_key),
        keys_(keys),
        values_(values) {}

  BWLeafNode(const std::vector<KeyType> &keys,
             const std::vector<ValueType> &values, const PID &left,
             const PID &right, const bool &has_low_key, const KeyType &high_key)
      : BWNormalNode<KeyType, KeyComparator>(keys.size(), 0, true, left, right,
                                             has_low_key, high_key),
        keys_(keys),
        values_(values) {
    myassert(!has_low_key);
  }

  BWLeafNode(const std::vector<KeyType> &keys,
             const std::vector<ValueType> &values, const PID &left,
             const PID &right, const KeyType &low_key, const bool &has_high_key)
      : BWNormalNode<KeyType, KeyComparator>(keys.size(), 0, true, left, right,
                                             low_key, has_high_key),
        keys_(keys),
        values_(values) {
    myassert(!has_high_key);
  }

  BWLeafNode(const std::vector<KeyType> &keys,
             const std::vector<ValueType> &values, const PID &left,
             const PID &right, const bool &has_low_key,
             const bool &has_high_key)
      : BWNormalNode<KeyType, KeyComparator>(keys.size(), 0, true, left, right,
                                             has_low_key, has_high_key),
        keys_(keys),
        values_(values) {
    myassert(!has_low_key && !has_high_key);
  }

  BWLeafNode(std::vector<KeyType> &&keys, std::vector<ValueType> &&values,
             const PID &left, const PID &right, const KeyType &low_key,
             const KeyType &high_key, const bool &has_low_key,
             const bool &has_high_key)
      : BWNormalNode<KeyType, KeyComparator>(keys.size(), 0, true, left, right,
                                             low_key, high_key, has_low_key,
                                             has_high_key),
        keys_(keys),
        values_(values) {}

  inline const std::vector<KeyType> &GetKeys() const { return keys_; }

  inline const std::vector<ValueType> &GetValues() const { return values_; }

  inline NodeType GetType() const { return NLeaf; }

  virtual size_t GetMemoryFootprint() const {
    myassert(keys_.size() == values_.size());
    // not true for duplicate values
    return BWNormalNode<KeyType, KeyComparator>::GetMemoryFootprint() +
           sizeof(keys_) + sizeof(values_) +
           (sizeof(KeyType) + sizeof(ValueType)) * keys_.size();
  }

  void Print(const PIDTable<KeyType, KeyComparator> &, int indent) const {
    std::string s;
    for (size_t i = 0; i < indent; i++) {
      s += "\t";
    }
    LOG_DEBUG("%sLeafNode, left %lu, right %lu, size %lu", s.c_str(),
              BWNormalNode<KeyType, KeyComparator>::left_,
              BWNormalNode<KeyType, KeyComparator>::right_, keys_.size());
  }

 protected:
  const std::vector<KeyType> keys_;
  const std::vector<ValueType> values_;
};

template <typename KeyType, typename KeyComparator>
class BWDeltaNode : public BWNode<KeyType, KeyComparator> {
 public:
  inline virtual NodeType GetType() const = 0;

  inline const BWNode<KeyType, KeyComparator> *GetNext() const { return next_; }

  virtual size_t GetMemoryFootprint() const {
    return BWNode<KeyType, KeyComparator>::GetMemoryFootprint() +
           next_->GetMemoryFootprint() + sizeof(next_);
  }

 protected:
  const BWNode<KeyType, KeyComparator> *next_;

  BWDeltaNode(const SizeType &slot_usage, const SizeType &chain_length,
              const BWNode<KeyType, KeyComparator> *next, const PID &left,
              const PID &right, const KeyType &low_key, const KeyType &high_key,
              const bool &has_low_key, const bool &has_high_key)
      : BWNode<KeyType, KeyComparator>(slot_usage, chain_length,
                                       next->IfLeafNode(), left, right, low_key,
                                       high_key, has_low_key, has_high_key),
        next_(next) {}
};

template <typename KeyType, typename KeyComparator, typename ValueType>
class BWInsertNode : public BWDeltaNode<KeyType, KeyComparator> {
 public:
  BWInsertNode(const SizeType &slot_usage, const SizeType &chain_length,
               const BWNode<KeyType, KeyComparator> *next, const PID &left,
               const PID &right, const KeyType &key, const ValueType &value)
      : BWDeltaNode<KeyType, KeyComparator>(
            slot_usage, chain_length, next, left, right, next->GetLowKey(),
            next->GetHighKey(), next->HasLowKey(), next->HasHighKey()),
        key_(key),
        value_(value) {}

  BWInsertNode(const BWNode<KeyType, KeyComparator> *next,
               const SizeType &slot_usage, const PID &left, const PID &right,
               const KeyType &key, const ValueType &value)
      : BWInsertNode(slot_usage, next->GetChainLength() + 1, next, left, right,
                     key, value) {}

  BWInsertNode(const BWNode<KeyType, KeyComparator> *next,
               const SizeType &slot_usage, const KeyType &key,
               const ValueType &value)
      : BWInsertNode(slot_usage, next->GetChainLength() + 1, next,
                     PIDTable<KeyType, KeyComparator>::PID_NULL,
                     PIDTable<KeyType, KeyComparator>::PID_NULL, key, value) {}

  inline NodeType GetType() const { return NInsert; }

  inline const KeyType &GetKey() const { return key_; }

  inline const ValueType &GetValue() const { return value_; }

  virtual size_t GetMemoryFootprint() const {
    return BWDeltaNode<KeyType, KeyComparator>::GetMemoryFootprint() +
           sizeof(key_) + sizeof(value_);
  }

  void Print(const PIDTable<KeyType, KeyComparator> &pid_table,
             int indent) const {
    std::string s;
    for (size_t i = 0; i < indent; i++) {
      s += "\t";
    }
    LOG_DEBUG("%sInsertNode", s.c_str());
    BWDeltaNode<KeyType, KeyComparator>::next_->Print(pid_table, indent);
  }

 protected:
  const KeyType key_;
  const ValueType value_;
};

template <typename KeyType, typename KeyComparator, typename ValueType>
class BWDeleteNode : public BWDeltaNode<KeyType, KeyComparator> {
 public:
  inline NodeType GetType() const { return NDelete; }

  BWDeleteNode(const SizeType &slot_usage, const SizeType &chain_length,
               const BWNode<KeyType, KeyComparator> *next, const PID &left,
               const PID &right, const KeyType &key, const ValueType &value)
      : BWDeltaNode<KeyType, KeyComparator>(
            slot_usage, chain_length, next, left, right, next->GetLowKey(),
            next->GetHighKey(), next->HasLowKey(), next->HasHighKey()),
        key_(key),
        value_(value) {}

  BWDeleteNode(const BWNode<KeyType, KeyComparator> *next,
               const SizeType &slot_usage, const PID &left, const PID &right,
               const KeyType &key, const ValueType &value)
      : BWDeleteNode(slot_usage, next->GetChainLength() + 1, next, left, right,
                     key, value) {}

  BWDeleteNode(const BWNode<KeyType, KeyComparator> *next,
               const SizeType &slot_usage, const KeyType &key,
               const ValueType &value)
      : BWDeleteNode(slot_usage, next->GetChainLength() + 1, next,
                     PIDTable<KeyType, KeyComparator>::PID_NULL,
                     PIDTable<KeyType, KeyComparator>::PID_NULL, key, value) {}

  inline const KeyType &GetKey() const { return key_; }

  inline const ValueType &GetValue() const { return value_; }

  virtual size_t GetMemoryFootprint() const {
    return BWDeltaNode<KeyType, KeyComparator>::GetMemoryFootprint() +
           sizeof(key_) + sizeof(value_);
  }

  void Print(const PIDTable<KeyType, KeyComparator> &pid_table,
             int indent) const {
    std::string s;
    for (size_t i = 0; i < indent; i++) {
      s += "\t";
    }
    LOG_DEBUG("%sDeleteNode", s.c_str());
    BWDeltaNode<KeyType, KeyComparator>::next_->Print(pid_table, indent);
  }

 protected:
  const KeyType key_;
  const ValueType value_;
};

template <typename KeyType, typename KeyComparator>
class BWSplitNode : public BWDeltaNode<KeyType, KeyComparator> {
 public:
  inline NodeType GetType() const { return NSplit; }

  inline const KeyType &GetSplitKey() const { return split_key_; }

  inline const PID &GetSplitTo() const { return split_to_; }

  BWSplitNode(const SizeType &slot_usage, const SizeType &chain_length,
              const BWNode<KeyType, KeyComparator> *next, const PID &left,
              const PID &right, const KeyType &split_key, const PID &split_to)
      : BWDeltaNode<KeyType, KeyComparator>(slot_usage, chain_length, next,
                                            left, right, next->GetLowKey(),
                                            split_key, next->HasLowKey(), true),
        split_key_(split_key),
        split_to_(split_to) {
    assert(right == split_to);
  }

  BWSplitNode(const BWNode<KeyType, KeyComparator> *next,
              const SizeType &slot_usage, const PID &left, const PID &right,
              const KeyType &split_key, const PID &split_to)
      : BWSplitNode(slot_usage, next->GetChainLength() + 1, next, left, right,
                    split_key, split_to) {}

  virtual size_t GetMemoryFootprint() const {
    return BWDeltaNode<KeyType, KeyComparator>::GetMemoryFootprint() +
           sizeof(split_key_) + sizeof(split_to_);
  }

  void Print(const PIDTable<KeyType, KeyComparator> &pid_table,
             int indent) const {
    std::string s;
    for (size_t i = 0; i < indent; i++) {
      s += "\t";
    }
    LOG_DEBUG("%sSplitNode, split to pid %lu", s.c_str(),
              (unsigned long)split_to_);
    BWDeltaNode<KeyType, KeyComparator>::next_->Print(pid_table, indent);
  }

 protected:
  // actually the same as high_key_
  const KeyType split_key_;
  // actually the same as right_
  const PID split_to_;
};

template <typename KeyType, typename KeyComparator>
class BWSplitEntryNode : public BWDeltaNode<KeyType, KeyComparator> {
 public:
  BWSplitEntryNode(const SizeType &slot_usage, const SizeType &chain_length,
                   const BWNode<KeyType, KeyComparator> *next, const PID &left,
                   const PID &right, const KeyType &to_low_key,
                   const KeyType &to_high_key, const PID &to)
      : BWDeltaNode<KeyType, KeyComparator>(
            slot_usage, chain_length, next, left, right, next->GetLowKey(),
            next->GetHighKey(), next->HasLowKey(), next->HasHighKey()),
        has_to_high_key_(true),
        to_low_key_(to_low_key),
        to_high_key_(to_high_key),
        to_(to) {}

  BWSplitEntryNode(const BWNode<KeyType, KeyComparator> *next, const PID &left,
                   const PID &right, const KeyType &to_low_key,
                   const KeyType &to_high_key, const PID &to)
      : BWSplitEntryNode(next->GetSlotUsage() + 1, next->GetChainLength() + 1,
                         next, left, right, to_low_key, to_high_key, to) {}

  BWSplitEntryNode(const BWNode<KeyType, KeyComparator> *next,
                   const KeyType &to_low_key, const KeyType &to_high_key,
                   const PID &to)
      : BWSplitEntryNode(next->GetSlotUsage() + 1, next->GetChainLength() + 1,
                         next, PIDTable<KeyType, KeyComparator>::PID_NULL,
                         PIDTable<KeyType, KeyComparator>::PID_NULL, to_low_key,
                         to_high_key, to) {}

  BWSplitEntryNode(const SizeType &slot_usage, const SizeType &chain_length,
                   const BWNode<KeyType, KeyComparator> *next, const PID &left,
                   const PID &right, const KeyType &to_low_key, const PID &to)
      : BWDeltaNode<KeyType, KeyComparator>(
            slot_usage, chain_length, next, left, right, next->GetLowKey(),
            next->GetHighKey(), next->HasLowKey(), next->HasHighKey()),
        has_to_high_key_(false),
        to_low_key_(to_low_key),
        to_high_key_(to_low_key),
        to_(to) {}

  BWSplitEntryNode(const BWNode<KeyType, KeyComparator> *next, const PID &left,
                   const PID &right, const KeyType &to_low_key, const PID &to)
      : BWSplitEntryNode(next->GetSlotUsage() + 1, next->GetChainLength() + 1,
                         next, left, right, to_low_key, to) {}

  BWSplitEntryNode(const BWNode<KeyType, KeyComparator> *next,
                   const KeyType &to_low_key, const PID &to)
      : BWSplitEntryNode(next->GetSlotUsage() + 1, next->GetChainLength() + 1,
                         next, PIDTable<KeyType, KeyComparator>::PID_NULL,
                         PIDTable<KeyType, KeyComparator>::PID_NULL, to_low_key,
                         to) {}

  inline NodeType GetType() const { return NSplitEntry; }

  inline const PID &GetTo() const { return to_; }

  inline bool HasToHighKey() const { return has_to_high_key_; }

  inline const KeyType &GetToLowKey() const { return to_low_key_; }

  inline const KeyType &GetToHighKey() const { return to_high_key_; }

  inline bool IfInToRange(const KeyType &key,
                          const KeyComparator &comparator) const {
    return !comparator(key, to_low_key_) &&
           (!has_to_high_key_ || comparator(key, to_high_key_));
  }

  virtual size_t GetMemoryFootprint() const {
    return BWDeltaNode<KeyType, KeyComparator>::GetMemoryFootprint() +
           sizeof(has_to_high_key_) + sizeof(to_low_key_) +
           sizeof(to_high_key_) + sizeof(to_);
  }

  void Print(const PIDTable<KeyType, KeyComparator> &pid_table,
             int indent) const {
    std::string s;
    for (size_t i = 0; i < indent; i++) {
      s += "\t";
    }
    LOG_DEBUG("%sSplitEntry, to pid %lu", s.c_str(), to_);

    BWDeltaNode<KeyType, KeyComparator>::next_->Print(pid_table, indent);
  }

 protected:
  const bool has_to_high_key_;
  const KeyType to_low_key_, to_high_key_;
  const PID to_;
};

/*
template<typename KeyType>
class BWRemoveNode: public BWDeltaNode {
public:
  BWRemoveNode(size_type slot_usage, size_type chain_length, const BWNode
*next):
          BWDeltaNode(slot_usage, chain_length, next, version_number_) { }

  inline NodeType GetType() const {
    return NRemove;
  }
};

template<typename KeyType>
class BWMergeNode: public BWDeltaNode {
public:
  BWMergeNode(size_type slot_usage, size_type chain_length, const BWNode *next,
const BWNode *right):
          BWDeltaNode(slot_usage, chain_length, next, version_number_),
right_(right) { }

  inline NodeType GetType() const {
    return NMerge;
  }

protected:
  const BWNode *right_;
};

// TODO
template<typename KeyType>
class MergeEntryNode: public BWDeltaNode {
public:
  MergeEntryNode(size_type slot_usage, size_type chain_length, BWNode *next):
          BWDeltaNode(slot_usage, chain_length, next, version_number_) { }

  inline NodeType GetType() const {
    return NMergeEntry;
  }
};
*/

template <typename KeyType, typename KeyComparator>
class PIDTable {
  /*
   * Class storing the mapping table between a PID and its corresponding address
   * The mapping table is organized as a two-level array, like a virtual memory
   * table.
   * The first level table is statically allocated, second level tables are
   * allocated as needed
   * Reclaimed PIDs are stored in a stack which will be given out first upon new
   * allocations.
   * To achieve both latch-free and simple of implementation, this table can
   * only reclaim PIDs but not space.
   */

 public:
  typedef std::atomic<PID> CounterType;
  typedef const BWNode<KeyType, KeyComparator> *Address;

 private:
  static constexpr unsigned int first_level_bits = 14;
  static constexpr unsigned int second_level_bits = 10;
  static constexpr PID first_level_mask = 0xFFFC00;
  static constexpr PID second_level_mask = 0x3FF;
  static constexpr unsigned int first_level_slots = 1 << first_level_bits;
  static constexpr unsigned int second_level_slots = 1 << second_level_bits;

 public:
  // NULL for PID
  static const PID PID_NULL;

  PIDTable() : counter_(0), free_PIDs(256) {
    first_level_table_[0] =
        (Address *)malloc(sizeof(Address) * second_level_slots);
  }

  ~PIDTable() {
    PID counter = counter_;
    PID num_slots = ((counter & first_level_mask) >> second_level_bits) + 1 + 1;
    for (PID i = 0; i < num_slots; ++i) {
      free(first_level_table_[i]);
    }
    myassert(counter == counter_);
  }

  // Get the address corresponding to the pid
  inline Address get(PID pid) const {
    return first_level_table_[(pid & first_level_mask) >>
                              second_level_bits][pid & second_level_mask];
  }

  // Free up a new PID
  inline void free_PID(PID pid) {
    set(pid, nullptr);
    free_PIDs.push(pid);
  }

  // Allocate a new PID, use argument "address" as its initial address
  inline PID allocate_PID(Address address) {
    PID result;
    if (!free_PIDs.pop(result)) result = allocate_new_PID(address);
    set(result, address);
    return result;
  }

  // Atomically CAS the address associated with "pid"
  // It compares the content associated with "pid" with "original"
  // If they are same, change the content associated with pid to "to" and return
  // true
  // Otherwise return false directly
  bool bool_compare_and_swap(PID pid, const Address original,
                             const Address to) {
    int row = (int)((pid & first_level_mask) >> second_level_bits);
    int col = (int)(pid & second_level_mask);
    return __sync_bool_compare_and_swap(&first_level_table_[row][col], original,
                                        to);
  }

 private:
  Address *first_level_table_[first_level_slots] = {nullptr};
  CounterType counter_;
  boost::lockfree::stack<PID> free_PIDs;

  PIDTable(const PIDTable &) = delete;

  PIDTable(PIDTable &&) = delete;

  PIDTable &operator=(const PIDTable &) = delete;

  PIDTable &operator=(PIDTable &&) = delete;

  // Allow GC to free PIDs
  friend class PIDNode;

  // No reclaimed PIDs available, need to allocate a new PID
  inline PID allocate_new_PID(Address) {
    PID pid = counter_++;
    if (is_first_PID(pid)) {
      LOG_DEBUG("%lu table[%lu][%lu] is first pid", pid,
                (pid & first_level_mask) >> second_level_bits,
                pid & second_level_mask);
      Address *table = (Address *)malloc(sizeof(Address) * second_level_slots);
      first_level_table_[((pid & first_level_mask) >> second_level_bits) + 1] =
          table;
    }
    return pid;
  }

  // Set the content associated with "pid" to "address"
  // Only used upon "pid"'s allocation and reclamation
  inline void set(PID pid, Address address) {
    int row = (int)((pid & first_level_mask) >> second_level_bits);
    int col = (int)(pid & second_level_mask);
    first_level_table_[row][col] = address;
  }

  inline static bool is_first_PID(PID pid) {
    return (pid & second_level_mask) == 0;
  }
};

template <typename KeyType, typename KeyComparator>
const PID PIDTable<KeyType, KeyComparator>::PID_NULL =
    std::numeric_limits<PID>::max();

typedef std::uint_fast32_t EpochTime;

class GarbageNode {
  /*
   * Wrap a piece of garbage into a list node
   */
 public:
  GarbageNode(const BWBaseNode *g, GarbageNode *next)
      : next_(next), garbage_(g) {}
  ~GarbageNode() {
    const BWBaseNode *head = garbage_;
    myassert(head != nullptr);
    while (head != nullptr) {
      const BWBaseNode *next = head->GetNext();
      delete head;
      head = next;
    }
    next_ = nullptr;
  }
  // Next garbage node in this garbage list
  GarbageNode *next_;
  inline void SetNext(GarbageNode *next) { next_ = next; }

 private:
  GarbageNode(const GarbageNode &) = delete;
  GarbageNode &operator=(const GarbageNode &) = delete;
  // The actual garbage bwnode
  const BWBaseNode *garbage_;
};

class PIDNode {
  /*
   * Wrap a PID into a list node
   */
 public:
  PIDNode(PID pid, PIDNode *next) : next_(next), pid_(pid) {}
  // Not needed now
  ~PIDNode() { /*(PIDTable::get_table()).free_PID(pid_);*/
    myassert(0);
  }
  // Next PIDNode in the list
  PIDNode *next_;
  inline void SetNext(PIDNode *next) { next_ = next; }

 private:
  PIDNode(const PIDNode &) = delete;
  PIDNode &operator=(const PIDNode &) = delete;
  // Actual PID needed to be reclaimed
  const PID pid_;
};

class Epoch {
  /*
   * Class representing an epoch
   */
 public:
  Epoch(EpochTime time, Epoch *next)
      : next_(next), registered_number_(0), epoch_time_(time) {}

  ~Epoch() {
    myassert(SafeToReclaim());
    // delete all garbage nodes, which will
    // delete the actual garbage in their destructor
    const GarbageNode *now = head_;
    const GarbageNode *next;
    while (now != nullptr) {
      next = now->next_;
      delete now;
      now = next;
    }
    // delete all PID garbage nodes, which will
    // reclaim the actual PID in their destructor
    const PIDNode *pid_now = pid_head_;
    const PIDNode *pid_next;
    while (pid_now != nullptr) {
      pid_next = pid_now->next_;
      delete pid_now;
      pid_now = pid_next;
    }
    next_ = nullptr;
  }

  // try to atomically submit a garbage node, return the submission result
  inline bool SubmitGarbage(GarbageNode *new_garbage) {
    GarbageNode *head = head_;
    new_garbage->SetNext(head);
    return __sync_bool_compare_and_swap(&head_, head, new_garbage);
  }

  // try to atomically submit a PID garbage node, return the submission result
  inline bool SubmitPID(PIDNode *new_pid) {
    PIDNode *head = pid_head_;
    new_pid->SetNext(head);
    return __sync_bool_compare_and_swap(&pid_head_, head, new_pid);
  }

  // register in this epoch
  inline EpochTime Register() {
    ++registered_number_;
    return epoch_time_;
  }

  // deregister a previous registration
  inline void Deregister() { --registered_number_; }

  inline bool TimeEquals(const EpochTime t) const { return epoch_time_ == t; }

  inline void SetNext(Epoch *n) { next_ = n; }

  // whether it is safe to reclaim all the garbage in this epoch
  inline bool SafeToReclaim() const { return registered_number_ == 0; }

  Epoch *next_;

 private:
  GarbageNode *head_ = nullptr;
  PIDNode *pid_head_ = nullptr;
  std::atomic<uint_fast32_t> registered_number_;
  const EpochTime epoch_time_;
};

void *Begin(void *arg);

class GarbageCollector {
  /*
   * This is an epoch-based garbage collector
   * The epochs are chained together as a latch-free singly-linked list
   * The garbage in a specific epoch is also chained together as a latch-free
   * singly-linked list
   */
 private:
  Epoch *volatile head_;
  volatile EpochTime timer_;
  // The Epoch at which we stopped at in the last garbage collection iteration
  Epoch *last_stopped_prev_;
  volatile bool stopped_ = false;
  // Daemon thread to do garbage collection, timer incrementation
  pthread_t clean_thread_;

  GarbageCollector() : head_(nullptr), timer_(0), last_stopped_prev_(nullptr) {
    LOG_DEBUG("GarbageCollector::GarbageCollector()");
    // Make sure there is at least one epoch
    head_ = new Epoch(timer_++, head_);
    // Start epoch allocation thread
    pthread_create(&clean_thread_, NULL, &Begin, this);
  }
  GarbageCollector(const GarbageCollector &) = delete;
  GarbageCollector &operator=(const GarbageCollector &) = delete;

  void ReclaimGarbage();
  void Stop() {
    LOG_DEBUG("GarbageCollector::Stop()");
    stopped_ = true;
    pthread_join(clean_thread_, NULL);
    LOG_DEBUG("finish GarbageCollector::Stop()");
  }

  // Reclaim all the epochs in the list starts at "head"
  static void ReclaimGarbageList(Epoch *head);

 public:
  // Daemon gc thread
  friend void *Begin(void *arg);
  static const int epoch_interval_;  // ms
  // Singleton
  static GarbageCollector global_gc_;

  ~GarbageCollector() {
    LOG_DEBUG("GarbageCollector::~GarbageCollector()");
    // stop epoch generation
    Stop();
    // TODO wait till all epochs are ready to be cleaned-up
    // force delete every garbage now
    Epoch *next;
    while (head_ != nullptr) {
      myassert(head_->SafeToReclaim());
      next = head_->next_;
      delete head_;
      head_ = next;
    }
    LOG_DEBUG("finish GarbageCollector::~GarbageCollector()");
  }

  // Submit a new bwnode garbage
  inline void SubmitGarbage(const BWBaseNode *garbage) {
    GarbageNode *new_garbage = new GarbageNode(garbage, nullptr);
    // loop until we successfully add this new garbage into the list
    Epoch *volatile head = head_;
    while (!head->SubmitGarbage(new_garbage)) head = head_;
  }

  // Submit a new garbage PID, not needed now
  inline void SubmitPID(PID pid) {
    PIDNode *new_pid_garbage = new PIDNode(pid, nullptr);
    // loop until we successfully add this new pid garbage into the list
    Epoch *volatile head = head_;
    while (!head->SubmitPID(new_pid_garbage)) head = head_;
  }

  // Register a thread, return the EpochTime that it registered in
  inline EpochTime Register() { return head_->Register(); }

  // Deregister a previous registration at "time"
  inline void Deregister(EpochTime time) {
    for (Epoch *volatile iter = head_; iter != nullptr; iter = iter->next_) {
      if (iter->TimeEquals(time)) {
        iter->Deregister();
        return;
      }
    }
    // should not happen
    myassert(0);
  }
};

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueComparator,
          class ValueEqualityChecker, bool Duplicate>
class BWTree {
  KeyComparator key_comparator_;
  KeyEqualityChecker key_equality_checker_;
  ValueComparator value_comparator_;
  ValueEqualityChecker value_equality_checker_;
  PIDTable<KeyType, KeyComparator> pid_table_;
  PID root_;
  VersionNumber root_version_number_;
  friend class ScanIterator;

 public:
  class ScanIterator {
   public:
    ScanIterator()
        : registered_time_(GarbageCollector::global_gc_.Register()),
          registered_(true) {}

    virtual ~ScanIterator() { Deregister(); }
    virtual bool HasNext() = 0;
    virtual std::pair<KeyType, ValueType> Next() = 0;

    // Register in the GC so that it can safely traverse the BWTree
    inline void Register() {
      if (!registered_) {
        registered_time_ = GarbageCollector::global_gc_.Register();
        registered_ = true;
      }
    }

    inline void Deregister() {
      if (registered_) {
        GarbageCollector::global_gc_.Deregister(registered_time_);
        registered_ = false;
      }
    }

    inline bool HasRegistered() const { return registered_; }

   protected:
    EpochTime registered_time_;
    bool registered_;
  };

  class ScanIteratorUnique : public ScanIterator {
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
           ValueComparator, ValueEqualityChecker, Duplicate> &bwtree_;
    const BWNode<KeyType, KeyComparator> *current_node_;
    std::vector<KeyType> keys_;
    std::vector<ValueType> values_;
    size_t next_;
    PID next_node_pid_;

    void UpdateSelf() {
      myassert(next_ == keys_.size());
      myassert((next_node_pid_ != PIDTable<KeyType, KeyComparator>::PID_NULL));

      current_node_ = bwtree_.pid_table_.get(next_node_pid_);
      myassert(current_node_->IfLeafNode());

      PID left_view;
      bwtree_.CreateLeafNodeView(current_node_, &keys_, &values_, &left_view,
                                 &next_node_pid_);
      next_ = 0;
    }

   public:
    ScanIteratorUnique(
        BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
               ValueComparator, ValueEqualityChecker, Duplicate> &bwtree)
        : bwtree_(bwtree), current_node_(bwtree.FirstLeafNode()) {
      myassert(!Duplicate);
      myassert(current_node_->IfLeafNode());
      PID left_view;
      bwtree_.CreateLeafNodeView(current_node_, &keys_, &values_, &left_view,
                                 &next_node_pid_);
      next_ = 0;
    }

    ScanIteratorUnique(
        BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
               ValueComparator, ValueEqualityChecker, Duplicate> &bwtree,
        const KeyType &start_key)
        : bwtree_(bwtree), current_node_(bwtree.FindLeafNode(start_key)) {
      myassert(!Duplicate);
      myassert(current_node_->IfLeafNode());
      PID left_view;
      bwtree_.CreateLeafNodeView(current_node_, &keys_, &values_, &left_view,
                                 &next_node_pid_);
      next_ = (size_t)std::distance(
          keys_.begin(), std::lower_bound(keys_.begin(), keys_.end(), start_key,
                                          bwtree_.key_comparator_));
    }

    virtual bool HasNext() {
      // LOG_TRACE("enter");
      while (next_ == keys_.size()) {
        if (next_node_pid_ == PIDTable<KeyType, KeyComparator>::PID_NULL) {
          // LOG_TRACE("leave false");
          return false;
        }
        UpdateSelf();
      }
      // LOG_TRACE("leave true");
      return true;
    }

    virtual std::pair<KeyType, ValueType> Next() {
      myassert(next_ < keys_.size());
      auto result = std::make_pair(keys_[next_], values_[next_]);
      ++next_;
      return result;
    }
  };

  class ScanIteratorDuplicate : public ScanIterator {
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
           ValueComparator, ValueEqualityChecker, Duplicate> &bwtree_;
    const BWNode<KeyType, KeyComparator> *current_node_;
    std::vector<KeyType> keys_;
    std::vector<std::vector<ValueType>> values_;
    size_t key_next_;
    size_t value_next_;
    PID next_node_pid_;
    EpochTime registered_time_;
    bool registered_;

    void UpdateSelf() {
      myassert(key_next_ == keys_.size());
      myassert((next_node_pid_ != PIDTable<KeyType, KeyComparator>::PID_NULL));

      current_node_ = bwtree_.pid_table_.get(next_node_pid_);
      myassert(current_node_->IfLeafNode());

      PID left_view;
      bwtree_.CreateLeafNodeView(current_node_, &keys_, &values_, &left_view,
                                 &next_node_pid_);
      key_next_ = 0;
      value_next_ = 0;
    }

   public:
    ScanIteratorDuplicate(
        BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
               ValueComparator, ValueEqualityChecker, Duplicate> &bwtree)
        : bwtree_(bwtree), current_node_(bwtree.FirstLeafNode()) {
      myassert(Duplicate);
      myassert(current_node_->IfLeafNode());
      PID left_view;
      bwtree_.CreateLeafNodeView(current_node_, &keys_, &values_, &left_view,
                                 &next_node_pid_);
      key_next_ = 0;
      value_next_ = 0;
    }

    ScanIteratorDuplicate(
        BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
               ValueComparator, ValueEqualityChecker, Duplicate> &bwtree,
        const KeyType &start_key)
        : bwtree_(bwtree), current_node_(bwtree.FindLeafNode(start_key)) {
      myassert(Duplicate);
      myassert(current_node_->IfLeafNode());
      PID left_view;
      bwtree_.CreateLeafNodeView(current_node_, &keys_, &values_, &left_view,
                                 &next_node_pid_);
      key_next_ = (size_t)std::distance(
          keys_.begin(), std::lower_bound(keys_.begin(), keys_.end(), start_key,
                                          bwtree_.key_comparator_));
      value_next_ = 0;
    }

    bool HasNext() {
      // LOG_TRACE("enter");
      while (key_next_ == keys_.size() ||
             value_next_ == values_[key_next_].size()) {
        if (key_next_ == keys_.size()) {
          if (next_node_pid_ == PIDTable<KeyType, KeyComparator>::PID_NULL) {
            // LOG_TRACE("leave false");
            return false;
          }
          UpdateSelf();
        } else {
          ++key_next_;
          value_next_ = 0;
        }
      }
      // LOG_TRACE("leave true");
      return true;
    }

    std::pair<KeyType, ValueType> Next() {
      myassert(key_next_ < keys_.size());
      myassert(value_next_ < values_[key_next_].size());
      auto result =
          std::make_pair(keys_[key_next_], values_[key_next_][value_next_]);
      ++value_next_;
      return result;
    }
  };

  BWTree(IndexMetadata *indexMetadata)
      : key_comparator_(indexMetadata),
        key_equality_checker_(indexMetadata),
        root_version_number_(0) {
    // a BWtree has at least two nodes residing at two levels.
    // at initialization, one root node pointing to one leaf node
    LOG_TRACE("BWTree::BWTree()");
    const BWNode<KeyType, KeyComparator> *first_leaf_node;
    if (!Duplicate)
      first_leaf_node = new BWLeafNode<KeyType, KeyComparator, ValueType>(
          std::vector<KeyType>(), std::vector<ValueType>(),
          PIDTable<KeyType, KeyComparator>::PID_NULL,
          PIDTable<KeyType, KeyComparator>::PID_NULL, false, false);
    else
      first_leaf_node =
          new BWLeafNode<KeyType, KeyComparator, std::vector<ValueType>>(
              std::vector<KeyType>(), std::vector<std::vector<ValueType>>(),
              PIDTable<KeyType, KeyComparator>::PID_NULL,
              PIDTable<KeyType, KeyComparator>::PID_NULL, false, false);
    PID first_leaf = pid_table_.allocate_PID(first_leaf_node);
    const BWNode<KeyType, KeyComparator> *root_node =
        new BWInnerNode<KeyType, KeyComparator>(
            std::vector<KeyType>(), {first_leaf},
            PIDTable<KeyType, KeyComparator>::PID_NULL,
            PIDTable<KeyType, KeyComparator>::PID_NULL, false, false);
    root_ = pid_table_.allocate_PID(root_node);
  }

  ~BWTree() {
    // wait for other garbage collection to finish
    // std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    PrintSelf(root_, pid_table_.get(root_), 0);
    EpochTime time = GarbageCollector::global_gc_.Register();
    LOG_TRACE("BWTree::~BWTree()");
    // garbage collect self
    const BWNode<KeyType, KeyComparator> *root_node = pid_table_.get(root_);
    pid_table_.free_PID(root_);
    SubmitGarbageNode(root_node);
    LOG_TRACE("finish BWTree::~BWTree()");
    GarbageCollector::global_gc_.Deregister(time);
  }

  inline ScanIterator *GetIterator() {
    if (Duplicate)
      return new ScanIteratorDuplicate(*this);
    else
      return new ScanIteratorUnique(*this);
  }

  inline ScanIterator *GetIterator(const KeyType &start_key) {
    if (Duplicate)
      return new ScanIteratorDuplicate(*this, start_key);
    else
      return new ScanIteratorUnique(*this, start_key);
  }

  inline size_t GetMemoryFootprint() const {
    EpochTime time = GarbageCollector::global_gc_.Register();
    size_t size = GetMemoryFootprint(root_);
    GarbageCollector::global_gc_.Deregister(time);
    return size;
  }

  // Force consolidation.
  // The BWTree will automatically do consolidation when the delta chain is too
  // long.
  // This function will consolidate all nodes unless it is unsafe
  // to consolidate the node or its delta chain is really short.
  inline bool CompactSelf() {
    EpochTime time = GarbageCollector::global_gc_.Register();
    bool result = CompactNode(root_);
    GarbageCollector::global_gc_.Deregister(time);
    return result;
  }

  void PrintSelf(__attribute__((unused)) PID pid,
                 const BWNode<KeyType, KeyComparator> *node, int indent) const {
    node->Print(pid_table_, indent);
    if (node->IfInnerNode()) {
      std::vector<KeyType> keys;
      std::vector<PID> children;
      PID left, right;
      CreateInnerNodeView(node, &keys, &children, &left, &right);
      std::string s;
      for (size_t i = 0; i < indent; i++) {
        s += "\t";
      }
      LOG_DEBUG("%sInner summary: size=%lu, self=%lu, left=%lu, right=%lu",
                s.c_str(), keys.size(), pid, left, right);
      for (int i = 0; i < children.size(); ++i)
        PrintSelf(children[i], pid_table_.get(children[i]), indent + 2);
    } else {
      if (!Duplicate) {
        std::vector<KeyType> keys;
        std::vector<ValueType> values;
        PID left, right;
        CreateLeafNodeView(node, &keys, &values, &left, &right);
        std::string s;
        for (size_t i = 0; i < indent; i++) {
          s += "\t";
        }
        LOG_DEBUG("%sLeaf summary: size=%lu, self=%lu, left=%lu, right=%lu",
                  s.c_str(), keys.size(), pid, left, right);
      } else {
        std::vector<KeyType> keys;
        std::vector<std::vector<ValueType>> values;
        PID left, right;
        CreateLeafNodeView(node, &keys, &values, &left, &right);
        std::string s;
        for (size_t i = 0; i < indent; i++) {
          s += "\t";
        }
        LOG_DEBUG("%sLeaf summary: size=%lu, self=%lu, left=%lu, right=%lu",
                  s.c_str(), keys.size(), pid, left, right);
      }
    }
  }

  inline bool InsertEntry(const KeyType &key, const ValueType &value) {
    EpochTime time = GarbageCollector::global_gc_.Register();
    std::vector<PID> path = {root_};
    bool result = InsertEntryUtil(key, value, path, root_version_number_);
    GarbageCollector::global_gc_.Deregister(time);
    return result;
  }

  inline bool DeleteEntry(const KeyType &key, const ValueType &value) {
    EpochTime time = GarbageCollector::global_gc_.Register();
    std::vector<PID> path = {root_};
    bool result = DeleteEntryUtil(key, value, path, root_version_number_);
    GarbageCollector::global_gc_.Deregister(time);
    return result;
  }

  inline void ScanKey(const KeyType &key, std::vector<ValueType> &result) {
    EpochTime time = GarbageCollector::global_gc_.Register();
    std::vector<PID> path = {root_};
    ScanKeyUtil(key, result, path, root_version_number_);
    GarbageCollector::global_gc_.Deregister(time);
  }

  void ScanAllKeys(std::vector<ValueType> &ret) const;

 private:
  size_t GetMemoryFootprint(const PID &node_pid) const;

  bool CompactNode(const PID &node_pid);

  // Submit a garbage node. This function is used at the destruction of this
  // BWTree
  // to submit all its nodes as garbage
  void SubmitGarbageNode(const BWNode<KeyType, KeyComparator> *node);

  bool CheckStatus(const BWNode<KeyType, KeyComparator> *node,
                   const KeyType &key, std::vector<PID> &path,
                   const VersionNumber &root_version_number);

  bool InsertEntryUtil(const KeyType &key, const ValueType &value,
                       std::vector<PID> &path,
                       VersionNumber root_version_number);

  bool DeleteEntryUtil(const KeyType &key, const ValueType &value,
                       std::vector<PID> &path,
                       VersionNumber root_version_number);

  void ScanKeyUtil(const KeyType &key, std::vector<ValueType> &result,
                   std::vector<PID> &path, VersionNumber root_version_number);

  const BWNode<KeyType, KeyComparator> *Split(
      const BWNode<KeyType, KeyComparator> *node_ptr, const PID &node_pid);

  const BWNode<KeyType, KeyComparator> *SplitInnerNode(
      const BWNode<KeyType, KeyComparator> *leaf_node, const PID &node_pid);

  const BWNode<KeyType, KeyComparator> *SplitLeafNode(
      const BWNode<KeyType, KeyComparator> *leaf_node, const PID &node_pid);

  bool InsertSplitEntry(const BWNode<KeyType, KeyComparator> *top,
                        const KeyType &key, std::vector<PID> &path,
                        const VersionNumber &root_version_number);

  bool ExistKeyValue(const BWNode<KeyType, KeyComparator> *node_ptr,
                     const KeyType &key, const ValueType &value) const;

  bool ExistKey(const BWNode<KeyType, KeyComparator> *node_ptr,
                const KeyType &key) const;

  PID FindNextNodePID(const BWNode<KeyType, KeyComparator> *node_ptr,
                      const KeyType &key) const;

  const BWNode<KeyType, KeyComparator> *FirstLeafNode() const;

  // Find the leaf node that could contain the given key
  const BWNode<KeyType, KeyComparator> *FindLeafNode(const KeyType &key);

  inline void CreateLeafNodeView(
      const BWNode<KeyType, KeyComparator> *node_chain,
      std::vector<KeyType> *keys, std::vector<ValueType> *values, PID *left,
      PID *right) const {
    myassert(!Duplicate);
    ConstructConsolidatedLeafNodeInternal(node_chain, keys, values, left,
                                          right);
  }

  inline void CreateLeafNodeView(
      const BWNode<KeyType, KeyComparator> *node_chain,
      std::vector<KeyType> *keys, std::vector<std::vector<ValueType>> *values,
      PID *left, PID *right) const {
    myassert(Duplicate);
    ConstructConsolidatedLeafNodeInternal(node_chain, keys, values, left,
                                          right);
  }

  inline void CreateInnerNodeView(
      const BWNode<KeyType, KeyComparator> *node_chain,
      std::vector<KeyType> *keys, std::vector<PID> *children, PID *left,
      PID *right) const {
    ConstructConsolidatedInnerNodeInternal(node_chain, keys, children, left,
                                           right);
  }

  bool Consolidate(const PID &cur,
                   const BWNode<KeyType, KeyComparator> *node_ptr);

  const BWNode<KeyType, KeyComparator> *ConstructConsolidatedInnerNode(
      const BWNode<KeyType, KeyComparator> *node_chain) const;

  const BWNode<KeyType, KeyComparator> *ConstructConsolidatedLeafNode(
      const BWNode<KeyType, KeyComparator> *node_chain) const;

  void ConstructConsolidatedLeafNodeInternal(
      const BWNode<KeyType, KeyComparator> *node_chain,
      std::vector<KeyType> *keys, std::vector<ValueType> *values, PID *left,
      PID *right) const;

  void ConstructConsolidatedLeafNodeInternal(
      const BWNode<KeyType, KeyComparator> *node_chain,
      std::vector<KeyType> *keys, std::vector<std::vector<ValueType>> *values,
      PID *left, PID *right) const;

  void ConstructConsolidatedInnerNodeInternal(
      const BWNode<KeyType, KeyComparator> *node_chain,
      std::vector<KeyType> *keys, std::vector<PID> *children, PID *left,
      PID *right) const;

  void ConsolidateInsertNode(
      const BWInsertNode<KeyType, KeyComparator, ValueType> *node,
      std::vector<KeyType> *keys, std::vector<ValueType> *values) const;

  void ConsolidateInsertNode(
      const BWInsertNode<KeyType, KeyComparator, ValueType> *node,
      std::vector<KeyType> *keys,
      std::vector<std::vector<ValueType>> *values) const;

  void ConsolidateDeleteNode(
      const BWDeleteNode<KeyType, KeyComparator, ValueType> *node,
      std::vector<KeyType> *keys, std::vector<ValueType> *values) const;

  void ConsolidateDeleteNode(
      const BWDeleteNode<KeyType, KeyComparator, ValueType> *node,
      std::vector<KeyType> *keys,
      std::vector<std::vector<ValueType>> *values) const;

  void ConsolidateSplitNode(const BWSplitNode<KeyType, KeyComparator> *node,
                            std::vector<KeyType> *keys,
                            std::vector<ValueType> *values, PID *right) const;

  void ConsolidateSplitNode(const BWSplitNode<KeyType, KeyComparator> *node,
                            std::vector<KeyType> *keys,
                            std::vector<std::vector<ValueType>> *values,
                            PID *right) const;

  void ConsolidateSplitNode(const BWSplitNode<KeyType, KeyComparator> *node,
                            std::vector<KeyType> *keys,
                            std::vector<PID> *children, PID *right) const;

  void ConsolidateSplitEntryNode(
      const BWSplitEntryNode<KeyType, KeyComparator> *node,
      std::vector<KeyType> *keys, std::vector<PID> *children) const;

  bool DeltaInsert(const PID &cur,
                   const BWNode<KeyType, KeyComparator> *node_ptr,
                   const KeyType &key, const ValueType &value,
                   bool need_expand);

  bool DeltaDelete(const PID &cur,
                   const BWNode<KeyType, KeyComparator> *node_ptr,
                   const KeyType &key, const ValueType &value,
                   bool need_shrink);
};
}  // End index namespace
}  // End peloton namespace
