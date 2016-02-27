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
//
#include "backend/catalog/manager.h"
#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/index/pid_table.h"
#include "backend/common/value.h"
#include <boost/lockfree/stack.hpp>

#include <cstdint>
#include <vector>
#include <map>
#include <set>
#include <chrono>

namespace peloton {
  namespace index {
    typedef size_t size_type;
    typedef uint_fast8_t VersionNumber;
    constexpr int max_chain_len = 8;
    constexpr int max_node_size = 20;
    constexpr int min_node_size = max_node_size<<1;
    enum NodeType {
      NInsert,
      NDelete,
      NRemove,
      NMerge,
      NUpdate,
      NSplit,
      NInner,
      NLeaf,
      NSplitEntry,
      NMergeEntry,
      Unknown
    };

    class BWNode {
    public:
      virtual NodeType GetType() const = 0;

      virtual const BWNode *GetNext() const = 0;

      inline const VersionNumber &GetVersionNumber() const { return version_number_; }

      virtual const PID &GetLeft() const = 0;

      virtual const PID &GetRight() const = 0;

      inline bool IfLeafNode() const {
        return if_leaf_;
      }

      inline bool IfInnerNode() const {
        return !if_leaf_;
      }

      inline bool IfChainTooLong() const {
        return chain_length_>max_chain_len;
      }

      inline bool IfOverflow() const {
        return slot_usage_>max_node_size;
      }

      inline bool IfUnderflow() const {
        return slot_usage_<min_node_size;
      }

      inline size_type GetSlotUsage() const { return slot_usage_; }

      inline size_type GetChainLength() const { return chain_length_; }

      virtual ~BWNode() { };

    protected:
      const size_type slot_usage_;
      const size_type chain_length_;
      const bool if_leaf_;
      const VersionNumber version_number_;
      BWNode(const size_type &slot_usage, const size_type &chain_length, bool if_leaf, const VersionNumber &version_number):
              slot_usage_(slot_usage), chain_length_(chain_length), if_leaf_(if_leaf), version_number_(version_number) { }

    private:
      BWNode(const BWNode &) = delete;

      BWNode &operator=(const BWNode &) = delete;
    };

    class BWNormalNode: public BWNode {
    public:
      virtual NodeType GetType() const = 0;

      // normal nodes don't have a next
      const BWNode *GetNext() const {
        return nullptr;
      };

      // left and right are not used or updated now
      inline const PID& GetLeft() const {
        return left_;
      }

      inline const PID& GetRight() const {
        return right_;
      }

    protected:
      BWNormalNode(const size_type &slot_usage, const size_type &chain_length, bool if_leaf, const PID &left,
                   const PID &right, const VersionNumber &version_number):
              BWNode(slot_usage, chain_length, if_leaf, version_number),
              left_(left),
              right_(right) { }
      const PID left_, right_;
    };

    template<typename KeyType>
    class BWInnerNode: public BWNormalNode {
    public:
      BWInnerNode(const std::vector<KeyType> &keys, const std::vector<PID> &children, const PID &left, const PID &right, const VersionNumber &version_number):
              BWNormalNode(keys.size(), 0, false, left, right, version_number),
              keys_(keys),
              children_(children) { }

      inline NodeType GetType() const {
        return NInner;
      }

      inline const std::vector<KeyType> &GetKeys() const {
        return keys_;
      }

      inline const std::vector<PID> &GetChildren() const {
        return children_;
      }
      inline const std::vector<PID> &GetPIDs() const {
        return GetChildren();
      }

    protected:
      const std::vector<KeyType> keys_;
      const std::vector<PID> children_;
    };

    template<typename KeyType, typename ValueType>
    class BWLeafNode: public BWNormalNode {
    public:
      // keys and values should not be used anymore
      BWLeafNode(const std::vector<KeyType> &keys, const std::vector<ValueType> &values, const PID &left, const PID &right, const VersionNumber &version_number):
              BWNormalNode(keys.size(), 0, true, left, right, version_number),
              keys_(keys),
              values_(values) { }
      BWLeafNode(const std::vector<KeyType> &keys, const std::vector<ValueType> &values, const PID &left, const PID &right):
              BWLeafNode(keys, values, left, right, std::numeric_limits<VersionNumber>::min()) { }

      inline const std::vector<KeyType> &GetKeys() const { return keys_; }

      inline const std::vector<ValueType> &GetValues() const { return values_; }
      inline NodeType GetType() const {
        return NLeaf;
      }

    protected:
      const std::vector<KeyType> keys_;
      const std::vector<ValueType> values_;
    };

    class BWDeltaNode: public BWNode {
    public:
      inline virtual NodeType GetType() const = 0;

      inline const BWNode *GetNext() const {
        return next_;
      }

      const PID &GetLeft() const {
        return next_->GetLeft();
      }

      const PID &GetRight() const {
        return next_->GetRight();
      }

      protected:
      const BWNode * next_;

      //TODO slot_usage, chain_length, next, if_leaf
      BWDeltaNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const VersionNumber &version_number):
              BWNode(slot_usage, chain_length, next->IfLeafNode(), version_number), next_(next) { }
    };

    template<typename KeyType, typename ValueType>
    class BWInsertNode: public BWDeltaNode {
    public:
      BWInsertNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key, const ValueType &value):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), key_(key), value_(value) { }
      BWInsertNode(const BWNode *next, const size_type &slot_usage, const KeyType &key, const ValueType &value):
              BWInsertNode(slot_usage, next->GetChainLength()+1, next, key, value) { }

      inline NodeType GetType() const {
        return NInsert;
      }

      inline const KeyType &GetKey() const {
        return key_;
      }

      inline const ValueType &GetValue() const {
        return value_;
      }

    protected:
      const KeyType key_;
      const ValueType value_;
    };

    template<typename KeyType, typename ValueType>
    class BWDeleteNode: public BWDeltaNode {
    public:
      inline NodeType GetType() const {
        return NDelete;
      }

      BWDeleteNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key, const ValueType &value):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), key_(key), value_(value) { }
      BWDeleteNode(const BWNode *next, const size_type &slot_usage, const KeyType &key, const ValueType &value):
              BWDeleteNode(slot_usage, next->GetChainLength()+1, next, key, value) { }

      inline const KeyType &GetKey() const {
        return key_;
      }

      inline const ValueType &GetValue() const {
        return value_;
      }

    protected:
      const KeyType key_;
      const ValueType value_;
    };

    template<typename KeyType>
    class BWSplitNode: public BWDeltaNode {
    public:
      inline NodeType GetType() const {
        return NSplit;
      }

      inline const KeyType &GetSplitKey() const {
        return key_;
      }

      inline PID GetRightPID() const {
        return right_;
      }

      BWSplitNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key, const PID &right):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()+1), key_(key), right_(right) { }
      BWSplitNode(const size_type &slot_usage, const BWNode *next, const KeyType &key, const PID &right):
              BWSplitNode(slot_usage, next->GetChainLength()+1, next, key, right) { }

    protected:
      const KeyType key_;
      const PID right_;
    };

    template<typename KeyType>
    class BWSplitEntryNode: public BWDeltaNode {
    public:

      BWSplitEntryNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &low, const KeyType &high,
                       const PID &to):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), low_key_(low), high_key_(high), to_(to), has_high_key_(true) { }

      BWSplitEntryNode(const BWNode *next, const KeyType &low, const KeyType &high, PID to):
              BWSplitEntryNode(next->GetSlotUsage()+1, next->GetChainLength()+1, next, low, high, to) { }

      BWSplitEntryNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &low, const PID &to):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), low_key_(low), high_key_(low), to_(to), has_high_key_(false) { }

      BWSplitEntryNode(const BWNode *next, const KeyType &low, PID to):
              BWSplitEntryNode(next->GetSlotUsage()+1, next->GetChainLength()+1, next, low, to) { }

      inline NodeType GetType() const {
        return NSplitEntry;
      }

      inline const KeyType &GetLowKey() const {
        return low_key_;
      }

      inline const KeyType &GetHightKey() const {
        return high_key_;
      }

      inline bool HasHighKey() const {
        return has_high_key_;
      }

      inline const PID &GetNextPID() const {
        return to_;
      }

    protected:
      const KeyType low_key_, high_key_;
      const PID to_;
      const bool has_high_key_;
    };

    template<typename KeyType>
    class BWRemoveNode: public BWDeltaNode {
    public:
      BWRemoveNode(size_type slot_usage, size_type chain_length, const BWNode *next):
              BWDeltaNode(slot_usage, chain_length, next, version_number_) { }

      inline NodeType GetType() const {
        return NRemove;
      }
    };

    template<typename KeyType>
    class BWMergeNode: public BWDeltaNode {
    public:
      BWMergeNode(size_type slot_usage, size_type chain_length, const BWNode *next, const BWNode *right):
              BWDeltaNode(slot_usage, chain_length, next, version_number_), right_(right) { }

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

    enum OpType {
      OInsert,
      ODelete,
      OUnknown
    };

    class Operation {
    public:
      virtual OpType GetOpType() const = 0;

    };

    class InsertOperation: public Operation {
    public:
      OpType GetOpType() {
        return OInsert;
      }
    };

    class DeleteOperation: public Operation {
    public:
      OpType GetOpType() {
        return ODelete;
      }
    };

    // todo:




    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    class BWTree {
      KeyComparator comparator_;
      KeyEqualityChecker key_equality_checker_;
      ValueComparator value_comparator_;
      ValueEqualityChecker value_equality_checker_;
      PID root_, first_leaf_;
      PIDTable pid_table_;
    public :
      BWTree(IndexMetadata *indexMetadata): comparator_(indexMetadata), key_equality_checker_(indexMetadata) {
        const BWNode *first_leaf_node;
        if(!Duplicate)
          first_leaf_node = new BWLeafNode<KeyType, ValueType>(std::vector<KeyType>(), std::vector<ValueType>(),
                                                               nullptr, nullptr);
        else
          first_leaf_node = new BWLeafNode<KeyType, std::vector<ValueType>>(std::vector<KeyType>(), std::vector<std::vector<ValueType>>(),
                                                                     nullptr, nullptr);
        first_leaf_ = pid_table_.allocate_PID(first_leaf_node);

        const BWNode *root_node = new BWInnerNode<KeyType>(std::vector<KeyType>(), {first_leaf_}, nullptr,
                                                           nullptr, std::numeric_limits<VersionNumber>::min());
        root_ = pid_table_.allocate_PID(root_node);
      }

      bool InsertEntry(const KeyType &key, const ValueType &value) {
        std::vector<PID> path = {root_};
        std::vector<VersionNumber> version_number = {pid_table_.get(root_)->GetVersionNumber()};
        return InsertEntryUtil(key, value, path, version_number);
      }

      bool DeleteEntry(const KeyType &key, const ValueType &value) {
        std::vector<PID> path = {root_};
        std::vector<VersionNumber> version_number = {pid_table_.get(root_)->GetVersionNumber()};
        return DeleteEntryUtil(key, value, path, version_number);
      }
/*
      void ScanKey(KeyType key, std::vector<ValueType> &ret) {
        ScanKeyUtil(PIDTable::PID_ROOT, key, ret);
      }

      void ScanAllKeys(std::vector<ValueType> & ) {
        PIDTable & pidTable = PIDTable::get_table();
        const BWNode *node_ptr = pidTable.get(PIDTable::PID_ROOT);

        while (node_ptr->GetType() != NLeaf) {
          const BWInnerNode<KeyType> *cur_ptr = static_cast<const BWInnerNode<KeyType> *>(node_ptr);

          PID next_pid = cur_ptr->GetPIDs()[0];
          node_ptr = pidTable.get(next_pid);
        }

        // reach first leaf node

      }
*/
    private:
      bool CheckStatus(const BWNode *node, const KeyType &key, std::vector<PID> &path, std::vector<VersionNumber> &version_number);

      bool InsertEntryUtil(const KeyType &key, const ValueType &value, std::vector<PID> &path, std::vector<VersionNumber> &version_number);

      bool DeleteEntryUtil(const KeyType &key, const ValueType &value, std::vector<PID> &path, std::vector<VersionNumber> &version_number);
      
      const BWNode *Split(const BWNode *node_ptr, const PID &node_pid);

      const BWNode *SplitInnerNode(const BWNode *leaf_node, const PID &node_pid);

      const BWNode *SplitLeafNode(const BWNode *leaf_node, const PID &node_pid);

      bool InsertSplitEntry(const BWNode *top, const KeyType &key, const std::vector<PID> &path, std::vector<VersionNumber> &version_number);

      bool ExistKey(const BWNode *node_ptr, const KeyType &key);

      void CreateLeafNodeView(const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<ValueType> &values, PID &left, PID &right);

      void CreateLeafNodeView(const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<std::vector<ValueType>> &values, PID &left, PID &right);

      void CreateInnerNodeView(const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<PID> &children, PID &left, PID &right);

      bool Consolidate(PID cur, const BWNode *node_ptr);

      const BWNode *ConstructConsolidatedInnerNode(const BWNode *node_chain);

      const BWNode *ConstructConsolidatedLeafNode(const BWNode *node_chain);

      void ConstructConsolidatedLeafNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                 std::vector<ValueType> &values, PID &left, PID &right);
      void ConstructConsolidatedLeafNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                 std::vector<std::vector<ValueType>> &values, PID &left, PID &right);

      void ConstructConsolidatedInnerNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                  std::vector<PID> &children, PID &left, PID &right);

      void ConsolidateInsertNode(const BWInsertNode<KeyType, ValueType> *node, std::vector<KeyType> &keys, std::vector<ValueType> &values);
      void ConsolidateInsertNode(const BWInsertNode<KeyType, ValueType> *node, std::vector<std::vector<KeyType>> &keys, std::vector<ValueType> &values);

      void ConsolidateDeleteNode(const BWDeleteNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<ValueType> &values);
      void ConsolidateDeleteNode(const BWDeleteNode<KeyType> *node, std::vector<std::vector<KeyType>> &keys, std::vector<ValueType> &values);

      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<ValueType> &values,
                                PID &right);
      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<std::vector<KeyType>> &keys, std::vector<ValueType> &values,
                                PID &right);

      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<PID> &children,
                                PID &right);

      void ConsolidateSplitEntryNode(const BWSplitEntryNode<KeyType> *node, std::vector<KeyType> &keys,
                                     std::vector<PID> &children);

      PID GetRightPID(const BWNode *node_ptr) {
        const BWNode *cur_ptr = node_ptr;
        while(cur_ptr->GetType()!=NLeaf||cur_ptr->GetType()!=NInner) {
          cur_ptr = cur_ptr->GetNext();
        }

        return cur_ptr->GetLeft();
      }

      /*
       *  In inner levels, the delta nodes should be SplitEntryNode, MergeEntryNode, SplitNode.
       *  For now, only handle SplitEntryNode and SplitNode.
       *
       *  This function has handle duplicate keys
       *
       *  TODO: Handle MergeEntryNode.
       */
/*
      void SplitInnerNodeUtil(const BWNode *node_ptr, KeyType &split_key, PID &right_pid,
                              std::vector<KeyType> &keys,
                              std::vector<PID> &pids);
*/

      // TODO: handle duplicate keys
/*
      void SplitLeafNodeUtil(const BWNode *node_ptr, KeyType &split_key, PID &right_pid,
                             std::vector<KeyType> &keys,
                             std::vector<ValueType> &values);
*/


      bool DeltaInsert(const PID &cur, const BWNode *node_ptr, const KeyType &key, const ValueType &value, bool need_expand);
      bool DeltaDelete(const PID &cur, const BWNode *node_ptr, const KeyType &key, const ValueType &value, bool &exist_value);

      // TODO: handle MergeEntryNode
/*
      PID GetNextPID(PID cur, KeyType key) {
        const BWNode *cur_ptr = PIDTable::get_table().get(cur);

        while(cur_ptr->GetType()!=NInner) {
          if(cur_ptr->GetType()==NSplitEntry) {
            const BWSplitEntryNode<KeyType> *split_entry_ptr = static_cast<const BWSplitEntryNode<KeyType> *>(cur_ptr);

            if(!comparator_(key, split_entry_ptr->GetLowKey())&&comparator_(key, split_entry_ptr->GetHightKey())) {
              return split_entry_ptr->GetNextPID();
            }
          }
          else if(cur_ptr->GetType()==NMergeEntry) {

          }
          else {

          }

          cur_ptr = cur_ptr->GetNext();
        }

        //const BWInnerNode<KeyType> *inner_ptr = static_cast<const BWInnerNode<KeyType> *>(cur_ptr);
        // TODO: implement scan_key;

        return PIDTable::PID_ROOT;
      }
*/


//      void ScanKeyUtil(PID cur, KeyType key, std::vector<ValueType> &ret);

    };


    ////////////////////////////////////////////////////////////////



  }  // End index namespace
}  // End peloton namespace
