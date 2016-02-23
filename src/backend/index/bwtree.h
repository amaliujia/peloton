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
//#include "backend/index/pid_table.h"
#include "backend/common/value.h"
#include <boost/lockfree/stack.hpp>

#include <vector>
#include <map>
#include <set>
#include <chrono>

namespace peloton {
  namespace index {
    typedef size_t size_type;
    typedef size_t PID;
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
      inline virtual NodeType GetType() const = 0;

      inline virtual const BWNode *GetNext() const = 0;

      inline virtual PID GetLeft() const = 0;

      inline virtual PID GetRight() const = 0;

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

      BWNode(const size_type &slot_usage, const size_type &chain_length, bool if_leaf):
              slot_usage_(slot_usage), chain_length_(chain_length), if_leaf_(if_leaf) { }

    private:
      BWNode(const BWNode &) = delete;

      BWNode &operator=(const BWNode &) = delete;
    };

    template<typename KeyType>
    class BWNormalNode: public BWNode {
    public:
      virtual NodeType GetType() const = 0;

      // TODO ???
      inline const BWNode *GetNext() const {
        return NULL;
      };

      inline PID GetLeft() const {
        return left_;
      }

      inline PID GetRight() const {
        return right_;
      }

    protected:
      BWNormalNode(const size_type &slot_usage, const size_type &chain_length, bool if_leaf, const PID &left,
                   const PID &right): BWNode(slot_usage, chain_length, if_leaf),
                               left_(left),
                               right_(right) { }
      const PID left_, right_;
    };

    template<typename KeyType>
    class BWInnerNode: public BWNormalNode<KeyType> {
    public:
      // keys and children should not be used anymore
      /*BWInnerNode(std::vector<KeyType> &keys, std::vector<PID> &children, const PID &left, const PID &right):
              BWNormalNode<KeyType>(keys.size(), 0, false, left, right), keys_(std::move(keys)), children_(std::move(children)) {

      }*/
      BWInnerNode(const std::vector<KeyType> &keys, const std::vector<PID> &children, const PID &left, const PID &right):
              BWNormalNode<KeyType>(keys.size(), 0, false, left, right), keys_(keys), children_(children) {

      }

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

    template<typename KeyType, typename ValueType, class KeyComparator>
    class BWLeafNode: public BWNormalNode<KeyType> {
    public:
      // keys and values should not be used anymore
      /*BWLeafNode(std::vector<KeyType> &keys, std::vector<ValueType> &values, const PID &left, const PID &right):
              BWNormalNode<KeyType>(keys.size(), 0, true, left, right), keys_(std::move(keys)), values_(std::move(values)) { }*/
      BWLeafNode(const std::vector<KeyType> &keys, const std::vector<ValueType> &values, const PID &left, const PID &right):
              BWNormalNode<KeyType>(keys.size(), 0, true, left, right), keys_(keys), values_(values) { }

      // TODO: add iterator
      inline const std::vector<KeyType> &GetKeys() const { return keys_; }

      inline const std::vector<ValueType> &GetValues() const { return values_; }
      inline NodeType GetType() const {
        return NLeaf;
      }

      void ScanKey(const KeyType &key, const KeyComparator &comparator, std::vector<ValueType> &ret) const {

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

    protected:

      BWDeltaNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next): BWNode(slot_usage, chain_length, false), next_(next) { }
      const BWNode *next_;
    };

    template<typename KeyType, typename ValueType>
    class BWInsertNode: public BWDeltaNode {
    public:
      BWInsertNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key, const ValueType &value):
              BWDeltaNode(slot_usage, chain_length, next), key_(key), value_(value) { }

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

    template<typename KeyType>
    class BWDeleteNode: public BWDeltaNode {
    public:
      inline NodeType GetType() const {
        return NDelete;
      }

      BWDeleteNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key):
              BWDeltaNode(slot_usage, chain_length, next), key_(key) { }

      inline const KeyType &GetKey() const {
        return key_;
      }

    protected:
      const KeyType key_;
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

      BWSplitNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key, PID right, PID old):
              BWDeltaNode(slot_usage, chain_length, next), key_(key), right_(right), old_(old) { }

    protected:
      const KeyType key_;
      // From Andy's slide, split node must have three child.
      // 1. old left.
      // 2. old right.
      // 3. new

      // new one
      const PID right_;

      // old right
      const PID old_;
    };

    template<typename KeyType>
    class BWSplitEntryNode: public BWDeltaNode {
    public:
      BWSplitEntryNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &low, const KeyType &high,
                       PID to):
              BWDeltaNode(slot_usage, chain_length, next), low_key_(low), high_key_(high), to_(to) { }

      inline NodeType GetType() const {
        return NSplitEntry;
      }

      inline const KeyType &GetLowKey() const {
        return low_key_;
      }

      inline const KeyType &GetHightKey() const {
        return high_key_;
      }

      inline PID GetNextPID() const {
        return to_;
      }

    protected:
      const KeyType low_key_, high_key_;
      const PID to_;
    };

    template<typename KeyType>
    class BWRemoveNode: public BWDeltaNode {
    public:
      BWRemoveNode(size_type slot_usage, size_type chain_length, const BWNode *next):
              BWDeltaNode(slot_usage, chain_length, next) { }

      inline NodeType GetType() const {
        return NRemove;
      }
    };

    template<typename KeyType>
    class BWMergeNode: public BWDeltaNode {
    public:
      BWMergeNode(size_type slot_usage, size_type chain_length, const BWNode *next, const BWNode *right):
              BWDeltaNode(slot_usage, chain_length, next), right_(right) { }

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
              BWDeltaNode(slot_usage, chain_length, next) { }

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


    ////////////////////////////////////////////////////////////////
    // todo:

    class PIDTable {

      /*
       * class storing the mapping table between a PID and its corresponding address
       * the mapping table is organized as a two-level array, like a virtual memory table.
       * the first level table is statically allocated, second level tables are allocated as needed
       * reclaimed PIDs are stored in a stack which will be given out first upon new allocations.
       * to achieve both latch-free and simple of implementation, this table can only reclaim PIDs but not space.
       */
      typedef std::uint_fast32_t PID;
      typedef std::atomic<PID> CounterType;
      typedef const BWNode * Address;
      static constexpr unsigned int first_level_bits = 14;
      static constexpr unsigned int second_level_bits = 10;
      static constexpr PID first_level_mask = 0xFFFC00;
      static constexpr PID second_level_mask = 0x3FF;
      static constexpr unsigned int first_level_slots = 1<<first_level_bits;
      static constexpr unsigned int second_level_slots = 1<<second_level_bits;
      // singleton
      static PIDTable global_table_;
      public:
      // NULL for PID
      static constexpr PID PID_NULL = std::numeric_limits<PID>::max();
      static constexpr PID PID_ROOT = 0;
      static inline PIDTable& get_table() { return global_table_; }

      // get the address corresponding to the pid
      inline Address get(PID pid) const {
        return first_level_table_
        [(pid&first_level_mask)>>second_level_bits]
        [pid&second_level_mask];
      }

      // allocate a new PID
      inline PID allocate_PID(Address address) {
        PID result;
        if(free_PIDs.pop(result)) {
          set(result, address);
          return result;
        }
        return allocate_new_PID(address);
      }

      // free up a new PID
      inline void free_PID(PID pid) {
        free_PIDs.push(pid);
        // bool push_result = freePIDs_.push(pid);
        // assert(push_result);
      }

      // atomic operation
      bool bool_compare_and_swap(PID pid, const Address original, const Address to) {
        int row = (int)((pid&first_level_mask)>>second_level_bits);
        int col = (int)(pid&second_level_mask);
        return __sync_bool_compare_and_swap(
          &first_level_table_[row][col], original, to);
      }

      private:
      volatile Address *first_level_table_[first_level_slots] = {nullptr};
      CounterType counter_;
      boost::lockfree::stack <PID> free_PIDs;

      PIDTable(): counter_(0) {
        first_level_table_[0] = (Address *)malloc(sizeof(Address)*second_level_slots);
      }

      PIDTable(const PIDTable &) = delete;

      PIDTable(PIDTable &&) = delete;

      PIDTable &operator=(const PIDTable &) = delete;

      PIDTable &operator=(PIDTable &&) = delete;

      inline bool is_first_PID(PID pid) {
        return (pid&second_level_mask)==0;
      }

      inline PID allocate_new_PID(Address address) {
        PID pid = counter_++;
        if(is_first_PID(pid)) {
          Address *table = (Address* )malloc(sizeof(Address)*second_level_slots);
          first_level_table_[((pid&first_level_mask)>>second_level_bits)+1] = table;
        }
        int row = (int)((pid&first_level_mask)>>second_level_bits);
        int col = (int)(pid&second_level_mask);
        first_level_table_[row][col] = address;
        return pid;
      }

      inline void set(PID pid, Address address) {
        int row = (int)((pid&first_level_mask)>>second_level_bits);
        int col = (int)(pid&second_level_mask);
        first_level_table_[row][col] = address;
      }
    };




    // todo:




    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    class BWTree {
      KeyComparator comparator_;
      KeyEqualityChecker key_equality_checker_;

    private:
      int BinaryGreaterThan(std::vector<KeyType> &arr, KeyType target, KeyComparator comp) {
        int i = 0, j = (int) arr.size()-1;

        while(i<=j) {
          int mid = (i+j)/2;
          if(comp(target, arr[mid])) {
            j -= 1;
          }
          else {
            i += 1;
          }
        }

        return i;
      }

    private:

      void InsertSplitEntry(const std::vector<PID> &path, const KeyType &low_key, const PID &right_pid) {
        PIDTable pid_table = PIDTable::get_table();
        if(path.size()>1) { // In this case, this is a normal second step split
          // KeyType high_key;

          while(true) {
            // Step 1: check if split finish.
            const BWNode *parent_ptr = pid_table.get(path[path.size()-2]);
            const BWNode *cur_ptr = parent_ptr;

            while(cur_ptr->GetType()!=NInner) {
              if(cur_ptr->GetType()==NSplitEntry) {
                const BWSplitEntryNode<KeyType> *split_ptr = static_cast<const BWSplitEntryNode<KeyType> *>(cur_ptr);
                if(split_ptr->GetLowKey()==low_key) {
                  return;
                }
              }
              else {
                // TODO: then what?
              }

              cur_ptr = cur_ptr->GetNext();
            }

            // Step 2: try to finish second step.
            size_type slot_usage = parent_ptr->GetSlotUsage();
            size_type chain_len = parent_ptr->GetChainLength();

            // When only one key, low_key should be equal to high_key.
            BWNode *split_entry_ptr = new BWSplitEntryNode<KeyType>(slot_usage+1,
                                                           chain_len+1, parent_ptr, low_key, low_key, right_pid);

            PID split_entry_pid = pid_table.allocate_PID(split_entry_ptr);


            bool ret = pid_table.bool_compare_and_swap(path[path.size()-2], parent_ptr, split_entry_ptr);
            if(ret==false) {
              delete split_entry_ptr;
              pid_table.free_PID(split_entry_pid);
              continue;
            }
            return;
          }
        }
        else { // TODO: need think about root case.

        }
      }

      bool Consolidate(PID cur, const BWNode *node_ptr);

      const BWInnerNode<KeyType> *ConstructConsolidatedInnerNode(const BWNode *node_chain);

      void ConstructConsolidatedLeafNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                 std::vector<ValueType> &values, PID &left, PID &right);

      const BWLeafNode <KeyType, ValueType, KeyComparator> *ConstructConsolidatedLeafNode(const BWNode *node_chain);

      void ConstructConsolidatedInnerNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                  std::vector<PID> &children, PID &left, PID &right);

      void ConsolidateInsertNode(const BWInsertNode<KeyType, ValueType> *node, std::vector<KeyType> &keys, std::vector<ValueType> &values);

      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<ValueType> &values);

      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<PID> &children);

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
      void SplitInnerNodeUlti(const BWNode *node_ptr, KeyType &split_key, PID &right_pid,
                              std::vector<KeyType> &keys,
                              std::vector<PID> &pids);

      // TODO: handle duplicate keys
      void SplitLeafNodeUtil(const BWNode *node_ptr, KeyType &split_key, PID &right_pid,
                             std::vector<KeyType> &keys,
                             std::vector<ValueType> &values);

      // Duplicate keys handled
      bool SplitInnerNode(PID cur, const BWNode *node_ptr, KeyType &split_key, PID &right_pid);

      bool SplitLeafNode(PID cur, const BWNode *node_ptr, KeyType &split_key, PID &right_pid);

      bool Split(PID cur, const BWNode *node_ptr, KeyType &split_key, PID &right_pid, NodeType right_type);

      bool DupExist(const BWNode *node_ptr, const KeyType &key);


      bool DeltaInsert(PID cur, BWNode *node_ptr, const KeyType &key, const ValueType &value);

      // TODO: handle MergeEntryNode
      PID GetNextPID(PID cur, KeyType key) {
        const BWNode *cur_ptr = PIDTable::get_table().get(cur);

        while(cur_ptr->GetType()!=NInner) {
          if(cur_ptr->GetType()==NSplitEntry) {
            BWSplitEntryNode<KeyType> *split_entry_ptr = static_cast<BWSplitEntryNode<KeyType> *>(cur_ptr);

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

        BWInnerNode<KeyType> *inner_ptr = static_cast<BWInnerNode<KeyType> *>(cur_ptr);
        // TODO: implement scan_key;

        return PIDTable::PID_ROOT;
      }

    private:
      bool InsertEntryUtil(KeyType key, ValueType value, std::vector<PID> &path, PID cur);


      void ScanKeyUtil(PID cur, KeyType key, std::vector<ValueType> &ret);

    public :
      BWTree(IndexMetadata *indexMetadata): comparator_(indexMetadata), key_equality_checker_(indexMetadata) {

      }

      bool InsertEntry(KeyType key, ValueType value) {
        std::vector<PID> path;
        bool ret = InsertEntryUtil(key, value, path, PIDTable::PID_ROOT);
        return ret;
      }

      void ScanKey(KeyType key, std::vector<ValueType> &ret) {
        ScanKeyUtil(PIDTable::PID_ROOT, key, ret);
      }

      void ScanAllKeys(std::vector<ValueType> &ret) {
        PIDTable pidTable = PIDTable::get_table();
        const BWNode *node_ptr = pidTable.get(PIDTable::PID_ROOT);

        while (node_ptr->GetType() != NLeaf) {
          BWInnerNode<KeyType> *cur_ptr = static_cast<BWInnerNode<KeyType> *>(node_ptr);

          PID next_pid = cur_ptr->GetPIDs()[0];
          node_ptr = pidTable.get(next_pid);
        }

        // reach first leaf node

      }
    };


    ////////////////////////////////////////////////////////////////



  }  // End index namespace
}  // End peloton namespace
