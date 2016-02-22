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
#include "backend/index/pid_table.h"
#include <backend/common/value.h>

#include <vector>
#include <map>
#include <set>
#include <chrono>

namespace peloton {
  namespace index {


#define EPOCH 10 // unit : ms
#define NULL_PID 0
#define ROOT_PID 1 // root pid should be 1

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
      virtual NodeType GetType() = 0;

      virtual BWNode *GetNext() = 0;

      virtual PID GetLeft() const = 0;

      virtual PID GetRight() const = 0;

      inline bool IfLeafNode() {
        return if_leaf_;
      }

      inline bool IfInnerNode() {
        return !if_leaf_;
      }

      inline bool IfChainTooLong() {
        return chain_length_>max_chain_len;
      }

      inline bool IfOverflow() {
        return slot_usage_>max_node_size;
      }

      inline bool IfUnderflow() {
        return slot_usage_<min_node_size;
      }

      size_type GetSlotUsage() { return slot_usage_; }

      size_type GetChainLength() { return chain_length_; }

      virtual ~BWNOde() { };

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
      virtual NodeType GetType() = 0;

      BWNode *GetNext() {
        return NULL;
      };

      PID GetLeft() const {
        return left_;
      }

      PID GetRight() const {
        return right_;
      }

    protected:
      BWNormalNode(const size_type &slot_usage, const size_type &chain_length, bool if_leaf, const KeyType &low, const KeyType &high, const PID &left,
                   const PID &right): BWNode(slot_usage, chain_length, if_leaf),
                               low_key_(low),
                               high_key_(high),
                               left_(left),
                               right_(right) { }

      const KeyType low_key_, high_key_;
      const PID left_, right_;

    };

    template<typename KeyType>
    class BWInnerNode: public BWNormalNode<KeyType> {
    protected:
      const std::vector<KeyType> keys_;
      const std::vector<PID> children_;

    public:
      // keys and children should not be used anymore
      BWInnerNode(std::vector<KeyType> &keys, std::vector<PID> &children, const PID &left, const PID &right):
              BWNormalNode(keys.size(), 0, false, keys.front(), keys.back(), left, right), keys_(std::move(keys)), children_(std::move(children)) {

      }

      NodeType GetType() {
        return NInner;
      }

      std::vector<KeyType> GetKeys() const {
        return keys_;
      }

      std::vector<PID> GetPIDs() const {
        return children_;
      }

    };

    template<typename KeyType, typename ValueType, class KeyComparator>
    class BWLeafNode: public BWNormalNode<KeyType> {
    public:
      // keys and values should not be used anymore
      BWLeafNode(std::vector<KeyType> &keys, std::vector<ValueType> &values, const PID &left, const PID &right):
              BWNormalNode(keys.size(), 0, true, keys.front(), keys.back(), left, right), keys_(std::move(keys)), values_(std::move(values)) {
      }

      // TODO: add iterator
      std::vector<KeyType> GetKeys() { return keys_; }

      std::vector<ValueType> GetValues() {
        return values_;
      }

    protected:
      const std::vector<KeyType> keys_;
      const std::vector<ValueType> values_;

    public:
      NodeType GetType() {
        return NLeaf;
      }

      void ScanKey(const KeyType &key, const KeyComparator &comparator, std::vector<ValueType> &ret) {

      }
    };

// template<typename KeyType>
// Add your declarations here
    class BWDeltaNode: public BWNode {
    public:
      virtual NodeType GetType() = 0;


      BWNode *GetNext() {
        return next;
      }

      PID GetLeft() const {
        return NULL_PID;
      }

      virtual PID GetRight() {
        return NULL_PID;
      }

    protected:

      BWDeltaNode(size_type slot_usage, size_type chain_length, BWNode *n): BWNode(slot_usage, chain_length, false) { }

      // omit const
      BWNode *next;

    };

    template<typename KeyType, typename ValueType,>
    class BWInsertNode: public BWDeltaNode {
    public:
      BWInsertNode(size_type slot_usage, size_type chain_length, BWNode *next, KeyType k, ValueType v):
              BWDeltaNode(slot_usage, chain_length, next), key(k), value(v) { }

      NodeType GetType() {
        return NInsert;
      }

      KeyType GetKey() const {
        return key;
      }

      ValueType GetValue() const {
        return value;
      }

    protected:
      const KeyType key;
      const ValueType value;

    };

    template<typename KeyType>
    class BWDeleteNode: public BWDeltaNode {
    public:
      NodeType GetType() {
        return NDelete;
      }

      BWDeleteNode(size_type slot_usage, size_type chain_length, BWNode *next, KeyType k):
              BWDeltaNode(slot_usage, chain_length, next), key(k) { }

      KeyType GetKey() const {
        return key;
      }

    protected:
      const KeyType key;
    };

    template<typename KeyType>
    class BWSplitNode: public BWDeltaNode {
    public:
      NodeType GetType() {
        return NSplit;
      }

      KeyType &GetSplitKey() const {
        return key;
      }

      PID GetRightPID() const {
        return right;
      }

      BWSplitNode(size_type slot_usage, size_type chain_length, BWNode *next, KeyType k, PID r, PID o):
              BWDeltaNode(slot_usage, chain_length, next), key(k), right(r), old(o) { }

    protected:
      const KeyType key;
      // From Andy's slide, split node must have three child.
      // 1. old left.
      // 2. old right.
      // 3. new

      // new one
      const PID right;

      // old right
      const PID old;
    };

    template<typename KeyType>
    class BWSplitEntryNode: public BWDeltaNode {
    public:
      BWSplitEntryNode(size_type slot_usage, size_type chain_length, BWNode *next, KeyType low, KeyType high,
                       PID t):
              BWDeltaNode(slot_usage, chain_length, next), low_key(low), high_key(high), to(t) { }

      NodeType GetType() {
        return NSplitEntry;
      }

      KeyType GetLowKey() {
        return low_key;
      }

      KeyType GetHightKey() {
        return high_key;
      }

      PID GetNextPID() {
        return to;
      }

    protected:
      const KeyType low_key, high_key;
      const PID to;
    };

    template<typename KeyType>
    class BWRemoveNode: public BWDeltaNode {
    public:
      BWRemoveNode(size_type slot_usage, size_type chain_length, BWNode *next):
              BWDeltaNode(slot_usage, chain_length, next) { }

      NodeType GetType() {
        return NRemove;
      }
    };

    template<typename KeyType>
    class BWMergeNode: public BWDeltaNode {
    public:
      BWMergeNode(size_type slot_usage, size_type chain_length, BWNode *next, BWNode *r):
              BWDeltaNode(slot_usage, chain_length, next), right(r) { }

      NodeType GetType() {
        return NMerge;
      }

    protected:
      const BWNode *right;
    };

    // TODO
    template<typename KeyType>
    class MergeEntryNode: public BWDeltaNode {
    public:
      MergeEntryNode(size_type slot_usage, size_type chain_length, BWNode *next):
              BWDeltaNode(slot_usage, chain_length, next) { }

      NodeType GetType() {
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
      virtual OpType GetOpType() = 0;

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

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    class BWTree {

      KeyComparator comparator;
      KeyEqualityChecker keyEqualityChecker;

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
            BWNode *parent_ptr = pid_table.get(path[path.size()-2]);
            BWNode *cur_ptr = parent_ptr;

            while(cur_ptr->GetType()!=NInner) {
              if(cur_ptr->GetType()==NSplitEntry) {
                BWSplitEntryNode <KeyType> *split_ptr = static_cast<BWSplitEntryNode <KeyType> *>(cur_ptr);
                if(split_ptr->GetLowKey()==low_key) {
                  return;
                }
              }
              else {
                // TODO: then what?
              }

              cur_ptr = cur_ptr->GetNext();
            }

//            // Step 2: find high key
//            std::vector<KeyType> keys = static_cast<BWInnerNode<KeyType> *>(cur_ptr)->GetKeys();
//            size_t i;
//            for (i = 0; i < keys.size(); i++) {
//              if (comparator(low_key, keys[i])) {
//                high_key = keys[i];
//                break;
//              }
//            }
//
//            if (i == keys.size()) {
//              // TODO: high_key should equal to infinite.
//            }


            // Step 3: try to finish second step.
            size_type slot_usage = parent_ptr->GetSlotUsage();
            size_type chain_len = parent_ptr->GetChainLength();

            // When only one key, low_key should be equal to high_key.
            BWNode *split_entry_ptr = new BWSplitEntryNode(slot_usage+1,
                                                           chain_len+1, parent_ptr, low_key, low_key, right_pid);

            PID split_entry_pid = pid_table.allocate_PID();
            bool ret = pid_table.bool_compare_and_swap(split_entry_pid, 0, split_entry_ptr);
            if(ret==false) {
              delete split_entry_ptr;
              pid_table.free_PID(split_entry_pid);
              continue;
            }

            ret = pid_table.bool_compare_and_swap(path[path.size()-2], parent_ptr, split_entry_ptr);
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

      bool Consolidate(PID cur, BWNode *node_ptr);

      BWInnerNode *ConstructConsolidatedInnerNode(BWNode *node_chain);
      void ConstructConsolidatedLeafNodeInternal(BWNode *node_chain, std::vector<KeyType> &keys, std::vector<ValueType> &values, PID &left, PID &right);

      BWLeafNode *ConstructConsolidatedLeafNode(BWNode *node_chain);
      void ConstructConsolidatedInnerNodeInternal(BWNode *node_chain, std::vector<KeyType> &keys, std::vector<PID> &children, PID &left, PID &right);

      void ConsolidateInsertNode(BWInsertNode *node, std::vector<KeyType> &keys, std::vector<ValueType> &values);

      void ConsolidateSplitNode(BWSplitNode *node, std::vector<KeyType> &keys, std::vector<ValueType> &values);
      void ConsolidateSplitNode(BWSplitNode *node, std::vector<KeyType> &keys, std::vector<PID> &children);

      void ConsolidateSplitEntryNode(BWSplitEntryNode *node, std::vector<KeyType> &keys, std::vector<PID> &children);

      PID GetRightPID(BWNode *node_ptr) {
        BWNode *cur_ptr = node_ptr;
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
      void SplitInnerNodeUlti(BWNode *node_ptr, KeyType &split_key, PID &right_pid,
                              std::vector<KeyType> &keys,
                              std::vector<PID> &pids) {
        std::vector<KeyType> key_stack;
        std::vector<PID> pid_stack;

        BWNode *cur_ptr = node_ptr;
        while(cur_ptr->GetType()!=NInner) {
          if(cur_ptr->GetType()==NSplitEntry) {
            BWSplitEntryNode <KeyType> *split_entry_ptr = static_cast<BWSplitEntryNode <KeyType> *>(cur_ptr);

            key_stack.push_back(split_entry_ptr->GetLowKey());
            pid_stack.push_back(split_entry_ptr->GetNextPID());
          }
          else {
            // TODO: handle MergeEntryNode and Split Node, if any
          }

          cur_ptr = cur_ptr->GetNext();
        }

        // come to inner node
        BWInnerNode <KeyType> *inner_ptr = static_cast<BWInnerNode <KeyType> *>(cur_ptr);
        keys = inner_ptr->GetKeys();
        pids = inner_ptr->GetPIDs();
        right_pid = inner_ptr->GetRight();

        while(!key_stack.empty()) {
          KeyType k = key_stack.back();
          PID p = pid_stack.back();
          key_stack.pop_back();
          pid_stack.pop_back();

          int pos = BinaryGreaterThan(keys, k, comparator);
          keys.insert(keys.begin()+pos, k);
          pids.insert(pids.begin()+pos+1, k);
        }

        assert(keys.size()>0);

        split_key = keys[keys.size()/2];
      }

      // TODO: handle duplicate keys
      void SplitLeafNodeUtil(BWNode *node_ptr, KeyType &split_key, PID &right_pid,
                             std::vector<KeyType> &keys,
                             std::vector<ValueType> &values) {
        std::vector<OpType> op_stack;
        std::vector<KeyType> key_stack;
        std::vector<ValueType> value_stack;

        BWNode *cur_ptr = node_ptr;
        // What kind of Delta?
        while(cur_ptr->GetType()!=NLeaf) {
          if(cur_ptr->GetType()==NInsert) {
            BWInsertNode <KeyType, ValueType> *insert_ptr = static_cast<BWInsertNode <KeyType, ValueType> *>(cur_ptr);

            op_stack.push_back(OInsert);
            key_stack.push_back(insert_ptr->GetKey());
            value_stack.push_back(insert_ptr->GetValue());
          }
          else if(cur_ptr->GetType()==NDelete) {
            BWDeleteNode <KeyType> *delete_ptr = static_cast<BWDeleteNode <KeyType> *>(cur_ptr);

            op_stack.push_back(ODelete);
            key_stack.push_back(delete_ptr->GetKey());
          }
          else {
            //TODO: how about next?
            continue;
          }

          cur_ptr = cur_ptr->GetNext();
        }

        std::map<KeyType, ValueType> key_value_map;
        BWLeafNode <KeyType, ValueType> *leaf_ptr = static_cast<BWLeafNode <KeyType, ValueType> *>(cur_ptr);

        right_pid = leaf_ptr->GetRight();

        std::vector<KeyType> leaf_keys = leaf_ptr->GetKeys();
        std::vector<ValueType> leaf_values = leaf_ptr->GetValues();

        for(size_t i = 0; i<leaf_keys.size(); i++) {
          key_value_map[leaf_keys[i]] = leaf_values[i];
        }

        while(op_stack.size()>0) {
          OpType op = op_stack.back();
          op_stack.pop_back();

          if(op==OInsert) {
            key_value_map[key_stack.back()] = value_stack.back();
            key_stack.pop_back();
            value_stack.pop_back();
          }
          else if(op==ODelete) {
            key_value_map.erase(key_value_map.find(key_stack.back()));
            key_stack.pop_back();
          }
          else {
            // TODO:: how about other type
          }
        }

        std::map<KeyType, ValueType>::iterator iter = key_value_map.begin();
        for(iter; iter!=key_value_map.end(); iter++) {
          keys.push_back(iter->first);
          values.push_back(iter->second);
        }

        split_key = keys[keys.size()/2];
      }

      // Duplicate keys handled
      bool SplitInnerNode(PID cur, BWNode *node_ptr, KeyType &split_key, PID &right_pid) {
        std::vector<KeyType> keys;
        std::vector<PID> pids;
        PID old_right = NULL_PID;

        SplitLeafNodeUtil(node_ptr, split_key, old_right, keys, pids);

        // Step 1: Create new right page;
        PIDTable pidTable = PIDTable::get_table();
        right_pid = pidTable.allocate_PID();

        BWNode *right_ptr = NULL;
        right_ptr = new BWInnerNode(keys.size(), 0, true, keys[0], keys[keys.size()-1], cur, old_right, keys, pids);
        bool ret = pidTable.bool_compare_and_swap(right_pid, 0, right_ptr);
        if(ret==false) {
          // TODO: need virtual destructors.
          delete right_ptr;
          pidTable.free_PID(right_pid);
          return false;
        }

        // Step 2: Create split node
        BWNode *split_ptr = NULL;
        split_ptr = new BWSplitNode(node_ptr->GetSlotUsage()-keys.size(), node_ptr->GetChainLength()+1,
                                    node_ptr, split_key, right_pid, old_right);

        // Step 3: install split node
        ret = pidTable.bool_compare_and_swap(cur, node_ptr, split_ptr);
        if(ret==false) {
          delete right_ptr;
          delete split_ptr;
          pidTable.free_PID(right_pid);

          return false;
        }

        return true;
      }

      bool SplitLeafNode(PID cur, BWNode *node_ptr, KeyType &split_key, PID &right_pid) {
        std::vector<KeyType> keys;
        std::vector<ValueType> values;
        PID old_right = NULL_PID;

        SplitLeafNodeUtil(node_ptr, split_key, old_right, keys, values);
        assert(keys.size()>=1);

        // Step 1: Create new right page;
        PIDTable pidTable = PIDTable::get_table();
        right_pid = pidTable.allocate_PID();

        BWNode *right_ptr = NULL;
        right_ptr = new BWLeafNode(keys.size(), 0, true, keys[0], keys[keys.size()-1], cur, old_right, keys, values);
        bool ret = pidTable.bool_compare_and_swap(right_pid, 0, right_ptr);
        if(ret==false) {
          // TODO: need virtual destructors.
          delete right_ptr;
          pidTable.free_PID(right_pid);
          return false;
        }

        // Step 2: Create split node
        BWNode *split_ptr = NULL;
        split_ptr = new BWSplitNode(node_ptr->GetSlotUsage()-keys.size(), node_ptr->GetChainLength()+1,
                                    node_ptr, split_key, right_pid, old_right);

        // Step 3: install split node
        ret = pidTable.bool_compare_and_swap(cur, node_ptr, split_ptr);
        if(ret==false) {
          delete right_ptr;
          delete split_ptr;
          pidTable.free_PID(right_pid);

          return false;
        }

        return true;
      }

      bool Split(PID cur, BWNode *node_ptr, KeyType &split_key, PID &right_pid, NodeType right_type) {
        if(right_type==NLeaf) {
          return SplitLeafNode(cur, node_ptr, split_key, right_pid);
        }
        else {
          return SplitInnerNode(cur, node_ptr, split_key, right_pid);
        }
      }

      bool DupExist(BWNode *node_ptr, const KeyType &key) {
        // assume node_ptr is the header of leaf node
        std::unordered_map<KeyType, int> map;
        int delta = 0;
        KeyEqualityChecker equality_checker;
        do {
          NodeType node_type = node_ptr->GetType();
          if(node_type==NLeaf) {
            BWLeafNode *leaf_node_ptr = static_cast<BWLeafNode <KeyType, ValueType> *>(node_ptr);
            std::vector<KeyType> keys = leaf_node_ptr->GetKeys();
            for(auto iter = keys.begin(); iter!=keys.end(); ++iter) {
              if(equality_checker(*iter, key)&&(++delta)>0) {
                return true;
              }
            }
            return false;
          }
          else if(node_type==NInsert) {
            BWInsertNode *insert_node_ptr = static_cast<BWInsertNode <KeyType, ValueType> *>(node_ptr);
            if(equality_checker(insert_node_ptr->GetKey(), key)&&(++delta>0)) {
              return true;
            }
          }
          else if(node_type==NDelete) {
            BWDeleteNode *delete_node_ptr = static_cast<BWDeleteNode <KeyType> *>(node_ptr);
            if(equality_checker(delete_node_ptr->GetKey(), key)) {
              --delta;
            }
            //TODO: complete this
          }
          else if(node_type==NSplitEntry) {


//          } else if (node_type == NMergeEntry){

          }
          node_ptr = (static_cast<BWDeltaNode *>(node_ptr))->GetNext();
        } while(true);
      }


      bool DeltaInsert(PID cur, BWNode *node_ptr, const KeyType &key, const ValueType &value) {
        PIDTable pidTable = PIDTable::get_table();
        BWInsertNode *insert_node_ptr = new BWInsertNode<KeyType, ValueType>(
                node_ptr->GetSlotUsage()+1, node_ptr->GetChainLength()+1, node_ptr, key, value);
        if(pidTable.bool_compare_and_swap(cur, node_ptr, insert_node_ptr))
          return true;
        else
          delete (insert_node_ptr);
      }

      // TODO: handle MergeEntryNode
      PID GetNextPID(PID cur, KeyType key) {
        BWNode *cur_ptr = PIDTable::get_table().get(cur);

        while(cur_ptr->GetType()!=NInner) {
          if(cur_ptr->GetType()==NSplitEntry) {
            BWSplitEntryNode <KeyType> *split_entry_ptr = static_cast<BWSplitEntryNode <KeyType> *>(cur_ptr);

            if(!comparator(key, split_entry_ptr->GetLowKey())&&comparator(key, split_entry_ptr->GetHightKey())) {
              return split_entry_ptr->GetNextPID();
            }
          }
          else if(cur_ptr->GetType()==NMergeEntry) {

          }
          else {

          }

          cur_ptr = cur_ptr->GetNext();
        }

        BWInnerNode <KeyType> *inner_ptr = static_cast<BWInnerNode <KeyType> *>(cur_ptr);
        // TODO: implement scan_key;

        return ROOT_PID;
      }

    private:
      bool InsertEntryUtil(KeyType key, ValueType value, std::vector<PID> &path, PID cur) {

        BWNode *node_ptr = NULL;
        while(true) {
          // TODO: Get from last element of path.
          node_ptr = NULL;
          // If current is split node
          if(node_ptr->GetType()==NSplit) {
            BWSplitNode <KeyType> *split_ptr = static_cast<BWSplitNode <KeyType> *>(node_ptr);
            InsertSplitEntry(path, split_ptr->GetSplitKey(), split_ptr->GetRightPID());
            //continue;
          }

          // If delta chain is too long
          if(node_ptr->IfChainTooLong()) {
            Consolidate(cur, node_ptr);
            path.pop_back();
            continue;
          }

          // TODO: should get size from node_ptr
          if(node_ptr->IfOverflow()) {
            KeyType split_key;
            PID right_pid;
            NodeType right_type = Unknown;
            if(node_ptr->IfLeafNode()) {
              right_type = NLeaf;
            }
            else {
              right_type = NInner;
            }
            bool ret = Split(cur, node_ptr, split_key, right_pid, right_type);
            if(ret==true) {
              // note: only do two step.
              InsertSplitEntry(path, split_key, right_pid);
            }
            // retry
            path.pop_back();
            continue;
          }

          if(node_ptr->IfLeafNode()) {
            if(!Duplicate&&DupExist(node_ptr, key)) {
              return false;
            }

            if(!DeltaInsert(cur, node_ptr, key, value)) {
              continue;
            }

            return true;
          }
          else {
            PID next_pid = GetNextPID(path.back(), key);
            path.push_back(cur);
          }
        }
      }

      void ScanKeyUtil(PID cur, KeyType key, std::vector<ValueType> &ret) {
        PIDTable pidTable = PIDTable::get_table();
        BWNode *node_ptr = pidTable.get(cur);

        if(node_ptr->GetType()==NLeaf) {
          unsigned short delete_count = 0;

          while(true) {
            // TODO: assume no renmoe node delta and merge node delta.
            // Only handle split node, insert node, delete node.
            if(node_ptr->GetType()==NSplit) {
              BWSplitNode <KeyType> *split_ptr = static_cast<BWSplitNode <KeyType> *>(node_ptr);
              KeyType split_key = split_ptr->GetSplitKey();
              if(comparator(key, split_key)==true) { // == true means key < split_key
                node_ptr = split_ptr->GetNext();
              }
              else {
                node_ptr = pidTable.get(split_key->GetRightPID());
              }
            }
            else if(node_ptr->GetType()==NInsert) {
              BWInsertNode <KeyType, ValueType> *insert_ptr = static_cast<BWInsertNode <KeyType, ValueType> *>(node_ptr);
              if(keyEqualityChecker(insert_ptr->GetKey(), key)==true&&delete_count==0) {
                ret.push_back(insert_ptr->GetKey());
                if(!Duplicate) {
                  break;
                }
              }
              else if(keyEqualityChecker(insert_ptr->GetKey(), key)==true&&delete_count>0) {
                delete_count--;
              }

              node_ptr = insert_ptr->GetNext();
            }
            else if(node_ptr->GetType()==NDelete) {
              BWDeleteNode <KeyType> *delete_ptr = static_cast<BWDeleteNode <KeyType> *>(node_ptr);
              if(keyEqualityChecker(delete_ptr->GetKey(), key)==false) {
                delete_count++;
                if(!Duplicate) {
                  break;
                }
              }
              else if(keyEqualityChecker(delete_ptr->GetKey(), key)==true) {
                delete_count++;
              }

              node_ptr = delete_ptr->GetNext();
            }
            else if(node_ptr->GetType()==NLeaf) {
              BWLeafNode <KeyType, ValueType> *leaf_ptr = static_cast<BWLeafNode <KeyType, ValueType> *>(node_ptr);
              // TOOD: assume no scan key here.
              leaf_ptr->ScanKey(key, comparator, ret);
              break;
            }
            else { // TODO: handle other cases here.
              break;
            }
          }
        }
        else {
          PID next_pid = GetNextPID(cur, key);
          ScanKeyUtil(next_pid, key, ret);
        }
      }

    public :
      BWTree(IndexMetadata *indexMetadata): comparator(indexMetadata), keyEqualityChecker(indexMetadata) {

      }

      bool InsertEntry(KeyType key, ValueType value) {
        std::vector<PID> path;
        bool ret = InsertEntryUtil(key, value, path, ROOT_PID);
        return ret;
      }

      void ScanKey(KeyType key, std::vector<ValueType> &ret) {
        ScanKeyUtil(ROOT_PID, key, ret);
      }
    };

  }  // End index namespace
}  // End peloton namespace
