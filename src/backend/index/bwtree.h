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

#define TREE_DEGREE 2 
#define ROOT_PID 1

#include <utility> 
#include <mutex>
#include <vector>
#include <unordered_map>
#include <iostream>

#include "backend/common/types.h"


namespace peloton {
namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, class KeyComparator>
class BWTree {

public:
  // Constraints on the number of slots of tree nodes.
  static const unsigned short leaf_slot_max = TREE_DEGREE * 2;
  static const unsigned short leaf_slot_min = TREE_DEGREE;
  static const unsigned short inner_slot_max = TREE_DEGREE * 2;
  static const unsigned short inner_slot_min = TREE_DEGREE;

  // Flag that indicates if allow duplicate keys.
  // TODO:: Where to pass this flag into bwtree?
  // static const bool allow_dupulicate = false;

private:
  std::mutex mtx;

private:
  inline oid_t AllocPID() {
    oid_t ret = 0;

    next_pid++;
    ret = next_pid; 
    
    return ret;
  } 

private:
  enum NodeType {
    root,
    leaf,
    inner,
    update,
    insert,
    fdelete,  //delete is keyword, so use fdelete, aka, fake_delete, instead. 
    unknown 
  };

  class BWNode {
   public:
    // Level in the bw-tree, leaf node when level == 0
    NodeType type;
    
    // The number of slot in use.
    // Assume the number is n, then number of PID should be n + 1.
    // Of course, TREE_DEGREE <= n <= 2 * TREE_DEGREE.
    // And, TREE_DEGREE + 1 <= n <= 2 * TREE_DEGREE + 1
    
    // This varibale is also used in Delta nodes. 
    int slot_use;

    inline int GetSlotUse() {
      return slot_use;
    }

    inline void Initialize(const NodeType t) {
      type = t;
      slot_use = 0; 
    }

    inline bool IfInnerNode() const {
      return (type == inner) || (type == root);
    }

    inline bool IfLeafNode() const {
      return (type == leaf); 
    }

    inline bool IfRootNode() const {
      return (type == root);
    }
    
    inline bool IfInsertDelta() const {
      return (type == insert);
    }

    inline bool IfDeleteDelta() const {
      return (type == fdelete);
    } 

    inline void IncrementSlot() {
      slot_use++;
    }
    
    inline void IncrementSlot(int base) {
      slot_use = base + 1;
    }
   
    inline void SetSlotUsage(int s) {
      slot_use = s;
    } 

    inline int GetSlotUsage() {
      return slot_use;
    } 
  };

  class DeltaNode : public BWNode {
   public:
    int chain_len;

    inline void Initialize(NodeType t) {
      BWNode::Initalize(t);
      chain_len = 0;
    }

    inline void IncrementChainLen() {
      chain_len++;
    }
    
    inline void IncrementChainLen(int base) {
      chain_len = base + 1;
    }
  };

  class DeleteDelta : public DeltaNode {
   public:
    std::vector<KeyType> key;

    inline void Initialize(NodeType t, KeyType k) {
        DeltaNode::Initialize(t);
        key.push_back(k);
    }
  };

  class InsertDelta : public DeltaNode {
   public:
     std::vector<KeyType> key;
      
     // length of chain increases bt one 
     inline void Initialize(NodeType t, KeyType k) {
        DeltaNode::Initialize(t);
        key.push_back(k);
     }

  };

  class BWInnerNode : public BWNode {
   public:
    // slot that saves keys 
    std::vector<KeyType> key_slot;

    // slot that saves PIDs
    std::vector<oid_t> pid_slot;

    inline void Initialize(const NodeType t) {
      BWNode::Initialize(t);
     
      // resize to prealloc vector space, try to avoid push_back. 
      key_slot.reserve(inner_slot_max);
      pid_slot.reserve(inner_slot_max + 1);
    }

    inline bool IfFull() const {
      return (BWNode::slot_use == inner_slot_max); 
    }

    // TODO:: what's this function used for?
    inline bool IfFew() const {
      return (BWNode::slot_use <= inner_slot_min);
    }

    // if root, no underflow problem.
    inline bool IfUnderflow() const {
      if (BWNode::IfRootNode()) {
        return false;
      }
      return (BWNode::slot_use < inner_slot_min);
    }

    inline bool InsertKey(const KeyType& key, int offset) {
      if (IfFull()) {
        return false;
      }

      key_slot.insert(key_slot.begin() + offset, key);
      return true;
    }

    inline bool InsertPID(const oid_t& pid, int offset) {
      if (IfFull()) {
        return false; 
      }
      
      pid_slot(pid_slot.begin() + offset, pid);
      return true; 
    } 

  };

  class BWLeafNode : public BWNode {
   public: 
    std::vector<KeyType> key_slot;
    
    std:: vector<ValueType> value_slot;

    oid_t prev_leaf;

    oid_t next_leaf;

    bool if_original;
    
    inline void Initialize(const NodeType t) {
      BWNode::Initialize(t);
      key_slot.reserve(leaf_slot_max);
      value_slot.reserve(leaf_slot_max);

      prev_leaf = 0;
      next_leaf = 0; 
    }
    
    inline void Initialize(const NodeType t, const bool flag) {
      Initialize(t);
      if_original = flag; 
    }

    inline bool IfFull() const {
      return (BWNode::slot_use == leaf_slot_max); 
    }

    // TODO:: what's this function used for?
    inline bool IfFew() const {
      return (BWNode::slot_use <= leaf_slot_min);
    }

    // if first two leaf, no underflow problem
    inline bool IfUnderflow() const {
      if (if_original == true) {
        return false; 
      }

      return (BWNode::slot_use < leaf_slot_min);
    }

    inline bool InsertKey(const KeyType& key, int offset) {
      if (IfFull()) {
        return false;
      }

      key_slot.insert(key_slot.begin() + offset, key);
      return true;
    }

    inline bool InsertValue(const ValueType& value, int offset) {
      if (IfFull()) {
        return false;
      }
      
      value_slot.insert(value_slot.begin() + offset, value);
      return true;
    }
  };

private:
  // mapping table
  // Not thread safe. When two threads write two different KeyValue pair,
  // behaviour is not defined.
  // Arrary would be better. 
  std::unordered_map<oid_t, BWNode *> pid_table; 

  // Next pid
  // PID is the id for each node. PID should start from 2, therefore
  // PID 0 is NULL, PID 1 is root id.
  // Note: root node have two ids. one is assigned when node created,
  // another one is 1.
  oid_t next_pid;

private:
  inline bool AtomicCompareAndSwapBool(oid_t *target,
      oid_t old_v, oid_t new_v) {
    return __sync_bool_compare_and_swap(target, old_v, new_v);
  }
  
  inline oid_t AtomicCompareAndSwapVal(oid_t *target,
      oid_t old_v, oid_t new_v) {
    if( __sync_bool_compare_and_swap(target, old_v, new_v)) {
      return old_v;
    }
    return 0;
  }

  inline oid_t AddPageToPIDTable(BWNode *node_ptr) {
     mtx.lock();
     oid_t cur_pid = AllocPID();
     pid_table[cur_pid] = node_ptr;
     // pid_table.insert(std::make_pair<oid_t, BWNode *>(cur_pid, node_ptr));  
     mtx.unlock(); 
     return cur_pid;
  }

public: 
  void BWTreeInitialize() {
    next_pid = 0;
    assert(pid_table.size() == 0);
    
    // Init root node
    BWInnerNode *root_ptr = new BWInnerNode(); 
    root_ptr->Initialize(root); 
    
    assert(AddPageToPIDTable(static_cast<BWNode *>(root_ptr)) == ROOT_PID);
    
    // add two leaf node
    BWLeafNode *left_ptr = new BWLeafNode();
    left_ptr->Initialize(leaf, true); 
    
    BWLeafNode *right_ptr = new BWLeafNode();
    right_ptr->Initialize(leaf, true);

    oid_t left_pid = AddPageToPIDTable(static_cast<BWNode *>(left_ptr));
    oid_t right_pid = AddPageToPIDTable(static_cast<BWNode *>(right_ptr));
    
    right_ptr->prev_leaf = left_pid;
    left_ptr->next_leaf = right_pid;
    root_ptr->pid_slot[0] = left_pid;
    root_ptr->pid_slot[1] = right_pid;   
  }

public:
  bool InsertEntry(const KeyType& key, const ValueType& value, const KeyComparator& comparator); 

private:
  void InsertEntryUtil(BWNode *node_ptr, const KeyType& key, const ValueType& value, const KeyComparator& comparator);

public:
  /* Constructors and Deconstructors. */
  BWTree() {
    BWTreeInitialize(); 
  }

};

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::InsertEntryUtil(__attribute__((unused)) BWNode *node_ptr,  __attribute__((unused)) const KeyType& key, __attribute__((unused)) const ValueType& value, __attribute__((unused)) const KeyComparator& comparator) {
  if (node_ptr->IfInnerNode()) {
    BWInnerNode *inner_ptr = static_cast<BWInnerNode *>(node_ptr);
    auto const& key_slot = inner_ptr->key_slot;
    for (int i = 0; i < key_slot.size(); i++) {
      if (comparator(key, key_slot[i])) {
        oid_t next_pid = inner_ptr->pid_slot[i]; 
        return InsertEntryUtil(pid_table[next_pid], key, value, comparator); 
      } 
    }
    
    oid_t next_pid = inner_ptr->pid_slot[key_slot.size()];
    return InsertEntryUtil(pid_table[next_pid], key, value, comparator); 
  } else {
    BWLeafNode *leaf_ptr = static_cast<BWLeafNode *>(node_ptr);
    auto const& key_slot = leaf_ptr->key_slot;
    for (int i = 0; i < key_slot.size(); i++) {
      if(comparator(key, key_slot[i])) {
        leaf_ptr->InsertKey(key, i);
        leaf_ptr->InsertValue(value, i); 
      }
    }
    leaf_ptr->InsertKey(key, key_slot.size() - 1);
    leaf_ptr->InsertValue(value, key_slot.size() - 1); 
  } 
}


template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::InsertEntry( __attribute__((unused)) const KeyType& key, __attribute__((unused)) const ValueType& value, __attribute__((unused)) const KeyComparator& comparator) {
  BWNode *root_ptr = pid_table[ROOT_PID]; 
  InsertEntryUtil(root_ptr, key, value, comparator); 
  return false;
}

}  // End index namespace
}  // End peloton namespace
