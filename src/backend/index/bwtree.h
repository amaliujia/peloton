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
#define FIRST_INIT_LEAF_OFF 1
#define SECOND_INIT_LEAF_OFF 1 
#define RETRY_LIMIT 5
#define CONSOLIDATION_THRESHOLD 50

#include <utility> 
#include <mutex>
#include <vector>
#include <unordered_map>
#include <iostream>

#include "backend/common/types.h"
#include "backend/common/logger.h"

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
    seperate,
    split,
    remove, 
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
    // int slot_use;

    inline bool IfDeltaUpdate() const {
      return (type == insert) || (type == fdelete) || (type == seperate) ||
              (type == split) || (type == remove); 
    }

    inline bool IfLeafDelta() const {
      return (type == insert) || (type == fdelete);
    }

    inline bool IfInnerDelta() const {
      return (type == split) || (type == seperate);
    }

    inline bool IfSeperateKeyDelta() const {
      return (type == seperate);
    }

    inline void Initialize(const NodeType t) {
      type = t;
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

    inline virtual int GetSlotUsage() {
      return 0;
    }

    inline virtual int GetChainLen() {
      return 0;
    } 
  };

  class DeltaNode : public BWNode {
   public:
    int chain_len;
    int slot_num; 
    // logical pointer to next delta node 
    oid_t next_delta;

    inline void Initialize(NodeType t) {
      BWNode::Initialize(t);
      chain_len = 0;
      slot_num = 0;;
    }
    
    inline int GetSlotUsage() {
      return slot_num;
    }

    inline int GetChainLen() {
       return chain_len;
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
     std::vector<ValueType> value;
  
     // length of chain increases bt one 
     inline void Initialize(NodeType t, KeyType k, ValueType v) {
       DeltaNode::Initialize(t);
       key.push_back(k);
       value.push_back(v);
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
      return (key_slot.size() == inner_slot_max); 
    }

    // TODO:: what's this function used for?
    inline bool IfFew() const {
      return (key_slot.size() <= inner_slot_min);
    }

    // if root, no underflow problem.
    inline bool IfUnderflow() const {
      if (BWNode::IfRootNode()) {
        return false;
      }
      return (key_slot.size() < inner_slot_min);
    }

    inline int GetSlotUsage() {
      return key_slot.size();
    }
    
    inline int GetChainLen() {
      return 0; 
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
      return (key_slot.size() == leaf_slot_max); 
    }

    // TODO:: what's this function used for?
    inline bool IfFew() const {
      return (key_slot.size() <= leaf_slot_min);
    }

    // if first two leaf, no underflow problem
    inline bool IfUnderflow() const {
      if (if_original == true) {
        return false; 
      }
      
      return (key_slot.size() < leaf_slot_min);
    }
    
    inline int GetSlotUsage() {
      return key_slot.size();
    }

    inline int GetChainLen() {
      return 0;
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
  inline bool AtomicCompareAndSwapBool(BWNode **target,
      BWNode *old_v, BWNode *new_v) {
    return __sync_bool_compare_and_swap(target, old_v, new_v);
  }
  
  inline oid_t AtomicCompareAndSwapVal(BWNode **target,
      BWNode *old_v, BWNode *new_v) {
    if( __sync_bool_compare_and_swap(target, old_v, new_v)) {
      return old_v;
    }
    return 0;
  }

  oid_t AddPageToPIDTable(BWNode *node_ptr) {
     mtx.lock();
     oid_t cur_pid = AllocPID();
     pid_table[cur_pid] = node_ptr;
     // pid_table.insert(std::make_pair<oid_t, BWNode *>(cur_pid, node_ptr));  
     mtx.unlock(); 
     return cur_pid;
  }

  bool InsertDeltaUpdate(oid_t node_pid, BWNode *node_ptr, const KeyType& key, const ValueType& value); 

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
  bool InsertEntryUtil(oid_t node_pid, const KeyType& key, const ValueType& value, const KeyComparator& comparator);

public:
  /* Constructors and Deconstructors. */
  BWTree() {
    BWTreeInitialize(); 
  }
};

template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::InsertDeltaUpdate(oid_t node_pid, BWNode *node_ptr, const KeyType& key, const ValueType& value) {
  /*
   * Find the delta chain of leaf node
   * 1. Apply current operation.
   * 2. If fail, abort, return false.
   * 3. If evetually succeed, check if need 
   *  a. conlidate
   *  b. split
   * 4. Conlidation should be applied before split
   * 5. to be continue... 
   */
  // Step 1: apply current operation, at most five times.
  InsertDelta *insert_delta = new InsertDelta();
  insert_delta->Initialize(insert, key, value);
  insert_delta->next_delta = node_pid;
  insert_delta->chain_len = node_ptr->GetChainLen() + 1;
  insert_delta->slot_num = node_ptr->GetSlotUsage() + 1;

  bool success = AtomicCompareAndSwapBool(&pid_table[node_pid], node_ptr, insert_delta); 
  if (!success) {
   return false;
  }

  // TODO: Step 2: check if need  
  //if (insert_delta->chain_len >= CONSOLIDATION_THRESHOLD) {
  // LOG_DEBUG("Start to consolidation");
  //}

  // TODO: Step 3: check if need split 

  return true;
} 

template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::InsertEntryUtil(oid_t node_pid, const KeyType& key, const ValueType& value, const KeyComparator& comparator) {
  BWNode *node_ptr = pid_table[node_pid];
  
  if (node_ptr->IfInnerNode()) {
    BWInnerNode *inner_ptr = static_cast<BWInnerNode *>(node_ptr);
    // if is root node and if root node is null, then this is first key
    // should be inserted into key node.
    // TODO: not thread safe. 
    if (node_ptr->IfRootNode() && node_ptr->GetSlotUsage() == 0) {
      assert(inner_ptr->InsertKey(key, 0));
      return InsertEntryUtil(inner_ptr->pid_slot[SECOND_INIT_LEAF_OFF], key, value, comparator);   
    } 
    auto const& key_slot = inner_ptr->key_slot;
    for (int i = 0; i < key_slot.size(); i++) {
      if (comparator(key, key_slot[i])) {
        oid_t next_pid = inner_ptr->pid_slot[i]; 
        return InsertEntryUtil(next_pid, key, value, comparator); 
      } 
    }
    
    oid_t next_pid = inner_ptr->pid_slot[key_slot.size()];
    return InsertEntryUtil(next_pid, key, value, comparator); 
  } else if (node_ptr->IfLeafDelta()) {
    return InsertDeltaUpdate(node_pid, node_ptr, key, value); 
  } else if (node_ptr->IfLeafNode()) {
    return InsertDeltaUpdate(node_pid, node_ptr, key, value); 
  } else {
    // TODO: handle inner delta
    // TODO: handle remove
    return false;
  } 
}


template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::InsertEntry(const KeyType& key, const ValueType& value, const KeyComparator& comparator) {
  return InsertEntryUtil(ROOT_PID, key, value, comparator); 
}

}  // End index namespace
}  // End peloton namespace
