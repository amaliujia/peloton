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
    int slot_use;

    inline int GetSlotUse() {
      return slot_use;
    }

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
   
    // logical pointer to next delta node 
    oid_t next_delta;
    
    inline void SetNextDelta(oid_t pid) {
      next_delta = pid;
    }

    inline void Initialize(NodeType t) {
      BWNode::Initialize(t);
      chain_len = 0;
    }

    inline void IncrementChainLen() {
      chain_len++;
    }
    
    inline void IncrementChainLen(int base) {
      chain_len = base + 1;
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
  void InsertEntryUtil(oid_t node_pid, const KeyType& key, const ValueType& value, const KeyComparator& comparator);

public:
  /* Constructors and Deconstructors. */
  BWTree() {
    BWTreeInitialize(); 
  }

};

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::InsertEntryUtil(oid_t node_pid, const KeyType& key, const ValueType& value, const KeyComparator& comparator) {
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
    /*
     * Find the delta chain of leaf node
     * 1. Apply current operation.
     * 2. If fail, retry (say 5 times), then return false.
     * 3. If evetually succeed, check if need 
     *  a. conlidate
     *  b. split
     * 4. Conlidation should be applied before split
     * 5. to be continue... 
     */
     DeltaNode *delta_ptr = static_cast<DeltaNode *>(node_ptr);

     // Step 1: apply current operation, at most five times.
     InsertDelta *insert_delta = new InsertDelta();
     insert_delta->Initialize(insert, key);
     insert_delta->SetNextDelta(node_pid);
     insert_delta->IncrementChainLen(delta_ptr->GetChainLen());
     // insert_delta->IncrementSlotUsage(delta_ptr->GetSlotUsage());

  } else if (node_ptr->IfLeafNode()) {
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
  } else {
    // TODO: handle inner delta
    // TODO: handle remove
    return false;
  } 
}


template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::InsertEntry(const KeyType& key, const ValueType& value, const KeyComparator& comparator) {
  InsertEntryUtil(ROOT_PID, key, value, comparator); 
  return false;
}

}  // End index namespace
}  // End peloton namespace
