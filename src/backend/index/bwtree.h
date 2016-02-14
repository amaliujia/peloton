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

    mtx.lock();
    next_pid++;
    ret = next_pid; 
    mtx.unlock();

    return ret;
  } 

private:
  enum NodeType {
    root,
    leaf,
    inner,
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
    unsigned short slot_use;

    inline void Initialize(const NodeType t) {
      type = t;
      slot_use = 0; 
    }

    inline bool IfLeafNode() const {
      return (type == leaf); 
    }

    inline bool IfRootNode() const {
      return (type == root);
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
      key_slot.resize(inner_slot_max);
      pid_slot.resize(inner_slot_max + 1);
    }

    inline bool IfFull() const {
      return (BWNode::slot_use == inner_slot_max); 
    }

    // TODO:: what's this function used for?
    inline bool IfFew() const {
      return (BWNode::slot_use <= inner_slot_min);
    }

    // TODO:: how to merge node?
    inline bool IfUnderflow() const {
      return (BWNode::slot_use < inner_slot_min);
    } 
  };

  class BWLeafNode : public BWNode {
   public: 
    std::vector<KeyType> key_slot;
    
    std:: vector<ValueType> value_slot;

    oid_t prev_leaf;

    oid_t next_leaf;
    
    inline void Initialize(const NodeType t) {
      BWNode::Intialize(t);
      key_slot.resize(leaf_slot_max);
      value_slot.resize(leaf_slot_max);

      prev_leaf = 0;
      next_leaf = 0; 
    }

    inline bool IfFull() const {
      return (BWNode::slot_use == leaf_slot_max); 
    }

    // TODO:: what's this function used for?
    inline bool IfFew() const {
      return (BWNode::slot_use <= leaf_slot_min);
    }

    // TODO:: how to merge node?
    inline bool IfUnderflow() const {
      return (BWNode::slot_use < leaf_slot_min);
    }
  };

private:
  // mapping table 
  std::unordered_map<oid_t, BWNode *> pid_table; 

  // Next pid
  // PID is the id for each node. PID should start from 2, therefore
  // PID 0 is NULL, PID 1 is root id.
  // Note: root node have two ids. one is assigned when node created,
  // another one is 1.
  oid_t next_pid;

private:
  inline bool AtomicCompareAndSwapBool(BWNode *target,
      oid_t old_v, oid_t new_v) {
    return __sync_bool_compare_and_swap(target, old_v, new_v);
  }
  
  inline oid_t AtomicCompareAndSwapVal(BWNode *target,
      oid_t old_v, oid_t new_v) {
    if( __sync_bool_compare_and_swap(target, old_v, new_v)) {
      return old_v;
    }
    return 0;
  }
  
  void BWTreeIntialize() {
    next_pid = 0;
    assert(pid_table.size() == 0);
    
    // Init root node
    BWInnerNode *root_ptr = new BWInnerNode(); 
    root_ptr->Initialize(root); 
    oid_t root_pid = AllocPID();
    assert(root_pid == ROOT_PID);
    
    pid_table[root_pid] = static_cast<BWNode *>(root_ptr); 
 
  }

public:
  /* Constructors and Deconstructors. */
  BWTree() {
    BWTreeIntialize(); 
  }

};

}  // End index namespace
}  // End peloton namespace
