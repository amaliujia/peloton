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
#include <chrono>

namespace peloton {
  namespace index {


#define EPOCH 10 // unit : ms
    typedef size_t size_type;
    typedef size_t PID;
    constexpr int max_chain_len = 8;
    constexpr int max_node_size = 20;
    constexpr int min_node_size = max_node_size << 1;
    enum NodeType {

    };

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    class BWTree {

      KeyComparator comparator;
      KeyEqualityChecker keyEqualityChecker;


      public :
        BWTree(IndexMetadata * indexMetadata): comparator(indexMetadata), keyEqualityChecker(indexMetadata){

        }
    };

    class BWNode {
      size_type slot_usage;
      size_type chain_length;
    };

    template<typename KeyType>
    class BWNormalNode : public BWNode {
      KeyType low_key, high_key;
      PID left, right;
    };

    template<typename KeyType>
    class BWInnerNode : public BWNormalNode<KeyType>{
      KeyType keys[max_node_size + max_chain_len];
      PID children[max_node_size + max_chain_len + 1];
    };

    template<typename KeyType, typename ValueType>
    class BWLeafNode : public BWNormalNode<KeyType> {
      KeyType keys[max_node_size + max_chain_len];
      ValueType values[max_node_size + max_chain_len];
    };

//    template<typename KeyType>
    // Add your declarations here
    class BWDeltaNode : public BWNode {
      BWNode *next;
    };

    template<typename KeyType, typename ValueType>
    class BWInsertNode : public BWDeltaNode {
      KeyType key;
      ValueType value;
    };

    template<typename KeyType>
    class BWDeleteNode : public BWDeltaNode {
      KeyType key;
    };

    template<typename KeyType>
    class BWSplitNode : public BWDeltaNode {
      KeyType key;
      PID right;
    };

    template<typename KeyType>
    class BWSplitEntryNode : public BWDeltaNode {
      KeyType low_key, high_key;
      PID to;
    };

    template<typename KeyType>
    class BWRemoveNode : public BWDeltaNode {

    };

    template<typename KeyType>
    class BWMergeNode : public BWDeltaNode {
      BWNode *right;
    };

    template<typename KeyType>
    class MergeEntryNode : public BWDeltaNode {

    };


  }  // End index namespace
}  // End peloton namespace
