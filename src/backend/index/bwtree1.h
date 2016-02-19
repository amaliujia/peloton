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

#include <cassert>
#include <atomic>
#include <limits>

#include <boost/lockfree/stack.hpp>

namespace peloton {
  namespace index {
    typedef size_t SizeType;

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    class BWTree {
      KeyComparator comparator;
      KeyEqualityChecker
      enum NodeType {

      };
      static constexpr int max_chain_len = 8;
      static constexpr int max_node_size = 20;
      static constexpr int min_node_size = max_node_size<<1;

      class BWNode {
        size_type slot_usage;
        size_type chain_length;
      };

      class BWNormalNode: public BWNode {
        KeyType low_key, high_key;
        PID left, right;
      };

      class BWInnerNode: public BWNormalNode {
        KeyType keys[max_node_size+max_chain_len];
        PID children[max_node_size+max_chain_len+1];
      };

      class BWLeafNode: public BWNormalNode {
        KeyType keys[max_node_size+max_chain_len];
        ValueType values[max_node_size+max_chain_len];
      };

      // Add your declarations here
      class BWDeltaNode: public BWNode {
        BWNode *next;
      };

      class BWInsertNode: public BWDeltaNode {
        KeyType key;
        ValueType value;
      };

      class BWDeleteNode: public BWDeltaNode {
        KeyType key;
      };

      class BWSplitNode: public BWDeltaNode {
        KeyType key;
        PID right;
      };

      class BWSplitEntryNode: public BWDeltaNode {
        KeyType low_key, high_key;
        PID to;
      };

      class BWRemoveNode: public BWDeltaNode {

      };

      class BWMergeNode: public BWDeltaNode {
        BWNode *right;
      };

      class MergeEntryNode: public BWDeltaNode {

      };

      class GarbageCollector {
        typedef size_t EpochId;
      public:
        EpochId Register();

        void Deregister(EpochId id);

        void SubmitGarbage(BWNode *node);

        void start();
      };
    };

  }  // End index namespace
}  // End peloton namespace
