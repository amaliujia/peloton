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

#include <chrono>
namespace peloton {
namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
  template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class BWTree {
  typedef size_t size_type;
  typedef size_t PID;
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




// chrono library in c++ 11
// http://en.cppreference.com/w/cpp/chrono

  class GarbageCollector {
    typedef size_t EpochId;
  public:

    std::forward_list<EpochBlock> epochList;
    EpochBlock * head;
    std::chrono::milliseconds ms = std::chrono::duration_cast< std::chrono::milliseconds >(
      std::chrono::system_clock::now().time_since_epoch()
    );

    GarbageCollector(){
      head = new EpochBlock();

    }
    ~GarbageCollector(){
      // delete all the epochBlock
      for (auto iter = epochList.begin(); iter != epochList.end(); ++iter){
        delete(*iter);
      }
      head = NULL;
    }



    EpochId Register();
    void Deregister(EpochId id);
    void SubmitGarbage(BWNode *node);
    void start();

    void AddEpoch(){

    }
    void RemoveEpoch();
    void ThrowGarbage();

    class EpochBlock {

      EpochId epochId;
    };
  };

};

}  // End index namespace
}  // End peloton namespace
