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


#define EPOCH 10 // unit : ms

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





/*------------------------------------------------------------------
 *                    Begin of class Garbage Collector
 *-----------------------------------------------------------------*/
// chrono library in c++ 11
// http://en.cppreference.com/w/cpp/chrono

  class GarbageCollector {
    typedef size_t EpochId;
    typedef std::chrono::high_resolution_clock Clock;
    typedef std::chrono::milliseconds milliseconds;
  public:

    std::forward_list<EpochBlock> epochList;
//    EpochBlock * head;

    Clock::time_point base_time;

    GarbageCollector(){
      EpochBlock * head = new EpochBlock(1); // assign 1 as initial value, 0 as invalid value.
      epochList.emplace_front(&head);
      base_time = Clock::now();

    }

    ~GarbageCollector(){
      // delete all the epochBlock
      for (auto iter = epochList.begin(); iter != epochList.end(); ++iter){
        // is that right?
        delete(iter);
      }
      epochList.clear();
      head = NULL;
    }



    EpochId Register(){
      // For simplicity, always register in the "head".
      EpochBlock head = epochList.front();
      // ATOMIC()
      EpochId epochId = head.epochId_;
      head.Register();
      // UNATOMIC()
    };


    void Deregister(EpochId epochId, BWNode * garbage){
      auto iter = epochList.begin();
      for (;iter != epochList.end(); ++iter){
        if (iter->get_epochId() == id)
          break;
      }
      // Shouldn't happen: can't find that epochBlock.
      assert(iter!= epochList.end());

      iter->Deregister();

    }

    void SubmitGarbage(EpochId epochId, BWNode *garbage){
      auto iter = epochList.begin();
      for (;iter != epochList.end(); ++iter){
        if (iter->get_epochId() == id)
          break;
      }
      assert(iter!= epochList.end());
      iter->CollectGarbage(garbage);
    }

    void Start();

    // periodically add epoch block
    // no need to concurrent?
    // b.c. only one thread do this.
    void AddEpoch(){
      Clock::time_point curt_time = Clock::now();
      milliseconds duration_ms = std::chrono::duration_cast<milliseconds>(curt_time - base_time);
      EpochId expected_id = (EpochId)(duration_ms.count() / EPOCH);


      // ATOMIC()
      EpochBlock head = epochList.front();
      if (head.epochId_ != expected_id ){
//        if (head.everUsed){
          EpochBlock * new_head = new EpochBlock(expected_id);
          epochList.emplace_front(&new_head);

          while (true){
           break;

          }
          auto iter = epochList.begin();
          auto prev = iter;
          ++iter;
          for (; iter != epochList.end() && iter->thread_cnt_ == 0; ++iter, ++prev){
            // do remove outdated epochBlock
            RemoveEpoch(*iter);
            epochList.erase_after(prev);
            epochList.remove(iter);
          }

          if (iter != epochList.end()){ // do remove outdated epochList

          }


//        } else {
          // how to judge if the old head has something now?
          //?????????????


//        }
      }
      // UNATOMIC()

    }

    void RemoveEpoch(EpochBlock * block_to_remove){

    };

    void ThrowGarbage(){

    };

/*------------------------------------------------------------------
 *                    Begin of class EpochBLock
 *-----------------------------------------------------------------*/

    class EpochBlock {

      public:

      const EpochId epochId_;
      std::atomic<bool> everUsed = ATOMIC_VAR_INIT(false);

      std::forward_list< BWNode *> garbageList;
      std::atomic<int>  thread_cnt_ = ATOMIC_VAR_INIT(0);

      EpochBlock(EpochId epochId): epochId_(epochId) {};


      // combine the deconstructor and throwGarbage?
      // The deamon thread can just change the pointer, and deconstruct this epochblock
      ~EpochBlock(){
        ThrowGarbage();
      }

      void Register(){
        std::atomic_fetch_add(&thread_cnt_, 1);

        // if this operation fails, then everUsed must be true.
        std::atomic_compare_exchange_strong(&everUsed, false, true);
      };

      void Deregister(){
        std::atomic_fetch_sub(&thread_cnt_, 1);
      }

      void CollectGarbage(BWNode * garbage){
        // along the lists, add all these into the garbageList
      }

      // A deamon thread periodically lu the list
      // once it finds a outdated epoch block, throw it  away.
      // Note: the deamon should call ~EpochBlock
      void ThrowGarbage(){
        for (auto iter = garbageList.begin(); iter != garbageList.end(); ++iter){
          delete(iter);
        }
        garbageList.clear();
      }



    };
  };

};

}  // End index namespace
}  // End peloton namespace
