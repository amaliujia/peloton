//
// Created by Lu Zhang on 2/18/16.
//

#ifndef PELOTON_GARBAGE_COLLECTOR_H
#define PELOTON_GARBAGE_COLLECTOR_H

#include <chrono>
#include <forward_list>
#include <backend/expression/expressions.h>
#include "bwtree2.h"

namespace peloton {
  namespace index {


#define EPOCH 10 // unit : ms



/*------------------------------------------------------------------
 *                    Begin of class Garbage Collector
 *-----------------------------------------------------------------*/
// chrono library in c++ 11
// http://en.cppreference.com/w/cpp/chrono

  class GarbageCollector {
    typedef size_t EpochId;
    typedef std::chrono::high_resolution_clock Clock;
    typedef std::chrono::milliseconds milliseconds;
    typedef BWTree::BWNode BWNode;

  private:

    std::forward_list<EpochBlock> epoch_list_;
    Clock::time_point base_time_;

  public:

    GarbageCollector(){
      EpochBlock * head = new EpochBlock(1); // assign 1 as initial value, 0 as invalid value.
      epoch_list_.emplace_front(&head);
      base_time_ = Clock::now();

    }

    ~GarbageCollector(){
      // delete all the epochBlock
      for (auto iter = epoch_list_.begin(); iter != epoch_list_.end(); ++iter){
        // is that right?
        delete(iter);
      }
      epoch_list_.clear();
    }



    EpochId Register(){
      // For simplicity, always register in the "head".
      EpochBlock head = epoch_list_.front();
      // ATOMIC()
      EpochId epochId = head.epochId_;
      head.Register();
      // UNATOMIC()
    };


    void Deregister(EpochId epochId, BWNode * garbage){
      auto iter = epoch_list_.begin();
      for (;iter != epoch_list_.end(); ++iter){
        if (iter->epochId_ == epochId)
          break;
      }
      // Shouldn't happen: can't find that epochBlock.
      assert(iter!= epoch_list_.end());

      iter->Deregister();

    }

    void SubmitGarbage(EpochId epochId, BWNode *garbage){
      auto iter = epoch_list_.begin();
      for (;iter != epoch_list_.end(); ++iter){
        if (iter->epochId_ == epochId)
          break;
      }
      assert(iter!= epoch_list_.end());
      iter->CollectGarbage(garbage);
    }

    void Start();

    // periodically add epoch block
    // no need to concurrent?
    // b.c. only one thread do this.
    void AddEpoch(){
      Clock::time_point curt_time = Clock::now();
      milliseconds duration_ms = std::chrono::duration_cast<milliseconds>(curt_time - base_time_);
      EpochId expected_id = (EpochId)(duration_ms.count() / EPOCH);


      // ATOMIC()
      EpochBlock head = epoch_list_.front();
      if (head.epochId_ != expected_id ){
//        if (head.everUsed){
          EpochBlock * new_head = new EpochBlock(expected_id);
          epoch_list_.emplace_front(&new_head);

          // remove outdated epochBlock in the epochList
          // until meet a certain epochBlock is not empty.
          auto iter = epoch_list_.begin();
          auto prev = iter;
          ++iter;
          for (; iter != epoch_list_.end() && iter->thread_cnt_ == 0; ){
            // do remove outdated epochBlock
            RemoveEpoch(*iter);
            iter++;
            epoch_list_.erase_after(prev);
          }

      }
      // UNATOMIC()

    }

    void RemoveEpoch(EpochBlock block_to_remove){

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

      void CollectGarbage(BWTree::BWNode * garbage){
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


  }
}
#endif //PELOTON_GARBAGE_COLLECTOR_H
