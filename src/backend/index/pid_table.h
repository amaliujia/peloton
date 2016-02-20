//
// Created by wendongli on 2/18/16.
//

#pragma once

#include <cassert>
#include <atomic>
#include <limits>

#include <boost/lockfree/stack.hpp>

#include "bwtree.h"

namespace peloton {
  namespace index {
    class PIDTable {
      /*
       * class storing the mapping table between a PID and its corresponding address
       * the mapping table is organized as a two-level array, like a virtual memory table.
       * the first level table is statically allocated, second level tables are allocated as needed
       * reclaimed PIDs are stored in a stack which will be given out first upon new allocations.
       * to achieve both latch-free and simple of implementation, this table can only reclaim PIDs but not space.
       */
      typedef std::uint_fast32_t PID;
      typedef std::atomic<PID> CounterType;
      typedef BWNode *Address;
      static constexpr unsigned int first_level_bits = 14;
      static constexpr unsigned int second_level_bits = 10;
      static constexpr PID first_level_mask = 0xFFFC00;
      static constexpr PID second_level_mask = 0x3FF;
      static constexpr unsigned int first_level_slots = 1<<first_level_bits;
      static constexpr unsigned int second_level_slots = 1<<second_level_bits;
      // singleton
      static PIDTable global_table_;
    public:
      // NULL for PID
      static constexpr PID PID_NULL = std::numeric_limits<PID>::max();

      static inline PIDTable& get_table() { return global_table_; }

      // get the address corresponding to the pid
      inline Address get(PID pid) const {
        return first_level_table_
        [(pid&first_level_mask)>>second_level_bits]
        [pid&second_level_mask];
      }

      // allocate a new PID
      inline PID allocate_PID() {
        PID result;
        if(free_PIDs.pop(result))
          return result;
        return allocate_new_PID();
      }

      // free up a new PID
      inline void free_PID(PID pid) {
        free_PIDs.push(pid);
        // bool push_result = freePIDs_.push(pid);
        // assert(push_result);
      }

      // atomic operation
      bool bool_compare_and_swap(PID pid, Address original, Address to) {
        int row = (int)((pid&first_level_mask)>>second_level_bits);
        int col = (int)(pid&second_level_mask);
        return __sync_bool_compare_and_swap(
                &first_level_table_[row][col], original, to);
      }

    private:
      volatile Address *first_level_table_[first_level_slots] = {nullptr};
      CounterType counter_;
      boost::lockfree::stack <PID> free_PIDs;

      PIDTable(): counter_(0) {
        first_level_table_[0] = (Address* )malloc(sizeof(Address)*second_level_slots);
      }

      PIDTable(const PIDTable &) = delete;

      PIDTable(PIDTable &&) = delete;

      PIDTable &operator=(const PIDTable &) = delete;

      PIDTable &operator=(PIDTable &&) = delete;

      inline bool is_first_PID(PID pid) {
        return (pid&second_level_mask)==0;
      }

      inline PID allocate_new_PID() {
        PID pid = counter_++;
        if(is_first_PID(pid)) {
          Address *table = (Address* )malloc(sizeof(Address)*second_level_slots);
          first_level_table_[((pid&first_level_mask)>>second_level_bits)+1] = table;
        }
        return pid;
      }
    };
  }
}