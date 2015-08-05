//===----------------------------------------------------------------------===//
//
//							PelotonDB
//
// tile_group_header.h
//
// Identification: src/backend/storage/tile_group_header.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "backend/storage/abstract_backend.h"
#include "backend/common/logger.h"

#include <atomic>
#include <mutex>
#include <iostream>
#include <cassert>
#include <queue>
#include <cstring>

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group Header
//===--------------------------------------------------------------------===//

/**
 *
 * This contains information related to MVCC.
 * It is shared by all tiles in a tile group.
 *
 * Layout :
 *
 * 	--------------------------------------------------------------------------------------------------------
 *  | Txn ID (8 bytes)  | Begin TimeStamp (8 bytes) | End TimeStamp (8 bytes) |
 *Prev ItemPointer (4 bytes) |
 * 	--------------------------------------------------------------------------------------------------------
 *
 */

class TileGroupHeader {
  TileGroupHeader() = delete;

 public:
  TileGroupHeader(AbstractBackend *_backend, int tuple_count)
 : backend(_backend),
   data(nullptr),
   num_tuple_slots(tuple_count),
   next_tuple_slot(0),
   active_tuple_slots(0) {
    header_size = num_tuple_slots * header_entry_size;

    // allocate storage space for header
    data = (char *)backend->Allocate(header_size);
    assert(data != nullptr);
  }

  TileGroupHeader& operator=(const peloton::storage::TileGroupHeader &other) {
    // check for self-assignment
    if(&other == this)
      return *this;

    backend = other.backend;
    header_size = other.header_size;

    // copy over all the data
    memcpy(data, other.data, header_size);

    num_tuple_slots = other.num_tuple_slots;
    next_tuple_slot = other.next_tuple_slot;
    empty_slots = other.empty_slots;

    oid_t val = other.active_tuple_slots;
    active_tuple_slots = val;

    return *this;
  }


  ~TileGroupHeader() {
    // reclaim the space
    backend->Free(data);
    data = nullptr;
  }

  oid_t GetNextEmptyTupleSlot() {
    oid_t tuple_slot_id = INVALID_OID;

    {
      std::lock_guard<std::mutex> tile_header_lock(tile_header_mutex);

      //  first, check free slots
      if (empty_slots.empty() == false) {
        tuple_slot_id = empty_slots.front();
        empty_slots.pop();
      }
      // check tile group capacity
      else if (next_tuple_slot < num_tuple_slots) {
        tuple_slot_id = next_tuple_slot;
        next_tuple_slot++;
      }
    }

    return tuple_slot_id;
  }

  void ReclaimTupleSlot(oid_t tuple_slot_id) {
    {
      std::lock_guard<std::mutex> tile_header_lock(tile_header_mutex);
      empty_slots.push(tuple_slot_id);
    }
  }

  oid_t GetNextTupleSlot() const { return next_tuple_slot; }

  inline oid_t GetActiveTupleCount() const { return active_tuple_slots; }

  inline void IncrementActiveTupleCount() { ++active_tuple_slots; }

  inline void DecrementActiveTupleCount() { --active_tuple_slots; }

  //===--------------------------------------------------------------------===//
  // MVCC utilities
  //===--------------------------------------------------------------------===//

  // Getters

  inline txn_id_t GetTransactionId(const oid_t tuple_slot_id) const {
    return *((txn_id_t *)(data + (tuple_slot_id * header_entry_size)));
  }

  inline cid_t GetBeginCommitId(const oid_t tuple_slot_id) const {
    return *((cid_t *)(data + (tuple_slot_id * header_entry_size) +
        sizeof(txn_id_t)));
  }

  inline cid_t GetEndCommitId(const oid_t tuple_slot_id) const {
    return *((cid_t *)(data + (tuple_slot_id * header_entry_size) +
        sizeof(txn_id_t) + sizeof(cid_t)));
  }

  inline ItemPointer GetPrevItemPointer(const oid_t tuple_slot_id) const {
    return *((ItemPointer *)(data + (tuple_slot_id * header_entry_size) +
        sizeof(txn_id_t) + 2 * sizeof(cid_t)));
  }

  // Getters for addresses

  inline txn_id_t *GetTransactionIdLocation(const oid_t tuple_slot_id) const {
    return ((txn_id_t *)(data + (tuple_slot_id * header_entry_size)));
  }

  inline cid_t *GetBeginCommitIdLocation(const oid_t tuple_slot_id) const {
    return ((cid_t *)(data + (tuple_slot_id * header_entry_size) +
        sizeof(txn_id_t)));
  }

  inline cid_t *GetEndCommitIdLocation(const oid_t tuple_slot_id) const {
    return ((cid_t *)(data + (tuple_slot_id * header_entry_size) +
        sizeof(txn_id_t) + sizeof(cid_t)));
  }

  // Setters

  inline void SetTransactionId(const oid_t tuple_slot_id,
                               txn_id_t transaction_id) {
    *((txn_id_t *)(data + (tuple_slot_id * header_entry_size))) =
        transaction_id;
  }

  inline void SetBeginCommitId(const oid_t tuple_slot_id, cid_t begin_cid) {
    *((cid_t *)(data + (tuple_slot_id * header_entry_size) +
        sizeof(txn_id_t))) = begin_cid;
  }

  inline void SetEndCommitId(const oid_t tuple_slot_id, cid_t end_cid) const {
    *((cid_t *)(data + (tuple_slot_id * header_entry_size) + sizeof(txn_id_t) +
        sizeof(cid_t))) = end_cid;
  }

  inline void SetPrevItemPointer(const oid_t tuple_slot_id,
                                 ItemPointer item) const {
    *((ItemPointer *)(data + (tuple_slot_id * header_entry_size) +
        sizeof(txn_id_t) + 2 * sizeof(cid_t))) = item;
  }

  // Visibility check

  bool IsVisible(const oid_t tuple_slot_id, txn_id_t txn_id, cid_t at_cid) {
    txn_id_t tuple_txn_id = GetTransactionId(tuple_slot_id);
    cid_t tuple_begin_cid = GetBeginCommitId(tuple_slot_id);
    cid_t tuple_end_cid  = GetEndCommitId(tuple_slot_id);

    bool own = (txn_id == tuple_txn_id);
    bool activated = (at_cid >= tuple_begin_cid);
    bool invalidated = (at_cid >= tuple_end_cid);

    LOG_TRACE("Own :: %d txn id : %lu tuple txn id : %lu", own, txn_id, tuple_txn_id);
    LOG_TRACE("Activated :: %d cid : %lu tuple begin cid : %lu", activated, at_cid, tuple_begin_cid);
    LOG_TRACE("Invalidated:: %d cid : %lu tuple end cid : %lu", invalidated, at_cid, tuple_end_cid);

    // Visible iff past Insert || Own Insert
    if ((!own && activated && !invalidated) ||
        (own && !activated && !invalidated))
      return true;

    return false;
  }

  void PrintVisibility(txn_id_t txn_id, cid_t at_cid);

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation of this tile
  friend std::ostream &operator<<(std::ostream &os,
                                  const TileGroupHeader &tile_group_header);

 private:
  // header entry size is the size of the layout described above
  static const size_t header_entry_size =
      sizeof(txn_id_t) + 2 * sizeof(cid_t) + sizeof(ItemPointer);

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // storage backend
  AbstractBackend *backend;

  size_t header_size;

  // set of fixed-length tuple slots
  char *data;

  // number of tuple slots allocated
  oid_t num_tuple_slots;

  // next free tuple slot
  oid_t next_tuple_slot;

  // active tuples
  std::atomic<oid_t> active_tuple_slots;

  // free slots
  std::queue<oid_t> empty_slots;

  // synch helpers
  std::mutex tile_header_mutex;
};

}  // End storage namespace
}  // End peloton namespace
