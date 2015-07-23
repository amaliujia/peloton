/*-------------------------------------------------------------------------
 *
l* database.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/storage/data_table.h"

#include <iostream>

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// DATABASE
//===--------------------------------------------------------------------===//

class Database{

 public:
  Database(Database const&) = delete;

  Database(oid_t database_oid)
  : database_oid(database_oid) { }

  ~Database();

  //===--------------------------------------------------------------------===//
  // OPERATIONS
  //===--------------------------------------------------------------------===//

  oid_t GetOid() const {
    return database_oid;
  }

  //===--------------------------------------------------------------------===//
  // TABLE
  //===--------------------------------------------------------------------===//

  void AddTable(storage::DataTable *table);

  storage::DataTable *GetTable(const oid_t table_offset) const;

  storage::DataTable *GetTableWithOid(const oid_t table_oid) const;

  storage::DataTable *GetTableWithName(const std::string table_name) const;

  oid_t GetTableCount() const;

  void DropTableWithOid(const oid_t table_oid);

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  void UpdateStats();

  void UpdateStatsWithOid(const oid_t table_oid);

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  // Get a string representation of this database
  friend std::ostream& operator<<(std::ostream& os, const Database& database);

 protected:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // database oid
  oid_t database_oid;

  // TABLES

  std::vector<storage::DataTable*> tables;

  std::mutex database_mutex;

};


} // End storage namespace
} // End peloton namespace


