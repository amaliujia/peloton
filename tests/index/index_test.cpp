/*-------------------------------------------------------------------------
 *
 * index_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/index/index_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"
#include "harness.h"

#include "index/index_factory.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tests
//===--------------------------------------------------------------------===//

TEST(IndexTests, BtreeMultimapIndexTest) {

  std::vector<catalog::ColumnInfo> columns;
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string> > column_names;
  std::vector<catalog::Schema*> schemas;

  // SCHEMA

  catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
  catalog::ColumnInfo column2(VALUE_TYPE_VARCHAR, 25, false, true);
  catalog::ColumnInfo column3(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);
  catalog::ColumnInfo column4(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), false, true);

  columns.push_back(column1);
  columns.push_back(column2);

  catalog::Schema *schema1 = new catalog::Schema(columns);
  schemas.push_back(schema1);

  columns.clear();
  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema *schema2 = new catalog::Schema(columns);
  schemas.push_back(schema2);

  catalog::Schema *schema = catalog::Schema::AppendSchema(schema1, schema2);

  // TILES

  tile_column_names.push_back("COL 1");
  tile_column_names.push_back("COL 2");

  column_names.push_back(tile_column_names);

  tile_column_names.clear();
  tile_column_names.push_back("COL 3");
  tile_column_names.push_back("COL 4");

  column_names.push_back(tile_column_names);

  // TILE GROUP

  catalog::Catalog *catalog = new catalog::Catalog();

  storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(schemas, 4, column_names, true, catalog);

  storage::Tile *tile0 = tile_group->GetTile(0);

  // TUPLES

  storage::Tuple *tuple0 = new storage::Tuple(schema, true);
  storage::Tuple *tuple1 = new storage::Tuple(schema, true);
  storage::Tuple *tuple2 = new storage::Tuple(schema, true);

  for(id_t column_itr = 0; column_itr < schema->GetColumnCount(); column_itr++) {
    if(column_itr != 1) {
      tuple0->SetValue(column_itr, ValueFactory::GetIntegerValue(100 * (column_itr + 1)));
      tuple1->SetValue(column_itr, ValueFactory::GetIntegerValue(200 * (column_itr + 1)));
      tuple2->SetValue(column_itr, ValueFactory::GetIntegerValue(300 * (column_itr + 1)));
    }
    else {
      tuple0->SetValue(column_itr, ValueFactory::GetStringValue("a", tile0->GetPool()));
      tuple1->SetValue(column_itr, ValueFactory::GetStringValue("b", tile0->GetPool()));
      tuple2->SetValue(column_itr, ValueFactory::GetStringValue("c", tile0->GetPool()));
    }
  }

  // TRANSACTION

  txn_id_t txn_id = GetTransactionId();

  tile_group->InsertTuple(txn_id, tuple0);
  tile_group->InsertTuple(txn_id, tuple1);
  tile_group->InsertTuple(txn_id, tuple2);

  EXPECT_EQ(3, tile_group->GetActiveTupleCount());

  std::cout << (*tile_group);

  // BTREE MULTIMAP INDEX

  std::vector<id_t> table_columns_in_key;

  // SET UP KEY

  table_columns_in_key.push_back(0);
  table_columns_in_key.push_back(1);

  index::IndexMetadata index_metadata("btree_index",
                                      INDEX_TYPE_BTREE_MULTIMAP,
                                      catalog, schema,
                                      table_columns_in_key,
                                      true);

  catalog::Schema *key_schema = index_metadata.key_schema;

  index::Index *index = index::IndexFactory::GetInstance(index_metadata);

  EXPECT_EQ(true, index != NULL);

  // INDEX

  id_t tile_id = tile_group->GetTileId(0);

  storage::Tuple *key0 = new storage::Tuple(key_schema, true);
  storage::Tuple *key1 = new storage::Tuple(key_schema, true);
  storage::Tuple *key2 = new storage::Tuple(key_schema, true);
  storage::Tuple *key3 = new storage::Tuple(key_schema, true);
  storage::Tuple *key4 = new storage::Tuple(key_schema, true);
  storage::Tuple *keynonce = new storage::Tuple(key_schema, true);

  key0->SetValue(0, ValueFactory::GetIntegerValue(100));
  key1->SetValue(0, ValueFactory::GetIntegerValue(100));
  key2->SetValue(0, ValueFactory::GetIntegerValue(100));
  key3->SetValue(0, ValueFactory::GetIntegerValue(400));
  key4->SetValue(0, ValueFactory::GetIntegerValue(500));
  keynonce->SetValue(0, ValueFactory::GetIntegerValue(1000));

  key0->SetValue(1, ValueFactory::GetStringValue("a", tile0->GetPool()));
  key1->SetValue(1, ValueFactory::GetStringValue("b", tile0->GetPool()));
  key2->SetValue(1, ValueFactory::GetStringValue("c", tile0->GetPool()));
  key3->SetValue(1, ValueFactory::GetStringValue("d", tile0->GetPool()));
  key4->SetValue(1, ValueFactory::GetStringValue("e", tile0->GetPool()));
  keynonce->SetValue(1, ValueFactory::GetStringValue("f", tile0->GetPool()));

  ItemPointer item0(tile_id, 0);
  ItemPointer item1(tile_id, 1);
  ItemPointer item2(tile_id, 2);

  index->InsertEntry(key0, item0);
  index->InsertEntry(key1, item1);
  index->InsertEntry(key1, item2);
  index->InsertEntry(key2, item1);
  index->InsertEntry(key3, item1);
  index->InsertEntry(key4, item1);

  EXPECT_EQ(false, index->Exists(keynonce));
  EXPECT_EQ(true, index->Exists(key0));

  index->GetLocationsForKey(key1);
  index->GetLocationsForKey(key0);

  index->GetLocationsForKeyBetween(key1, key3);

  index->GetLocationsForKeyLT(key3);
  index->GetLocationsForKeyLTE(key3);

  index->GetLocationsForKeyGT(key1);
  index->GetLocationsForKeyGTE(key1);

  index->Scan();

  index->DeleteEntry(key0);
  index->DeleteEntry(key1);
  index->DeleteEntry(key2);
  index->DeleteEntry(key3);
  index->DeleteEntry(key4);

  EXPECT_EQ(false, index->Exists(key0));

  index->Scan();

  delete key0;
  delete key1;
  delete key2;
  delete key3;
  delete key4;
  delete keynonce;

  delete tuple0;
  delete tuple1;
  delete tuple2;

  delete index;
  delete tile_group;

  delete schema;
  delete catalog;
}


} // End test namespace
} // End nstore namespace



