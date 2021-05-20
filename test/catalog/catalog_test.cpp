//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);

  EXPECT_NE(nullptr, catalog->GetTable(table_metadata->name_));
  EXPECT_NE(nullptr, catalog->GetTable(table_metadata->oid_));

  std::string index_name = "potato_index";
  std::vector<Column> key_columns;
  key_columns.emplace_back("A", TypeId::INTEGER);
  Schema key_schema(key_columns);
  std::vector<uint32_t> key_attrs{0};
  IndexInfo *indexInfo = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      nullptr, index_name, table_name, schema, key_schema, key_attrs, static_cast<size_t>(4));

  EXPECT_NE(nullptr, catalog->GetIndex(indexInfo->index_oid_));
  EXPECT_NE(nullptr, catalog->GetIndex(indexInfo->name_, indexInfo->table_name_));

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
