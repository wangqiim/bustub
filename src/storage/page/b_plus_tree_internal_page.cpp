//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetSize(0);
  this->SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < this->GetSize());
  return this->array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < this->GetSize());
  this->array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int index = 0; index < this->GetSize(); index++) {
    if (value == this->array[index].second) {
      return index;
    }
  }
  assert(false);
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index >= 0 && index < this->GetSize());
  return this->array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // find the largest index that (this->array[index].first) <= key
  int le = 0;
  int ri = this->GetSize() - 1;
  while (le < ri) {
    int mid = (le + ri + 1) / 2;
    if (comparator(this->array[mid].first, key) > 0) {
      ri = mid - 1;
    } else {
      le = mid;
    }
  }
  return this->array[le].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  this->array[0].second = old_value;
  this->array[1].first = new_key;
  this->array[1].second = new_value;
  this->SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int insertIndex = this->ValueIndex(old_value) + 1;
  this->IncreaseSize(1);
  for (int i = this->GetSize() - 1; i > insertIndex; i--) {
    this->array[i].first = this->array[i - 1].first;
    this->array[i].second = this->array[i - 1].second;
  }
  this->array[insertIndex].first = new_key;
  this->array[insertIndex].second = new_value;
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  page_id_t recipientPage = recipient->GetPageId();
  // this->GetMaxSize() = 6, this.GetSize() = 7
  // x 1 2 3 4 5 6 = > x 1 2 3 | x 5 6
  // this->GetMaxSize() = 7, this.GetSize() = 8
  // x 1 2 3 4 5 6 7 = > x 1 2 3 | x 5 6 7
  int total = this->GetMaxSize() + 1;
  int left = total / 2;
  for (int i = left; i < total; i++) {
    recipient->array[i - left].first = this->array[i].first;
    recipient->array[i - left].second = this->array[i].second;
    // update child page node's parent
    page_id_t movePageId = recipient->array[i - left].second;
    B_PLUS_TREE_INTERNAL_PAGE_TYPE *childPage =
        reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(buffer_pool_manager->FetchPage(movePageId));
    assert(childPage != nullptr);
    childPage->SetParentPageId(recipientPage);
    buffer_pool_manager->UnpinPage(movePageId, true);
  }
  this->SetSize(left);
  recipient->SetSize(total - left);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < this->GetSize() - 1; i++) {
    this->array[i] = this->array[i + 1];
  }
  this->IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType value = ValueAt(0);
  IncreaseSize(-1);
  return value;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // 1. set index 0 key
  this->array[0].first = middle_key;
  int startIndex = recipient->GetSize();
  // 2. move all to recipient
  for (int i = 0; i < this->GetSize(); i++) {
    recipient->array[i + startIndex].first = this->array[i].first;
    recipient->array[i + startIndex].second = this->array[i].second;
    // 3. update child's parent
    page_id_t childID = this->array[i].second;
    BPlusTreePage *childPage = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(childID));
    assert(childPage != nullptr);
    childPage->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(childID, true);
  }
  recipient->IncreaseSize(this->GetSize());
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // 1. move this's first to recipient's end
  recipient->array[recipient->GetSize()].first = middle_key;
  recipient->array[recipient->GetSize()].second = this->array[0].second;
  recipient->IncreaseSize(1);
  // 2. update child's parent
  page_id_t childID = this->array[0].second;
  BPlusTreePage *childPage = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(childID));
  assert(childPage != nullptr);
  childPage->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(childID, true);
  // 3. adjust this->array
  for (int i = 0; i < this->GetSize() - 1; i++) {
    this->array[i].first = this->array[i + 1].first;
    this->array[i].second = this->array[i + 1].second;
  }
  this->IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  this->array[this->GetSize()] = pair;
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  // 1. adjust recipient->array
  recipient->array[0].first = middle_key;
  for (int i = recipient->GetSize(); i >= 1; i--) {
    recipient->array[i].first = recipient->array[i - 1].first;
    recipient->array[i].second = recipient->array[i - 1].second;
  }
  // 2. move this's last to recipient's first
  recipient->array[0].first = this->array[this->GetSize() - 1].first;
  recipient->array[0].second = this->array[this->GetSize() - 1].second;
  recipient->IncreaseSize(1);
  // 3. update child's parent
  page_id_t childID = this->array[this->GetSize() - 1].second;
  BPlusTreePage *childPage = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(childID));
  assert(childPage != nullptr);
  childPage->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(childID, true);
  this->IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
