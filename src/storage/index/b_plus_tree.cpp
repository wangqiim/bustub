//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
thread_local int BPLUSTREE_TYPE::rootLockedCnt = 0;

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::clearLockedPages(LockType lockType, Transaction *transaction) {
  // 0. find all locked page
  std::shared_ptr<std::deque<Page *>> pageSet = transaction->GetPageSet();
  std::shared_ptr<std::unordered_set<page_id_t>> deletedPageSet = transaction->GetDeletedPageSet();
  for (Page *page : *pageSet) {
    page_id_t pageId = page->GetPageId();
    this->unlock(page, lockType);
    this->buffer_pool_manager_->UnpinPage(pageId, true);
    // 1. if page needed to be deleted, delete it
    if (deletedPageSet->find(pageId) != deletedPageSet->end()) {
      this->buffer_pool_manager_->DeletePage(pageId);
    }
  }
  this->tryUnlockRoot(lockType);
  pageSet->clear();
  deletedPageSet->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::checkAndSolveSafe(OpType opType, Page *page, Transaction *transaction) {
  // 1. if is GET opration, clear clock directly
  if (opType == OpType::GET) {
    this->clearLockedPages(LockType::READ, transaction);
    return;
  }
  bool clearLock = false;
  // 2. check if leaf (leaf and internal check conditon are different < and <=)
  if (reinterpret_cast<BPlusTreePage *>(page)->IsLeafPage()) {
    LeafPage *leafPage = reinterpret_cast<LeafPage *>(page);
    if (opType == OpType::INSERT && leafPage->GetSize() + 1 < leafPage->GetMaxSize()) {
      clearLock = true;
    } else if (opType == OpType::REMOVE && leafPage->GetSize() - 1 >= leafPage->GetMinSize()) {
      clearLock = true;
    }
  } else {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(page);
    if (opType == OpType::INSERT && internalPage->GetSize() + 1 <= internalPage->GetSize()) {
      clearLock = true;
    } else if (opType == OpType::REMOVE && internalPage->GetSize() - 1 >= internalPage->GetMinSize()) {
      clearLock = true;
    }
  }
  if (clearLock) {
    this->clearLockedPages(LockType::WRITE, transaction);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return this->root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // 0. init result
  result->resize(1);
  // 1. find leaf
  this->lockRoot(LockType::READ);
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(getFindLeafPageWithLock(key, false));
  if (leafPage == nullptr) {
    return false;
  }
  // 2. lookup leaf
  bool isExist = leafPage->Lookup(key, &(*result)[0], comparator_);
  // 3. unpin leaf
  this->unlock(reinterpret_cast<Page *>(leafPage), LockType::READ);
  this->buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
  this->tryUnlockRoot(LockType::READ);
  return isExist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 0. if is empty StartNewTree
  this->lockRoot(LockType::WRITE);
  if (this->IsEmpty()) {
    this->StartNewTree(key, value);
    this->tryUnlockRoot(LockType::WRITE);
    return true;
  }
  // be very carefully remember unlockRoot
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 0. Get new page
  page_id_t newPageID;
  Page *newPage = this->buffer_pool_manager_->NewPage(&newPageID);
  if (newPage == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "can't new page from buffer pool manager");
    return;
  }
  LeafPage *root = reinterpret_cast<LeafPage *>(newPage);
  // 1. init this page
  // whether remain 1 place: first insert, then think about split?
  this->root_page_id_ = newPageID;
  root->Init(this->root_page_id_, bustub::INVALID_PAGE_ID, leaf_max_size_);
  // 2. update root page id
  this->UpdateRootPageId(true);
  // 3. insert pair in root page
  root->Insert(key, value, this->comparator_);
  // 4. unpin root page
  this->buffer_pool_manager_->UnpinPage(this->root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 0. get leaf
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(this->insertFindLeafPageWithLock(key, transaction));
  // 1. if key isExist return false
  bool isExist = leafPage->Lookup(key, &(const_cast<ValueType &>(value)), comparator_);
  if (isExist) {
    this->clearLockedPages(LockType::WRITE, transaction);
    return false;
  }
  // 2. else insert key value first
  leafPage->Insert(key, value, comparator_);
  // 3. think about to split
  if (leafPage->GetSize() == leafPage->GetMaxSize()) {
    LeafPage *leafPage2 = this->Split(leafPage, transaction);
    this->InsertIntoParent(leafPage, leafPage2->KeyAt(0), leafPage2, transaction);
  }
  this->clearLockedPages(LockType::WRITE, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
  // 0. new page
  page_id_t newPageID;
  Page *newPage;
  newPage = this->buffer_pool_manager_->NewPage(&newPageID);
  this->lock(newPage, LockType::WRITE);
  transaction->AddIntoPageSet(newPage);
  if (newPage == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "can't new page from buffer pool manager");
    return nullptr;
  }
  // 1. copy half from node to newNode
  N *newNode = reinterpret_cast<N *>(newPage);
  if (node->IsLeafPage()) {
    LeafPage *node1 = reinterpret_cast<LeafPage *>(node);
    LeafPage *node2 = reinterpret_cast<LeafPage *>(newNode);
    node2->Init(newPageID, node1->GetParentPageId(), this->leaf_max_size_);
    node1->MoveHalfTo(node2);
  } else {
    InternalPage *node1 = reinterpret_cast<InternalPage *>(node);
    InternalPage *node2 = reinterpret_cast<InternalPage *>(newNode);
    node2->Init(newPageID, node1->GetParentPageId(), this->internal_max_size_);
    node1->MoveHalfTo(node2, this->buffer_pool_manager_);
  }
  return newNode;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 1. create a new node containing N, K', N'
  if (old_node->IsRootPage()) {
    page_id_t newRootPageID;
    InternalPage *newRootPage = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->NewPage(&newRootPageID));
    if (newRootPage == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "can't new page from buffer pool manager");
      return;
    }
    this->lock(reinterpret_cast<Page *>(newRootPage), LockType::WRITE);
    transaction->AddIntoPageSet(reinterpret_cast<Page *>(newRootPage));
    newRootPage->Init(newRootPageID, bustub::INVALID_PAGE_ID, this->internal_max_size_);
    newRootPage->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(newRootPageID);
    new_node->SetParentPageId(newRootPageID);
    // 2. make R the root of tree
    this->root_page_id_ = newRootPageID;
    this->UpdateRootPageId(false);
    return;
  }
  // 3. get parent page
  page_id_t parentPageID = old_node->GetParentPageId();
  InternalPage *parentPage = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(parentPageID));
  // have pin in queue, so we can unpin deirectly, (because we have to call FetchPage for getting page)
  this->buffer_pool_manager_->UnpinPage(parentPageID, true);
  parentPage->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  new_node->SetParentPageId(parentPageID);
  // 4. if parent has less or equal than max_size
  if (parentPage->GetSize() <= parentPage->GetMaxSize()) {
    return;
  }
  // 5. else split p
  InternalPage *internalPage2 = this->Split(parentPage, transaction);
  this->InsertIntoParent(parentPage, internalPage2->KeyAt(0), internalPage2, transaction);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 0. if tree is empty, return
  this->lockRoot(LockType::WRITE);
  if (this->IsEmpty()) {
    this->tryUnlockRoot(LockType::WRITE);
    return;
  }
  // 1. else find the leaf, and key don't exist in leaf, return
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(this->removeFindLeafPageWithLock(key, transaction));
  assert(leafPage != nullptr);
  ValueType value;
  if (!leafPage->Lookup(key, &value, this->comparator_)) {
    this->clearLockedPages(LockType::WRITE, transaction);
    return;
  }
  // 3. delete key in leafpage
  this->delete_entry(leafPage, key, transaction);
  this->clearLockedPages(LockType::WRITE, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::delete_entry(N *node, const KeyType &key, Transaction *transaction) {
  // 0. delete(k) from node
  this->delete_key_in_node(node, key, transaction);
  // 1. if node is root
  BPlusTreePage *b_plus_tree_node = reinterpret_cast<BPlusTreePage *>(node);
  if (b_plus_tree_node->IsRootPage()) {
    page_id_t old_root_page_id = b_plus_tree_node->GetPageId();
    bool needDelete = this->AdjustRoot(b_plus_tree_node);
    if (needDelete) {
      transaction->AddIntoDeletedPageSet(old_root_page_id);
    }
    return;
  }
  // 2. if size < minsize
  if (b_plus_tree_node->GetSize() < b_plus_tree_node->GetMinSize()) {
    this->CoalesceOrRedistribute(b_plus_tree_node, transaction);
    return;
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::delete_key_in_node(N *node, const KeyType &key, Transaction *transaction) {
  if (reinterpret_cast<BPlusTreePage *>(node)->IsLeafPage()) {
    LeafPage *leafNode = reinterpret_cast<LeafPage *>(node);
    // std::cout << "before delete size = :" << leafNode->GetSize() << std::endl;
    leafNode->RemoveAndDeleteRecord(key, comparator_);
    // std::cout << "After delete size = :" << leafNode->GetSize() << std::endl;
  } else {
    InternalPage *internalNode = reinterpret_cast<InternalPage *>(node);
    page_id_t value = internalNode->Lookup(key, this->comparator_);
    int index = internalNode->ValueIndex(value);
    assert(this->comparator_(key, internalNode->KeyAt(index)) == 0);
    // std::cout << "before delete size = :" << internalNode->GetSize() << std::endl;
    internalNode->Remove(index);
    // std::cout << "After delete size = :" << internalNode->GetSize() << std::endl;
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (reinterpret_cast<BPlusTreePage *>(node)->IsLeafPage()) {
    // 1. node is leaf
    LeafPage *node0 = reinterpret_cast<LeafPage *>(node);
    page_id_t parentId = node0->GetParentPageId();
    InternalPage *parentNode = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(parentId));
    this->buffer_pool_manager_->UnpinPage(parentId, true);
    int valueIndex = parentNode->ValueIndex(node0->GetPageId());
    page_id_t neighbor_node_id;
    LeafPage *neighbor_node;
    if (valueIndex == 0) {
      valueIndex = 1;
      neighbor_node_id = parentNode->ValueAt(1);
    } else {
      neighbor_node_id = parentNode->ValueAt(valueIndex - 1);
    }
    neighbor_node = reinterpret_cast<LeafPage *>(this->buffer_pool_manager_->FetchPage(neighbor_node_id));
    this->lock(reinterpret_cast<Page *>(neighbor_node), LockType::WRITE);
    transaction->AddIntoPageSet(reinterpret_cast<Page *>(neighbor_node));
    // 2. if neighbor_node and node0 can fit in a single node
    if (neighbor_node->GetSize() + node0->GetSize() < this->leaf_max_size_) {
      this->Coalesce(reinterpret_cast<BPlusTreePage **>(&neighbor_node), reinterpret_cast<BPlusTreePage **>(&node),
                     &parentNode, valueIndex, transaction);
      transaction->AddIntoDeletedPageSet(reinterpret_cast<BPlusTreePage *>(node)->GetPageId());
      KeyType middle_key = parentNode->KeyAt(valueIndex);
      this->delete_entry(parentNode, middle_key, transaction);
      return true;
    }
    this->Redistribute(neighbor_node, node0, valueIndex);
  } else {
    // 1. node is internal
    InternalPage *node0 = reinterpret_cast<InternalPage *>(node);
    page_id_t parentId = node0->GetParentPageId();
    InternalPage *parentNode = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(parentId));
    this->buffer_pool_manager_->UnpinPage(parentId, true);
    int valueIndex = parentNode->ValueIndex(node0->GetPageId());
    page_id_t neighbor_node_id;
    InternalPage *neighbor_node;
    if (valueIndex == 0) {
      valueIndex = 1;
      neighbor_node_id = parentNode->ValueAt(1);
    } else {
      neighbor_node_id = parentNode->ValueAt(valueIndex - 1);
    }
    neighbor_node = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(neighbor_node_id));
    this->lock(reinterpret_cast<Page *>(neighbor_node), LockType::WRITE);
    transaction->AddIntoPageSet(reinterpret_cast<Page *>(neighbor_node));
    // 2. if neighbor_node and node0 can fit in a single node
    if (neighbor_node->GetSize() + node0->GetSize() <= this->internal_max_size_) {
      this->Coalesce(reinterpret_cast<BPlusTreePage **>(&neighbor_node), reinterpret_cast<BPlusTreePage **>(&node),
                     &parentNode, valueIndex, transaction);
      transaction->AddIntoDeletedPageSet(reinterpret_cast<BPlusTreePage *>(node)->GetPageId());
      KeyType middle_key = parentNode->KeyAt(valueIndex);
      this->delete_entry(parentNode, middle_key, transaction);
      return true;
    }
    this->Redistribute(neighbor_node, node0, valueIndex);
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  InternalPage *parentNode = *parent;
  // 1. if node is a predecessor of neighbor_node, swap_variables
  if (parentNode->ValueAt(index) == reinterpret_cast<BPlusTreePage *>(*neighbor_node)->GetPageId()) {
    std::swap(*neighbor_node, *node);
  }
  KeyType middle_key = parentNode->KeyAt(index);
  // 2. if node is not a leaf
  if (!reinterpret_cast<BPlusTreePage *>(*node)->IsLeafPage()) {
    InternalPage *deletedNode = reinterpret_cast<InternalPage *>(*node);
    InternalPage *recepientNode = reinterpret_cast<InternalPage *>(*neighbor_node);
    deletedNode->MoveAllTo(recepientNode, middle_key, this->buffer_pool_manager_);
  } else {
    // 3. if node is a leaf
    LeafPage *deletedNode = reinterpret_cast<LeafPage *>(*node);
    LeafPage *recepientNode = reinterpret_cast<LeafPage *>(*neighbor_node);
    deletedNode->MoveAllTo(recepientNode);
  }
  return true;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  page_id_t parentId = reinterpret_cast<BPlusTreePage *>(node)->GetParentPageId();
  InternalPage *parentNode = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(parentId));
  this->buffer_pool_manager_->UnpinPage(parentId, true);
  KeyType middle_key = parentNode->KeyAt(index);
  // 1. if neighbor_node is a predecessor of node
  if (parentNode->ValueAt(index) == reinterpret_cast<BPlusTreePage *>(node)->GetPageId()) {
    // 2. if node is not a leafnode
    if (!reinterpret_cast<BPlusTreePage *>(node)->IsLeafPage()) {
      InternalPage *node0 = reinterpret_cast<InternalPage *>(node);
      InternalPage *node1 = reinterpret_cast<InternalPage *>(neighbor_node);
      node1->MoveLastToFrontOf(node0, middle_key, this->buffer_pool_manager_);
      parentNode->SetKeyAt(index, node0->KeyAt(0));
    } else {
      // 3. if node is a leafnode
      LeafPage *node0 = reinterpret_cast<LeafPage *>(node);
      LeafPage *node1 = reinterpret_cast<LeafPage *>(neighbor_node);
      node1->MoveLastToFrontOf(node0);
      parentNode->SetKeyAt(index, node0->KeyAt(0));
    }
  } else {
    // 4. node is a predecessor of neighbor_node
    if (!reinterpret_cast<BPlusTreePage *>(node)->IsLeafPage()) {
      // 5. if node is not a leafnode
      InternalPage *node0 = reinterpret_cast<InternalPage *>(node);
      InternalPage *node1 = reinterpret_cast<InternalPage *>(neighbor_node);
      node1->MoveFirstToEndOf(node0, middle_key, this->buffer_pool_manager_);
      parentNode->SetKeyAt(index, node1->KeyAt(0));
    } else {
      // 6. if node is a leafnode
      LeafPage *node0 = reinterpret_cast<LeafPage *>(node);
      LeafPage *node1 = reinterpret_cast<LeafPage *>(neighbor_node);
      node1->MoveFirstToEndOf(node0);
      parentNode->SetKeyAt(index, node1->KeyAt(0));
    }
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (reinterpret_cast<BPlusTreePage *>(old_root_node)->IsLeafPage()) {
    // root node is leaf node
    LeafPage *leafNode = reinterpret_cast<LeafPage *>(old_root_node);
    if (leafNode->GetSize() == 0) {
      this->root_page_id_ = INVALID_PAGE_ID;
      this->UpdateRootPageId(false);
      return true;
    }
  } else {
    // root node is internal node
    InternalPage *internalNode = reinterpret_cast<InternalPage *>(old_root_node);
    if (internalNode->GetSize() == 1) {
      page_id_t new_root_id = internalNode->ValueAt(0);
      BPlusTreePage *new_root_node =
          reinterpret_cast<BPlusTreePage *>(this->buffer_pool_manager_->FetchPage(new_root_id));
      // child must have been locked by this thread( pin 2 times ), so we can unpin it directly
      this->buffer_pool_manager_->UnpinPage(new_root_id, true);
      new_root_node->SetParentPageId(INVALID_PAGE_ID);
      this->root_page_id_ = new_root_id;
      this->UpdateRootPageId(false);
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType key;
  this->lockRoot(LockType::READ);
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(this->getFindLeafPageWithLock(key, true));
  if (leafPage == nullptr) {
    return INDEXITERATOR_TYPE(this->buffer_pool_manager_, INVALID_PAGE_ID, 0);
  }
  page_id_t pageId = leafPage->GetPageId();
  this->unlock(reinterpret_cast<Page *>(leafPage), LockType::READ);
  this->tryUnlockRoot(LockType::READ);
  return INDEXITERATOR_TYPE(this->buffer_pool_manager_, pageId, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  this->lockRoot(LockType::READ);
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(this->getFindLeafPageWithLock(key, false));
  if (leafPage == nullptr) {
    return INDEXITERATOR_TYPE(this->buffer_pool_manager_, INVALID_PAGE_ID, 0);
  }
  page_id_t pageId = leafPage->GetPageId();
  int index = leafPage->KeyIndex(key, this->comparator_);
  assert(index < leafPage->GetSize());
  this->unlock(reinterpret_cast<Page *>(leafPage), LockType::READ);
  this->tryUnlockRoot(LockType::READ);
  return INDEXITERATOR_TYPE(this->buffer_pool_manager_, pageId, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(this->buffer_pool_manager_, INVALID_PAGE_ID, 0); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // deprated!!! (don't support concurrent)
  if (this->IsEmpty()) {
    return nullptr;
  }
  Page *pagePtr = this->buffer_pool_manager_->FetchPage(this->root_page_id_);
  page_id_t curPageID;
  page_id_t nextPageID;
  for (curPageID = this->root_page_id_; !reinterpret_cast<BPlusTreePage *>(pagePtr)->IsLeafPage();
       curPageID = nextPageID, pagePtr = buffer_pool_manager_->FetchPage(curPageID)) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(pagePtr);
    if (leftMost) {
      nextPageID = internalPage->ValueAt(0);
    } else {
      nextPageID = internalPage->Lookup(key, comparator_);
    }
    this->buffer_pool_manager_->UnpinPage(curPageID, false);
  }
  return pagePtr;
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::getFindLeafPageWithLock(const KeyType &key, bool leftMost) {
  if (this->IsEmpty()) {
    this->tryUnlockRoot(LockType::READ);
    return nullptr;
  }
  Page *pagePtr = this->buffer_pool_manager_->FetchPage(this->root_page_id_);
  this->lock(pagePtr, LockType::READ);
  page_id_t curPageID;
  page_id_t nextPageID;
  for (curPageID = this->root_page_id_; !reinterpret_cast<BPlusTreePage *>(pagePtr)->IsLeafPage();) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(pagePtr);
    if (leftMost) {
      nextPageID = internalPage->ValueAt(0);
    } else {
      nextPageID = internalPage->Lookup(key, comparator_);
    }
    Page *parentPage = pagePtr;
    curPageID = nextPageID;
    pagePtr = buffer_pool_manager_->FetchPage(curPageID);
    this->lock(pagePtr, LockType::READ);
    this->unlock(parentPage, LockType::READ);
    this->buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
    this->tryUnlockRoot(LockType::READ);
  }
  return pagePtr;
}

/*
 * call this function with Root Lock !!! Be carefully!!!
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::insertFindLeafPageWithLock(const KeyType &key, Transaction *transaction) {
  Page *pagePtr = this->buffer_pool_manager_->FetchPage(this->root_page_id_);
  this->lock(pagePtr, LockType::WRITE);
  transaction->AddIntoPageSet(pagePtr);
  page_id_t curPageID = this->root_page_id_;
  page_id_t nextPageID;
  for (; !reinterpret_cast<BPlusTreePage *>(pagePtr)->IsLeafPage();) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(pagePtr);
    nextPageID = internalPage->Lookup(key, comparator_);
    curPageID = nextPageID;
    pagePtr = buffer_pool_manager_->FetchPage(curPageID);
    this->lock(reinterpret_cast<Page *>(pagePtr), LockType::WRITE);
    this->checkAndSolveSafe(OpType::INSERT, pagePtr, transaction);
    transaction->AddIntoPageSet(pagePtr);
  }
  return pagePtr;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::removeFindLeafPageWithLock(const KeyType &key, Transaction *transaction) {
  Page *pagePtr = this->buffer_pool_manager_->FetchPage(this->root_page_id_);
  this->lock(pagePtr, LockType::WRITE);
  transaction->AddIntoPageSet(pagePtr);
  page_id_t curPageID;
  page_id_t nextPageID;
  for (curPageID = this->root_page_id_; !reinterpret_cast<BPlusTreePage *>(pagePtr)->IsLeafPage();) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(pagePtr);
    nextPageID = internalPage->Lookup(key, comparator_);
    curPageID = nextPageID;
    pagePtr = buffer_pool_manager_->FetchPage(curPageID);
    this->lock(reinterpret_cast<Page *>(pagePtr), LockType::WRITE);
    this->checkAndSolveSafe(OpType::REMOVE, pagePtr, transaction);
    transaction->AddIntoPageSet(pagePtr);
  }
  return pagePtr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
