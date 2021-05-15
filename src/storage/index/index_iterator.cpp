/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() { throw std::runtime_error("unimplemented"); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index)
    : bpm_(bpm), page_id_(page_id), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return this->page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  assert(this->page_id_ != INVALID_PAGE_ID);
  B_PLUS_TREE_LEAF_PAGE_TYPE *leafNode =
      reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(this->bpm_->FetchPage(this->page_id_));
  const MappingType &mappingType = leafNode->GetItem(this->index_);
  this->bpm_->UnpinPage(this->page_id_, false);
  return mappingType;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  assert(this->page_id_ != INVALID_PAGE_ID);
  page_id_t old_page_id = this->page_id_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leafNode =
      reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(this->bpm_->FetchPage(this->page_id_));
  this->index_++;
  if (this->index_ == leafNode->GetSize()) {
    this->page_id_ = leafNode->GetNextPageId();
    this->index_ = 0;
  }
  this->bpm_->UnpinPage(old_page_id, false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
