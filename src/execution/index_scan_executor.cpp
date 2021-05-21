//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  this->indexInfo_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  this->tableHeap_ = exec_ctx_->GetCatalog()->GetTable(this->indexInfo_->table_name_)->table_.get();
  this->iter_ = std::make_unique<TableIterator>(this->tableHeap_->Begin(exec_ctx_->GetTransaction()));
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  TableIterator &iter = *(this->iter_);
  while (iter != this->tableHeap_->End()) {
    *tuple = *(iter++);
    *rid = tuple->GetRid();
    // i don't know what should i do, because i lack test file
    Tuple keyTuple = tuple->KeyFromTuple(this->getSchema(), this->getKeySchema(), this->getKeyAttrs());
    if (this->plan_->GetPredicate()->Evaluate(&keyTuple, this->GetOutputSchema()).GetAs<bool>()) {
      return true;
    }
  }
  return false;
}
}  // namespace bustub
