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

void IndexScanExecutor::Init() { this->iter_ = this->getBeginIterator(); }

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (this->iter_ != this->getEndIterator()) {
    // For this project, you can safely assume the key value type for index is
    // <GenericKey<8>, RID, GenericComparator<8>> to not worry about template,
    // though a more general solution is also welcomed
    *rid = (*(this->iter_)).second;
    ++this->iter_;
    this->getTableHeap()->GetTuple(*rid, tuple, this->exec_ctx_->GetTransaction());
    if (this->plan_->GetPredicate()->Evaluate(tuple, &this->getSchema()).GetAs<bool>()) {
      *tuple = this->genOutputTuple(tuple, &this->getSchema(), this->GetOutputSchema());
      return true;
    }
  }
  return false;
}
}  // namespace bustub
