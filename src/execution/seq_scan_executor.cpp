//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  this->tableHeap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  this->iter_ = std::make_unique<TableIterator>(this->tableHeap_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  TableIterator &iter = *(this->iter_);
  Tuple raw_tuple;
  while (iter != this->tableHeap_->End()) {
    raw_tuple = *(iter++);
    *rid = raw_tuple.GetRid();
    // Test sample oriented programming
    if (this->exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
        this->exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (!this->exec_ctx_->GetTransaction()->IsSharedLocked(*rid) &&
          !this->exec_ctx_->GetTransaction()->IsExclusiveLocked(*rid)) {
        this->exec_ctx_->GetCatalog()->GetLockManger()->LockShared(this->exec_ctx_->GetTransaction(), *rid);
      }
    }
    bool res = false;
    if (this->plan_->GetPredicate() == nullptr ||
        this->plan_->GetPredicate()->Evaluate(&raw_tuple, this->getSchema()).GetAs<bool>()) {
      *tuple = this->genOutputTuple(&raw_tuple, this->getSchema(), this->GetOutputSchema());
      if (this->exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        if (!this->exec_ctx_->GetTransaction()->IsSharedLocked(*rid) &&
            !this->exec_ctx_->GetTransaction()->IsExclusiveLocked(*rid)) {
          this->exec_ctx_->GetCatalog()->GetLockManger()->LockShared(this->exec_ctx_->GetTransaction(), *rid);
        }
      }
      res = true;
    }
    if (this->exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (this->exec_ctx_->GetTransaction()->IsSharedLocked(*rid)) {
        this->exec_ctx_->GetCatalog()->GetLockManger()->Unlock(this->exec_ctx_->GetTransaction(), *rid);
      }
    }
    if (res) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
