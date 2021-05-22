//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  this->tableIndexes_ = this->getTableIndexes();
  this->num_inserted_ = 0;
  if (!this->plan_->IsRawInsert()) {
    this->child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (this->plan_->IsRawInsert()) {
    size_t i = this->num_inserted_;
    if (i >= this->plan_->RawValues().size()) {
      return false;
    }
    *tuple = Tuple(this->plan_->RawValuesAt(i), this->getSchema());
    assert(this->getTableHeap()->InsertTuple(*tuple, rid, this->exec_ctx_->GetTransaction()) == true);
    for (auto index : this->tableIndexes_) {
      Tuple key =
          tuple->KeyFromTuple(*(this->getSchema()), *(index->index_->GetKeySchema()), index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, *rid, this->exec_ctx_->GetTransaction());
    }
    this->num_inserted_++;
    return true;
  }

  if (child_executor_->Next(tuple, rid)) {
    assert(this->getTableHeap()->InsertTuple(*tuple, rid, this->exec_ctx_->GetTransaction()) == true);
    for (auto index : this->tableIndexes_) {
      Tuple key =
          tuple->KeyFromTuple(*(this->getSchema()), *(index->index_->GetKeySchema()), index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, *rid, this->exec_ctx_->GetTransaction());
    }
    return true;
  }
  return false;
}

}  // namespace bustub
