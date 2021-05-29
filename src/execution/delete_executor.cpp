//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->TableOid());
  this->tableIndexes_ = this->exec_ctx_->GetCatalog()->GetTableIndexes(this->table_info_->name_);
  if (this->child_executor_ != nullptr) {
    this->child_executor_->Init();
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (this->child_executor_->Next(tuple, rid)) {
    if (this->exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      this->exec_ctx_->GetCatalog()->GetLockManger()->LockUpgrade(this->exec_ctx_->GetTransaction(), *rid);
    } else {  // RUC or RR
      this->exec_ctx_->GetCatalog()->GetLockManger()->LockExclusive(this->exec_ctx_->GetTransaction(), *rid);
    }
    assert(this->table_info_->table_->MarkDelete(*rid, this->exec_ctx_->GetTransaction()) == true);
    for (auto index : this->tableIndexes_) {
      Tuple key =
          tuple->KeyFromTuple(*(this->getSchema()), *(index->index_->GetKeySchema()), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, this->exec_ctx_->GetTransaction());
      this->exec_ctx_->GetTransaction()->AppendTableWriteRecord(IndexWriteRecord(
          *rid, this->plan_->TableOid(), WType::DELETE, *tuple, index->index_oid_, this->exec_ctx_->GetCatalog()));
    }
    return true;
  }
  return false;
}

}  // namespace bustub
