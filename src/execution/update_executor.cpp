//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->TableOid());
  this->tableIndexes_ = this->exec_ctx_->GetCatalog()->GetTableIndexes(this->table_info_->name_);
  if (this->child_executor_ != nullptr) {
    this->child_executor_->Init();
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tup;
  if (this->child_executor_->Next(&old_tup, rid)) {
    *tuple = this->GenerateUpdatedTuple(old_tup);

    if (this->exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      this->exec_ctx_->GetCatalog()->GetLockManger()->LockUpgrade(this->exec_ctx_->GetTransaction(), *rid);
    } else {  // RUC or RR
      this->exec_ctx_->GetCatalog()->GetLockManger()->LockExclusive(this->exec_ctx_->GetTransaction(), *rid);
    }
    assert(this->table_info_->table_->UpdateTuple(*tuple, *rid, this->exec_ctx_->GetTransaction()) == true);

    // delete old_tuple and insert new index
    for (auto index : this->tableIndexes_) {
      Tuple old_key = old_tup.KeyFromTuple(*(this->child_executor_->GetOutputSchema()),
                                           *(index->index_->GetKeySchema()), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_key, *rid, this->exec_ctx_->GetTransaction());

      Tuple key = tuple->KeyFromTuple(*(this->child_executor_->GetOutputSchema()), *(index->index_->GetKeySchema()),
                                      index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, *rid, this->exec_ctx_->GetTransaction());
      IndexWriteRecord indexWriteRecord(IndexWriteRecord(*rid, this->plan_->TableOid(), WType::DELETE, *tuple,
                                                         index->index_oid_, this->exec_ctx_->GetCatalog()));
      indexWriteRecord.old_tuple_ = old_tup;
      this->exec_ctx_->GetTransaction()->AppendTableWriteRecord(indexWriteRecord);
    }
    return true;
  }
  return false;
}
}  // namespace bustub
