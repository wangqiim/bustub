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
  this->tableIndexes_ = this->exec_ctx_->GetCatalog()->GetTableIndexes(this->table_info_->name_);
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->TableOid());
  if (this->child_executor_ != nullptr) {
    this->child_executor_->Init();
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tup;
  if (this->child_executor_->Next(&old_tup, rid)) {
    *tuple = this->GenerateUpdatedTuple(old_tup);
    assert(this->table_info_->table_->UpdateTuple(*tuple, *rid, this->exec_ctx_->GetTransaction()) == true);

    // delete old_tuple and insert new index
    for (auto index : this->tableIndexes_) {
      Tuple old_key =
          old_tup.KeyFromTuple(*(this->getSchema()), *(index->index_->GetKeySchema()), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_key, *rid, this->exec_ctx_->GetTransaction());

      Tuple key =
          tuple->KeyFromTuple(*(this->getSchema()), *(index->index_->GetKeySchema()), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, this->exec_ctx_->GetTransaction());
    }
    return true;
  }
  return false;
}
}  // namespace bustub
