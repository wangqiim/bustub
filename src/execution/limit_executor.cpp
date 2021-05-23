//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  this->cur_ = 0;
  this->offset_ = this->plan_->GetOffset();
  this->limit_ = this->plan_->GetLimit();
  assert(this->child_executor_ != nullptr);
  this->child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  // 1, 2, 3, 4, 5  -> limit 3 offset 1 - > 2, 3, 4
  while (this->child_executor_->Next(tuple, rid)) {
    size_t cur = this->cur_++;
    if (cur >= this->offset_ && cur < this->offset_ + this->limit_) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
