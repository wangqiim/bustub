//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  this->iter_ = this->getBeginIterator();
  this->outer_tuple_ = std::make_unique<Tuple>();
  assert(this->child_executor_ != nullptr);
  this->child_executor_->Init();
  RID rid;
  if (!this->child_executor_->Next(this->outer_tuple_.get(), &rid)) {
    this->outer_tuple_ = nullptr;
  }
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  assert(this->outer_tuple_ != nullptr);
  while (true) {
    // 1. if right table is end, record next tuple in left(outer) table
    if (this->iter_ == this->getEndIterator()) {
      if (!this->child_executor_->Next(this->outer_tuple_.get(), rid)) {
        // 2. if left table is end, return false
        this->outer_tuple_ = nullptr;
        return false;
      }
      // 3. left table is not end, init right cursor
      this->iter_ = this->getBeginIterator();
      continue;
    }
    *rid = (*(this->iter_)).second;
    ++this->iter_;
    assert(this->getTableHeap()->GetTuple(*rid, &right_tuple, this->exec_ctx_->GetTransaction()));
    // 4. check whether two schema to join
    if (this->plan_->Predicate()
            ->EvaluateJoin(this->outer_tuple_.get(), this->plan_->OuterTableSchema(), &right_tuple,
                           this->plan_->InnerTableSchema())
            .GetAs<bool>()) {
      // 5. generate join tuple
      *tuple = this->genJoinTuple(this->outer_tuple_.get(), &right_tuple, this->plan_->OuterTableSchema(),
                                  this->plan_->InnerTableSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
