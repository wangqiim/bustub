//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  assert(this->left_executor_ != nullptr);
  assert(this->right_executor_ != nullptr);
  this->left_executor_->Init();
  this->right_executor_->Init();
  outer_tuple_ = std::make_unique<Tuple>();
  RID rid;
  if (!this->left_executor_->Next(this->outer_tuple_.get(), &rid)) {
    this->outer_tuple_ = nullptr;
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  assert(this->outer_tuple_ != nullptr);
  while (true) {
    // 1. if right table is end, record next tuple in left(outer) table
    if (!this->right_executor_->Next(&right_tuple, rid)) {
      if (!this->left_executor_->Next(this->outer_tuple_.get(), rid)) {
        // 2. if left table is end, return false
        this->outer_tuple_ = nullptr;
        return false;
      }
      // 3. left table is not end, init right cursor
      this->right_executor_->Init();
      continue;
    }
    if (this->plan_->Predicate()
            ->EvaluateJoin(this->outer_tuple_.get(), left_executor_->GetOutputSchema(), &right_tuple,
                           right_executor_->GetOutputSchema())
            .GetAs<bool>()) {
      *tuple = this->genJoinTuple(this->outer_tuple_.get(), &right_tuple, this->left_executor_->GetOutputSchema(),
                                  this->right_executor_->GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
