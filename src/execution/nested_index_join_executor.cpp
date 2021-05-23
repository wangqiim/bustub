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
  this->outer_tuple_ = std::make_unique<Tuple>();
  assert(this->child_executor_ != nullptr);
  this->child_executor_->Init();
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  Tuple inner_tuple;
  assert(this->outer_tuple_ != nullptr);
  while (this->child_executor_->Next(this->outer_tuple_.get(), rid)) {
    // i think it is wrong, because i use inner index as outer index. i can't get outer inner :(
    Tuple key =
        this->outer_tuple_->KeyFromTuple(*this->plan_->OuterTableSchema(), this->getKeySchema(), this->getKeyAttrs());
    std::vector<RID> rids;
    this->getIndex()->ScanKey(key, &rids, this->exec_ctx_->GetTransaction());
    if (rids.empty()) {
      continue;
    }
    this->getInnerTableHeap()->GetTuple(rids[0], &right_tuple, this->exec_ctx_->GetTransaction());
    inner_tuple = this->genOutputTuple(&right_tuple, &this->getInnerSchema(), this->plan_->InnerTableSchema());
    if (this->plan_->Predicate()
            ->EvaluateJoin(this->outer_tuple_.get(), this->plan_->OuterTableSchema(), &inner_tuple,
                           this->plan_->InnerTableSchema())
            .GetAs<bool>()) {
      *tuple = this->genJoinTuple(this->outer_tuple_.get(), &inner_tuple, this->plan_->OuterTableSchema(),
                                  this->plan_->InnerTableSchema());
      return true;
    }
  }
  this->outer_tuple_ = nullptr;
  return false;
}

}  // namespace bustub
