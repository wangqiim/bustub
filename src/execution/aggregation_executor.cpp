//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(this->aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  this->child_->Init();
  Tuple tuple;
  RID rid;
  while (this->child_->Next(&tuple, &rid)) {
    AggregateKey agg_key = this->MakeKey(&tuple);
    AggregateValue agg_val = this->MakeVal(&tuple);
    // if (this->plan_->GetHaving() != nullptr) {
    //   if (!this->plan_->GetHaving()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_).GetAs<bool>()) {
    //     continue;
    //   }
    // }
    this->aht_.InsertCombine(agg_key, agg_val);
  }
  this->aht_iterator_ = this->aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (this->aht_iterator_ != this->aht_.End()) {
    const AggregateKey &agg_key = this->aht_iterator_.Key();
    const AggregateValue &agg_val = this->aht_iterator_.Val();
    ++(this->aht_iterator_);
    if (this->plan_->GetHaving() != nullptr) {
      if (!this->plan_->GetHaving()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_).GetAs<bool>()) {
        continue;
      }
    }
    std::vector<Value> values;
    for (const Column &col : this->GetOutputSchema()->GetColumns()) {
      Value value;
      value = col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_);
      values.push_back(value);
    }
    *tuple = Tuple(values, this->GetOutputSchema());
    return true;
  }
  return false;
}

}  // namespace bustub
