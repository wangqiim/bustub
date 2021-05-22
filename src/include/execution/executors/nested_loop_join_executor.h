//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * NestedLoopJoinExecutor joins two tables using nested loop.
 * The child executor can either be a sequential scan
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new NestedLoop join executor.
   * @param exec_ctx the executor context
   * @param plan the NestedLoop join plan to be executed
   * @param left_executor the child executor that produces tuple for the left side of join
   * @param right_executor the child executor that produces tuple for the right side of join
   *
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

 private:
  Tuple genJoinTuple(Tuple *left_tuple, Tuple *right_tuple, const Schema *left_schema, const Schema *right_schema) {
    std::vector<Value> values;
    for (auto &col : this->GetOutputSchema()->GetColumns()) {
      try {
        Value value = left_tuple->GetValue(left_schema, left_schema->GetColIdx(col.GetName()));
        values.push_back(value);
        continue;
      } catch (std::logic_error &e) {
        // do nothing
      }
      try {
        Value value = right_tuple->GetValue(right_schema, right_schema->GetColIdx(col.GetName()));
        values.push_back(value);
        continue;
      } catch (std::logic_error &e) {
        // do nothing
      }
      UNREACHABLE("Column in GetOutputSchema does not exist");
    }
    return Tuple(values, this->GetOutputSchema());
  }

  /** The NestedLoop plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::unique_ptr<Tuple> outer_tuple_;
};
}  // namespace bustub
