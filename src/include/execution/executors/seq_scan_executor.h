//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  Schema *getSchema() const { return &(this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid())->schema_); }

  Tuple genOutputTuple(const Tuple *raw_tuple, const Schema *schema, const Schema *outputSchema) {
    std::vector<Value> values;
    // generate new_tuple from raw_tuple through outputschema
    for (const Column &col : outputSchema->GetColumns()) {
      Value val = raw_tuple->GetValue(schema, schema->GetColIdx(col.GetName()));
      values.push_back(val);
    }
    return Tuple(values, this->GetOutputSchema());
  }

  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;
  TableHeap *tableHeap_;
  // must use smart_ptr to avoid memory leak!!!
  std::unique_ptr<TableIterator> iter_;
};
}  // namespace bustub
