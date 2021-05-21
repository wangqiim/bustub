//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

 private:
  const Schema &getSchema() const {
    return this->exec_ctx_->GetCatalog()->GetTable(this->indexInfo_->table_name_)->schema_;
  }

  const Schema &getKeySchema() const { return *(indexInfo_->index_->GetKeySchema()); }

  const std::vector<uint32_t> &getKeyAttrs() const { return this->indexInfo_->index_->GetKeyAttrs(); }

  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  IndexInfo *indexInfo_;
  TableHeap *tableHeap_;
  std::unique_ptr<TableIterator> iter_;
};
}  // namespace bustub
