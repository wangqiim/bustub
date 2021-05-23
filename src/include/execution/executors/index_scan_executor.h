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
#include "storage/index/index_iterator.h"
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
  IndexInfo *getIndexInfo() const { return this->exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid()); }

  Index *getIndex() const { return this->getIndexInfo()->index_.get(); }

  IndexIterator<GenericKey<8>, RID, GenericComparator<8>> getBeginIterator() const {
    return reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(this->getIndex())
        ->GetBeginIterator();
  }

  IndexIterator<GenericKey<8>, RID, GenericComparator<8>> getEndIterator() const {
    return reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(this->getIndex())
        ->GetEndIterator();
  }

  TableHeap *getTableHeap() const {
    return this->exec_ctx_->GetCatalog()->GetTable(this->getIndexInfo()->table_name_)->table_.get();
  }

  const Schema &getSchema() const {
    return this->exec_ctx_->GetCatalog()->GetTable(this->getIndexInfo()->table_name_)->schema_;
  }

  const Schema &getKeySchema() const { return *(this->getIndexInfo()->index_->GetKeySchema()); }

  const std::vector<uint32_t> &getKeyAttrs() const { return this->getIndexInfo()->index_->GetKeyAttrs(); }

  Tuple genOutputTuple(const Tuple *raw_tuple, const Schema *schema, const Schema *outputSchema) {
    std::vector<Value> values;
    // generate new_tuple from raw_tuple through outputschema
    for (const Column &col : outputSchema->GetColumns()) {
      Value val = raw_tuple->GetValue(schema, schema->GetColIdx(col.GetName()));
      values.push_back(val);
    }
    return Tuple(values, this->GetOutputSchema());
  }

  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  IndexIterator<GenericKey<8>, RID, GenericComparator<8>> iter_;
};
}  // namespace bustub
