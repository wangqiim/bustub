//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param outer table child
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

 private:
  IndexInfo *getIndexInfo() const {
    std::string table_name = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetInnerTableOid())->name_;
    return this->exec_ctx_->GetCatalog()->GetIndex(this->plan_->GetIndexName(), table_name);
  }

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
    return this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetInnerTableOid())->table_.get();
  }

  // const Schema &getSchema() const {
  //   return this->exec_ctx_->GetCatalog()->GetTable(this->getIndexInfo()->table_name_)->schema_;
  // }

  const Schema &getKeySchema() const { return *(this->getIndexInfo()->index_->GetKeySchema()); }

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

  const std::vector<uint32_t> &getKeyAttrs() const { return this->getIndexInfo()->index_->GetKeyAttrs(); }
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::unique_ptr<Tuple> outer_tuple_;
  /* this iter_ used to travels inner table */
  IndexIterator<GenericKey<8>, RID, GenericComparator<8>> iter_;
};
}  // namespace bustub
