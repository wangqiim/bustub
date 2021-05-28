//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {
// think about isolation level
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(this->latch_);
  // 1. if the txn isn't GROWING, abort it and return false
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 2. add txn in lock_table_
  this->lock_table_[rid].request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
  // 3. check all request in queue, if can't acquire lock, wait 
  while (this->isExistExclusiveLockInQueue(rid)) {
    this->lock_table_[rid].cv_.wait(lock);
  }
  // 4. if can acquire lock, update lock_queue
  this->grantRequestInQueue(rid, txn->GetTransactionId());
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(this->latch_);
  // 1. if the txn isn't GROWING, return false
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 2. add txn in lock_table_
  this->lock_table_[rid].request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
  // 3.  check all request in queue, if can't acquire lock, wait 
  while (this->isExistAnyLockInQueue(rid)) {
    this->lock_table_[rid].cv_.wait(lock);
  }
  // 4. if can acquire lock, update lock_queue
  this->grantRequestInQueue(rid, txn->GetTransactionId());
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(this->latch_);
  assert(txn->GetSharedLockSet()->count(rid) != 0); // txn must have shared lock at first
  // 1. if the txn isn't GROWING, return false
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 2. erase sharedLock set from txn
  txn->GetSharedLockSet()->erase(rid);
  // 3.  check all request in queue, if can't upgrade lock, wait 
  while (this->isExistExclusiveLockInQueue(rid)) {
    this->lock_table_[rid].cv_.wait(lock);
  }
  // 4. if can upgrade lock, upgrade it
  this->lock_table_[rid].upgrading_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(this->latch_);
  // if state is abort, we also need to unlock 
  // 1. if state is not growing and shrinking, return false
  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // 2. erase tx from request_queue and notify_all threads in request queue
  this->eraseRequestInQueue(txn, rid);
  this->lock_table_[rid].cv_.notify_all();
  // 3. erase rid lock from txn
  assert(txn->GetSharedLockSet()->count(rid) != 0 || txn->GetExclusiveLockSet()->count(rid) != 0);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
