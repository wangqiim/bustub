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

#include <algorithm>
#include <set>
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
  this->lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
  // 3. check all request in queue, if can't acquire lock, wait
  while (this->isExistExclusiveLockInQueue(rid)) {
    // refresh waits_for_
    for (const auto &lockRequest : this->lock_table_[rid].request_queue_) {
      if (lockRequest.granted_) {
        this->waits_for_[txn->GetTransactionId()].push_back(lockRequest.txn_id_);
      }
    }
    this->wait_rid_[txn->GetTransactionId()] = rid;
    this->lock_table_[rid].cv_.wait(lock);
    this->waits_for_[txn->GetTransactionId()].clear();
    if (this->isAbort_[txn->GetTransactionId()]) {
      this->isAbort_.erase(txn->GetTransactionId());
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
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
  this->lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
  // 3.  check all request in queue, if can't acquire lock, wait
  while (this->isExistAnyLockInQueue(rid)) {
    // refresh waits_for_
    for (const auto &lockRequest : this->lock_table_[rid].request_queue_) {
      if (lockRequest.granted_) {
        this->waits_for_[txn->GetTransactionId()].push_back(lockRequest.txn_id_);
      }
    }
    this->wait_rid_[txn->GetTransactionId()] = rid;
    this->lock_table_[rid].cv_.wait(lock);
    this->waits_for_[txn->GetTransactionId()].clear();
    if (this->isAbort_[txn->GetTransactionId()]) {
      this->isAbort_.erase(txn->GetTransactionId());
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  }
  // 4. if can acquire lock, update lock_queue
  this->grantRequestInQueue(rid, txn->GetTransactionId());
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(this->latch_);
  assert(txn->GetSharedLockSet()->count(rid) != 0);  // txn must have shared lock at first
  // 1. if the txn isn't GROWING, return false
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 2.  check all request in queue, if can't upgrade lock, wait
  while (this->isExistExclusiveLockInQueue(rid)) {
    for (const auto &lockRequest : this->lock_table_[rid].request_queue_) {
      if (lockRequest.granted_ && lockRequest.txn_id_ != txn->GetTransactionId()) {
        this->waits_for_[txn->GetTransactionId()].push_back(lockRequest.txn_id_);
      }
    }
    this->wait_rid_[txn->GetTransactionId()] = rid;
    this->lock_table_[rid].cv_.wait(lock);
    this->waits_for_[txn->GetTransactionId()].clear();
    if (this->isAbort_[txn->GetTransactionId()]) {
      this->isAbort_.erase(txn->GetTransactionId());
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  }
  // 3. if can upgrade lock, upgrade it
  this->lock_table_[rid].upgrading_ = true;
  // 4. erase sharedLock set from txn, insert exclusiveLock set
  txn->GetSharedLockSet()->erase(rid);
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

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  int node_size = static_cast<txn_id_t>(this->gragh_.size());
  if (node_size <= t1 || node_size <= t2) {
    this->gragh_.resize(std::max(t1 + 1, t2 + 1));
  }
  this->gragh_[t1].insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { this->gragh_[t1].erase(t2); }

bool LockManager::HasCycle(txn_id_t *txn_id) {
  // 0. get ready, incresing order
  std::set<txn_id_t> nodes;
  std::unordered_map<txn_id_t, bool> isVis;
  txn_id_t maxsize = static_cast<txn_id_t>(gragh_.size());
  for (txn_id_t t1 = 0; t1 < maxsize; t1++) {
    if (!this->gragh_[t1].empty()) {
      nodes.insert(t1);
      isVis[t1] = false;
    }
    for (const auto &t2 : this->gragh_[t1]) {
      nodes.insert(t2);
      isVis[t2] = false;
    }
  }
  if (nodes.empty()) {
    return false;
  }
  // 1. check cycle
  auto iter = nodes.end();
  while ((iter--) != nodes.begin()) {
    for (auto &txn_vis : isVis) {
      txn_vis.second = false;
    }
    txn_id_t cur_txn_id = *iter;
    if (dfs(cur_txn_id, cur_txn_id, &isVis)) {
      *txn_id = cur_txn_id;
      return true;
    }
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> res;
  int node_size = static_cast<txn_id_t>(this->gragh_.size());
  for (txn_id_t t1 = 0; t1 < node_size; t1++) {
    for (const auto &t2 : this->gragh_[t1]) {
      res.emplace_back(t1, t2);
    }
  }
  return res;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      for (auto &t1_wait_txns : this->waits_for_) {
        for (auto &t2 : t1_wait_txns.second) {
          this->AddEdge(t1_wait_txns.first, t2);
        }
      }
      txn_id_t txn_id;
      if (this->HasCycle(&txn_id)) {
        // abort-> clear queue -> notify_all
        this->isAbort_[txn_id] = true;
        RID rid = this->wait_rid_[txn_id];
        // std::cout << "HasCycle, rid = " << rid << std::endl;
        this->wait_rid_.erase(txn_id);
        this->lock_table_[rid].upgrading_ = false;
        for (auto iter = this->lock_table_[rid].request_queue_.begin();
             iter != this->lock_table_[rid].request_queue_.end(); iter++) {
          if (iter->txn_id_ == txn_id) {
            this->lock_table_[rid].request_queue_.erase(iter);
            break;
          }
        }
        this->lock_table_[rid].cv_.notify_all();
      }
      this->ClearGragh();
    }
  }
}

}  // namespace bustub
