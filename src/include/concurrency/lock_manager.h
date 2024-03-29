//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <fstream>
#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <stack>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_;
  };

  class LockRequestQueue {
   public:
    std::list<LockRequest> request_queue_;
    std::condition_variable cv_;  // for notifying blocked transactions on this rid
    bool upgrading_ = false;
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
    LOG_INFO("Cycle detection thread launched");
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
    LOG_INFO("Cycle detection thread stopped");
  }

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the same transaction, i.e. the transaction
   *    is responsible for keeping track of its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

  /*** Graph API ***/
  /**
   * Adds edge t1->t2
   */

  /** Adds an edge from t1 -> t2. */
  void AddEdge(txn_id_t t1, txn_id_t t2);

  /** Removes an edge from t1 -> t2. */
  void RemoveEdge(txn_id_t t1, txn_id_t t2);

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  bool HasCycle(txn_id_t *txn_id);

  /** @return the set of all edges in the graph, used for testing only! */
  std::vector<std::pair<txn_id_t, txn_id_t>> GetEdgeList();

  /** Runs cycle detection in the background. */
  void RunCycleDetection();

  bool isExistAnyLockInQueue(const RID &rid) {
    if (this->lock_table_[rid].upgrading_) {
      return true;
    }
    for (const auto &lockRequest : this->lock_table_[rid].request_queue_) {
      if (lockRequest.granted_) {
        return true;
      }
    }
    return false;
  }

  bool isExistExclusiveLockInQueue(const RID &rid) {
    if (this->lock_table_[rid].upgrading_) {
      return true;
    }
    for (const auto &lockRequest : this->lock_table_[rid].request_queue_) {
      if (lockRequest.lock_mode_ == LockMode::EXCLUSIVE && lockRequest.granted_) {
        return true;
      }
    }
    return false;
  }

  bool isHasUpgradeOrOtherLockInQueue(const RID &rid, txn_id_t txn_id) {
    if (this->lock_table_[rid].upgrading_) {
      return true;
    }
    for (const auto &lockRequest : this->lock_table_[rid].request_queue_) {
      if (lockRequest.granted_ && lockRequest.txn_id_ != txn_id) {
        return true;
      }
    }
    return false;
  }

  void grantRequestInQueue(const RID &rid, txn_id_t txnId) {
    for (auto &lockRequest : this->lock_table_[rid].request_queue_) {
      if (lockRequest.txn_id_ == txnId) {
        lockRequest.granted_ = true;
        return;
      }
    }
  }

  /* if this txn make queue upgrade, then deupgrade it */
  void eraseRequestInQueue(Transaction *txn, const RID &rid) {
    if (this->lock_table_[rid].upgrading_ && txn->GetExclusiveLockSet()->count(rid) != 0) {
      this->lock_table_[rid].upgrading_ = false;
    }
    for (auto iter = this->lock_table_[rid].request_queue_.begin(); iter != this->lock_table_[rid].request_queue_.end();
         iter++) {
      if (iter->txn_id_ == txn->GetTransactionId()) {
        this->lock_table_[rid].request_queue_.erase(iter);
        return;
      }
    }
  }

  bool dfs(txn_id_t root_txn_id, txn_id_t *max_txn_id, std::unordered_set<txn_id_t> *isVis_,
           std::stack<txn_id_t> *cycle_stack_, std::unordered_set<txn_id_t> *cycle_set_) {
    isVis_->insert(root_txn_id);
    cycle_stack_->push(root_txn_id);
    cycle_set_->insert(root_txn_id);
    for (const txn_id_t &t2 : this->gragh_[root_txn_id]) {
      // if t2 has been visited
      if (isVis_->count(t2) != 0) {
        // if t2 don't exist in stack, skip it
        if (cycle_set_->count(t2) == 0) {
          continue;
        }
        // if t2 exist in stack, there is a cycle, backtrack
        *max_txn_id = t2;
        while (cycle_stack_->top() != t2) {
          *max_txn_id = std::max(*max_txn_id, cycle_stack_->top());
          cycle_stack_->pop();
        }
        return true;
      }
      if (dfs(t2, max_txn_id, isVis_, cycle_stack_, cycle_set_)) {
        return true;
      }
    }
    cycle_stack_->pop();
    cycle_set_->erase(root_txn_id);
    return false;
  }

 private:
  std::mutex latch_;
  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;

  /** gragh */
  std::map<txn_id_t, std::set<txn_id_t>> gragh_;
  /** In order to communicate between txn and detection */
  std::unordered_map<txn_id_t, RID> wait_rid_;
  std::unordered_map<txn_id_t, bool> isAbort_;
};

}  // namespace bustub
