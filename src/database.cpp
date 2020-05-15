//
// Created by 范軒瑋 on 2020/5/13.
//

#include "database.h"
#include "lock_manager.h"

namespace mvcc {

MemoryDB::MemoryDB() {
    mLockManager = std::make_shared<LockManager>();
}

int MemoryDB::Insert(TxnId id, const KeyType &key, const ValueType &val) {
    // Should add mutex in following assignments
    auto iter = mStorage.find(key);
    DCHECK_EQ(id, INSERT_NO_ID); // for assignment 1 because only preparation contains insert
    if (iter == mStorage.end()) {
        // Not existed in the database before.
        mStorage[key] = LogList();
        // FIXME: Always true in assignment 1 because insertion does not belong to any txn
        auto txnLog = std::make_shared<TxnLog>(id, val, mvcc::GetTimeStamp(), true);
        mStorage[key].push_back(std::move(txnLog));
        return 0;
    } else {
        // Fail. You cannot insert an existed key.
        return 1;
    }
}

int MemoryDB::Read(TxnId id, const KeyType &key, std::shared_ptr<TxnLog> &res) const {
    auto iter = mStorage.find(key);
    if (iter == mStorage.end()) {
        return 1;
    }
    auto &logList = iter->second;
    size_t sz = logList.size();
    auto logIter = std::prev(logList.end());
    // Find the newest committed txn log.
    // Or uncommitted data within the same txn
    while ((*logIter)->id != id && !(*logIter)->committed && logIter != logList.begin()) {
        logIter = std::prev(logIter);
    }
    if (!(*logIter)->committed && (*logIter)->id != id)
        return 1;
    res = std::shared_ptr<TxnLog>(*logIter);
    return 0;
}

int MemoryDB::Update(TxnId id, const KeyType &key, MathOp mOP,
                     const ValueType &val, std::shared_ptr<TxnLog> &res) {
    // TODO: Add lock.
    std::lock_guard<std::mutex> lockGuard(*mLockManager->GetLock(key));

    auto iter = mStorage.find(key);
    if (iter == mStorage.end()) {
        return 1;
    }
    auto &logList = iter->second;
    if (logList.size() == 0) {
        return 1;
    }

    auto logIter = std::prev(logList.end());
    while ((*logIter)->id != id && !(*logIter)->committed && logIter != logList.begin()) {
        logIter = std::prev(logIter);
    }
    ValueType newVal = (*logIter)->val;
    if (mOP == MathOp::PLUS) {
        newVal += val;
    } else if (mOP == MathOp::MINUS) {
        newVal -= val;
    } else if (mOP == MathOp::TIMES) {
        newVal *= val;
    } else if (mOP == MathOp::DIVIDE) {
        if (val == 0) {
            LOG(ERROR) << "Invalid operation: Divide 0 in txn " << id;
            return 1;
        }
        newVal /= val;
    }
    res = std::make_shared<TxnLog>(id, newVal, mvcc::GetTimeStamp());
    logList.push_back(res);
    return 0;
}

std::ostream &operator<<(std::ostream &output, const MemoryDB &memoryDB) {
    output << "- - - - - - - - - - - - - - - - - - - - - - " << std::endl;
    output << "Database status at " << mvcc::GetTimeStamp() << std::endl;
    for(auto &item: memoryDB.mStorage) {
        const KeyType &key = item.first;
        const MemoryDB::LogList &logList = item.second;
        auto iter = std::prev(logList.end());
        while(!(*iter)->committed && iter != logList.begin()) {
            iter = std::prev(iter);
        }
        output << "  " << key << ": " << (*iter)->val << ", by txn_id " << (*iter)->id << ", at " << (*iter)->stamp << std::endl;
    }
    return output;
}


} // namespace mvcc;