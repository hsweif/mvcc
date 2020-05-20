//
// Created by 范軒瑋 on 2020/5/13.
//

#include "database.h"

namespace mvcc {

int LogList::AddData(TxnLog txnLog, int &index) {
    logs[curItem] = txnLog;
    index = curItem;
    curItem++;
    if (curItem >= maxItem)
        curItem = 0;
    return 0;
}

int LogList::GetNewestLog(TxnId id, TxnLog &txnLog, const TxnStamp &readStamp) const {
    int curIndex = curItem == 0 ? maxItem - 1 : curItem - 1;
    DCHECK_GE(curIndex, 0);
    DCHECK_LT(curIndex, maxItem);
    int cnt = 0;
    while (logs[curIndex].id != INSERT_NO_ID && readStamp < logs[curIndex].stamp) {
        if (curIndex == 0) {
            curIndex = maxItem;
        }
        curIndex--;
        cnt++;
        if (logs[curIndex].id == INVALID_ID || cnt > maxItem) {
            return 1;
        }
    }
    txnLog = logs[curIndex];
    return 0;
}


int LogList::AddData(const TxnLog &txnLog) {
    if (curItem >= maxItem)
        curItem = 0;
    logs[curItem] = txnLog;
    curItem++;
    return 0;
}

Database::Database() {
    mLock = std::make_shared<std::mutex>();
}

std::shared_ptr<std::mutex> Database::RequestDbLock() {
    return mLock;
}

void Database::BeginTxn(TxnId id, bool includeSet) {
    std::lock_guard<std::mutex> lockGuard(commitLock);
}

MemoryDB::MemoryDB() {
}

int MemoryDB::Insert(TxnId id, const KeyType &key, const ValueType &val) {
    // Should add mutex in following assignments
    auto iter = mStorage.find(key);
    DCHECK_EQ(id, INSERT_NO_ID); // for assignment 1 because only preparation contains insert
    int index;
    if (iter == mStorage.end()) {
        // Not existed in the database before.
        mStorage[key] = LogList();
        mStorage[key].AddData(TxnLog(id, key, val, mvcc::GetTxnStamp(), true), index);
        return 0;
    } else {
        // Fail. You cannot insert an existed key.
        return 1;
    }
}

int MemoryDB::Read(TxnId id, const KeyType &key, TxnLog &res, const TxnStamp &readStamp) const {
    auto iter = mStorage.find(key);
    if (iter == mStorage.end()) {
        return 1;
    }
    const LogList &logList = iter->second;
    int ret = logList.GetNewestLog(id, res, readStamp);
    return ret;
}


std::ostream &operator<<(std::ostream &output, const MemoryDB &memoryDB) {
    output << "- - - - - - - - - - - - - - - - - - - - - - " << std::endl;
    output << "Database status at " << mvcc::GetTxnStamp() << std::endl;
    for (auto &item: memoryDB.mStorage) {
        const KeyType &key = item.first;
        const LogList &logList = item.second;
        TxnLog readLog;
        logList.GetNewestLog(INSERT_NO_ID, readLog, mvcc::GetTxnStamp());
        output << "  " << key << ": " << readLog.val << ", by txn_id " << readLog.id << ", at " << readLog.stamp
               << std::endl;
    }
    return output;
}


int MemoryDB::Commit(TxnId id, std::map<KeyType, TxnLog> &logs, TxnStamp &commitStamp) {
    std::lock_guard<std::mutex> lockGuard(commitLock);
    commitStamp = mvcc::GetTxnStamp();
    for (auto &logItem: logs) {
        // TODO: Handling exceptions.
        auto &log = logItem.second;
        int ret = this->Update(log.id, log.key, log.val, commitStamp);
        DCHECK_EQ(ret, 0);
    }
    return 0;
}

int MemoryDB::Update(TxnId id, const KeyType &key, ValueType val, const TxnStamp &stamp) {
    auto iter = mStorage.find(key);
    if (iter == mStorage.end()) {
        return 1;
    }
    LogList &logList = iter->second;
    logList.AddData(TxnLog(id, key, val, stamp));
    return 0;
}


} // namespace mvcc;