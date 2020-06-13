//
// Created by 范軒瑋 on 2020/5/13.
//

#include "database.h"
#include "fstream"
#include "log.h"

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
    const TxnStamp &logStamp = logs[curIndex].stamp;
    while (logs[curIndex].id != INSERT_NO_ID && readStamp < logStamp) {
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
    TxnStamp stamp = mvcc::GetTxnStamp();
    return Insert(id, key, val, stamp);
}

int MemoryDB::Insert(TxnId id, const KeyType &key, const ValueType &val, TxnStamp stamp) {
    auto iter = mStorage.find(key);
    int index;
    if (iter == mStorage.end()) {
        // Not existed in the database before.
        mStorage[key] = LogList();
        TxnLog txnLog(id, key, val, stamp);
        mStorage[key].AddData(txnLog, index);
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
        int ret = logList.GetNewestLog(INSERT_NO_ID, readLog, mvcc::GetTxnStamp());
        DCHECK_EQ(ret, 0);
        output << "  " << key << ": " << readLog.val << ", by txn_id " << readLog.id << ", at " << readLog.stamp
               << std::endl;
    }
    return output;
}


int MemoryDB::Commit(TxnId id, std::map<KeyType, TxnLog> &logs, TxnStamp &commitStamp, int threadIdx) {
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


PersistDB::PersistDB(std::string fileName, std::string logName, int threadNum) :
        fileName(std::move(fileName)), threadNum(threadNum), commitCount(0) {
    logManager = std::make_unique<LogManager>(logName, threadNum);
    executedId = std::unique_ptr<int[]>(new int[threadNum]);
    for(int i = 0; i < threadNum; i ++) {
        executedId[i] = -1;
    }
}

int PersistDB::SaveSnapshot() {
    /**
     * Snapshot format (a single record)
     * uint64 stamp | uint32 id | uint32 key_length | (key_length bytes) key | int value
     */
    std::ofstream file;
    file.open(fileName, std::ios_base::binary);
    if (!file.is_open()) {
        LOG(ERROR) << "Fail to open file to save the snapshot.";
        return 1;
    }
    const auto saveStamp = mvcc::GetTxnStamp();
    const uint32_t recordNum = mStorage.size();
    file.write(reinterpret_cast<const char *>(&recordNum), sizeof(recordNum));
    file.write(reinterpret_cast<const char *>(&saveStamp), sizeof(saveStamp));
    file.write(reinterpret_cast<const char *>(&logManager->threadNum), sizeof(logManager->threadNum));
    for (int i = 0; i < logManager->threadNum; i++) {
        // Use logManager's txnPos to save. But use executedId to load.
        file.write(reinterpret_cast<const char *>(&logManager->txnPos[i]), sizeof(logManager->txnPos[i]));
    }
    for (const auto &item: mStorage) {
        const KeyType &key = item.first;
        const LogList &logList = item.second;
        TxnLog resLog;
        logList.GetNewestLog(INSERT_NO_ID, resLog, saveStamp);
        uint32_t keyLength = key.size();
        auto c = key.c_str();
        file.write(reinterpret_cast<const char *>(&resLog.stamp), sizeof(resLog.stamp));
        file.write(reinterpret_cast<const char *>(&resLog.id), sizeof(resLog.id));
        file.write(reinterpret_cast<const char *>(&keyLength), sizeof(keyLength));
        file.write(c, keyLength);
        file.write(reinterpret_cast<const char *>(&resLog.val), sizeof(resLog.val));
    }
    logManager->FlushMeta();
    file.close();
    return 0;
}

int PersistDB::LoadSnapshot() {
    std::lock_guard<std::mutex> lockGuard(this->commitLock);
    std::ifstream file;
    file.open(fileName, std::ios_base::binary);
    if (!file.is_open()) {
        LOG(WARNING) << "Unable to open file to load snapshot";
        return 1;
    }
    uint32_t recordNum;
    TxnStamp cacheStamp;
    file.read(reinterpret_cast<char *>(&recordNum), sizeof(recordNum));
    file.read(reinterpret_cast<char *>(&cacheStamp), sizeof(cacheStamp));
    file.read(reinterpret_cast<char *>(&threadNum), sizeof(threadNum));
    logManager->threadNum = threadNum;
    for (int i = 0; i < logManager->threadNum; i++) {
        file.read(reinterpret_cast<char *>(&executedId[i]), sizeof(executedId[i]));
    }
    InitTxnStamp(cacheStamp);
    for (uint32_t i = 0; i < recordNum; i++) {
        TxnStamp stamp;
        TxnId id;
        uint32_t keyLength;
        ValueType value;
        file.read(reinterpret_cast<char *>(&stamp), sizeof(stamp));
        file.read(reinterpret_cast<char *>(&id), sizeof(id));
        file.read(reinterpret_cast<char *>(&keyLength), sizeof(keyLength));
        char c[keyLength];
        file.read(c, keyLength);
        KeyType key;
        for (uint32_t k = 0; k < keyLength; k++) {
            key += c[k];
        }
        file.read(reinterpret_cast<char *>(&value), sizeof(value));
        Insert(id, key, value, stamp);
    }
    file.close();
    Redo();
    return 0;
}

int PersistDB::Commit(TxnId id, std::map<KeyType, TxnLog> &logs, TxnStamp &commitStamp, int threadIdx) {
    // TODO: persist db commit
    CheckSave();
    std::lock_guard<std::mutex> lockGuard(this->commitLock);
    int flushRes = logManager->Flush(id, logs, threadIdx);
    DCHECK_EQ(flushRes, 0);
    commitStamp = mvcc::GetTxnStamp();
    for (auto &logItem: logs) {
        auto &log = logItem.second;
        int ret = this->Update(log.id, log.key, log.val, commitStamp);
        DCHECK_EQ(ret, 0);
    }
    return 0;
}

int PersistDB::Redo() {
    std::map<KeyType, TxnLog> logs;
    int loadRet = logManager->Load(logs);
    if (loadRet) {
        LOG(WARNING) << "Unable to redo from the log.";
        return 1;
    }
    for (const auto &item: logs) {
        std::cout << "Redo..." << std::endl;
        const KeyType &key = item.first;
        const TxnLog &log = item.second;
        if (Update(log.id, log.key, log.val, log.stamp)) {
            Insert(log.id, log.key, log.val, log.stamp);
        }
    }
    return 0;
}

int PersistDB::CheckSave() {
    std::lock_guard<std::mutex> lockGuard(commitLock);
    commitCount ++;
    if(commitCount > 1000) {
        commitCount = 0;
        return SaveSnapshot();
    }
    return 0;
}


} // namespace mvcc;