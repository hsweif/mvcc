//
// Created by 范軒瑋 on 2020/5/13.
//

#ifndef MVCC_DATABASE_H
#define MVCC_DATABASE_H

#include "fwd.h"
#include <shared_mutex>
#include <utility>

namespace mvcc {

class LogList {
public:
    LogList() : curItem(0) {}

    int GetNewestLog(TxnId id, TxnLog &txnLog, const TxnStamp &readStamp) const;
    int AddData(TxnLog txnLog, int &index);
    int AddData(const TxnLog &txnLog);

private:
    int curItem;
    const static int maxItem = 128;
    TxnLog logs[maxItem];
};

class Database {
public:
    Database();
    virtual int Read(TxnId id, const KeyType &key, TxnLog &res, const TxnStamp &readStamp) const = 0;
    virtual int Insert(TxnId id, const KeyType &key, const ValueType &val) = 0;
    std::shared_ptr<std::mutex> RequestDbLock();
    void BeginTxn(TxnId id, bool includeSet);
    virtual int Commit(TxnId id, std::map<KeyType, TxnLog> &logs, TxnStamp &commitStamp) = 0;

    virtual inline void Commit() const {
        std::lock_guard<std::mutex> lockGuard(commitLock);
    }

    virtual int Update(TxnId, const KeyType &key, ValueType val, const TxnStamp &stamp) = 0;
protected:
    mutable std::mutex commitLock;
    std::shared_ptr<std::mutex> mLock;
};

class MemoryDB : public Database {
public:
    MemoryDB();
    int Read(TxnId id, const KeyType &key, TxnLog &res, const TxnStamp &readStamp) const override;
    int Insert(TxnId id, const KeyType &key, const ValueType &val) override;
    int Update(TxnId, const KeyType &key, ValueType val, const TxnStamp &stamp) override;
    int Commit(TxnId id, std::map<KeyType, TxnLog> &logs, TxnStamp &commitStamp) override;
    friend std::ostream &operator<<(std::ostream &output, const MemoryDB &momoryDB);
    int RecordNum() const {
        return mStorage.size();
    }

protected:
    std::map<KeyType, LogList> mStorage; // Save the key-value data in a STL map.
};

class PersistDB: public MemoryDB {
public:
    explicit PersistDB(std::string fileName): MemoryDB(), fileName(std::move(fileName)) {}
    int LoadSnapshot();
    int SaveSnapshot();

protected:
    std::string fileName;
};

} // namespace mvcc

#endif //MVCC_DATABASE_H
