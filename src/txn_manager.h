//
// Created by 范軒瑋 on 2020/5/14.
//

#ifndef MVCC_TXN_MANAGER_H
#define MVCC_TXN_MANAGER_H

#include "fwd.h"

namespace mvcc {

class TxnLogBuffer {
public:
    TxnId id;

    TxnLogBuffer(TxnId id) : id(id) {}

    void AddTxnLog(const KeyType &key, size_t index);

    // void Commit(std::shared_ptr<Database> database) const;

    bool Empty() const { return pool.empty(); }

    size_t Size() const { return pool.size(); }

    bool FindPrevRead(KeyType key, ValueType &prevRes);

    void UpdateCacheVal(KeyType key, const ValueType &val);


private:
    std::vector<std::pair<KeyType, size_t>> pool;
    std::map<KeyType, ValueType> cacheVal;
};

class TxnManager {
public:
    TxnManager(std::shared_ptr<Database> database, std::shared_ptr<std::vector<TxnId>> txnOrders);
    int Execute(const Txn &txn, TxnResult &txnResult);

private:
    std::shared_ptr<Database> mDatabase;
    // std::shared_ptr<std::vector<TxnId>> mTxnOrders;
};

}

#endif //MVCC_TXN_MANAGER_H
