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

    bool FindPrevRead(KeyType key, ValueType &prevRes);

    void UpdateCacheVal(KeyType key, const ValueType &val);


private:
    std::map<KeyType, ValueType> cacheVal;
};

class TxnManager {
public:
    TxnManager(std::shared_ptr<Database> database, std::shared_ptr<std::vector<TxnId>> txnOrders);
    int Execute(const Txn &txn, TxnResult &txnResult);

private:
    std::shared_ptr<Database> mDatabase;
};

}

#endif //MVCC_TXN_MANAGER_H
