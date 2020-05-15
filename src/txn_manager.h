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

    void AddTxnLog(std::shared_ptr<TxnLog> &txnLog);

    void Commit() const;

    bool Empty() const { return pool.empty(); }

    size_t Size() const { return pool.size(); }

private:
    std::vector<std::shared_ptr<TxnLog>> pool;
};

class TxnManager {
public:
    TxnManager(const std::shared_ptr<Database> &database, const std::shared_ptr<std::vector<TxnId>> &txnOrders);
    int Execute(const Txn &txn, TxnResult &txnResult);

private:
    std::shared_ptr<Database> mDatabase;
    std::shared_ptr<std::vector<TxnId>> mTxnOrders;
};

}

#endif //MVCC_TXN_MANAGER_H
