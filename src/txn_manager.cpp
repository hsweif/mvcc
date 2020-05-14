//
// Created by 范軒瑋 on 2020/5/14.
//

#include "txn_manager.h"
#include "database.h"

namespace mvcc {

void TxnLogBuffer::AddTxnLog(std::shared_ptr<TxnLog> &txnLog) {
    pool.push_back(std::move(std::shared_ptr<TxnLog>(txnLog)));
}

void TxnLogBuffer::Commit() const {
    for (auto &txnIter: pool) {
        txnIter->commited = true;
    }
}

TxnManager::TxnManager(std::shared_ptr<MemoryDB> &database) {
    mDatabase = std::shared_ptr<MemoryDB>(database);
}

int TxnManager::Execute(const Txn &txn, TxnResult &txnResult) {
    DCHECK(txnResult.readRes.empty());
    TxnLogBuffer txnLogBuffer(txn.txnId);
    txnResult.startStamp = GetTimeStamp();
    for(auto &operation: txn.operations)
    {
        if(operation.op == OP::SET) {

        } else if(operation.op == OP::READ) {

        } else{
            // Invalid operation in assignment 1;
            LOG(WARNING) << "Invalid operation: in txn " << txn.txnId;
        }
    }
    txnResult.endStamp = GetTimeStamp();
    return 0;
}

};