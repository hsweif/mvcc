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
        txnIter->committed = true;
    }
}

TxnManager::TxnManager(const std::shared_ptr<Database> &database,
                       const std::shared_ptr<std::vector<TxnId>> &txnOrders) {
    mDatabase = std::shared_ptr<Database>(database);
    mTxnOrders = std::shared_ptr<std::vector<TxnId>>(txnOrders);
}

int TxnManager::Execute(const Txn &txn, TxnResult &txnResult) {
    DCHECK(txnResult.readRes.empty());
    const TxnId &id = txn.txnId;
    txnResult.txnId = id;
    TxnLogBuffer txnLogBuffer(id);
    std::vector<TxnResult::ReadRes> tmpRes;
    txnResult.startStamp = GetTimeStamp();
    int ret = 0;
    for (auto &operation: txn.operations) {
        std::shared_ptr<TxnLog> txnLog;
        if (operation.op == OP::SET) {
            ret = mDatabase->Update(id, operation.key, operation.mathOp, operation.value, txnLog);
        } else if (operation.op == OP::READ) {
            ret = mDatabase->Read(id, operation.key, txnLog);
            if (ret)
                return ret;
            tmpRes.emplace_back(operation.key, txnLog->val, GetTimeStamp());
        } else {
            // Invalid operation in assignment 1;
            LOG(WARNING) << "Invalid operation: in txn " << txn.txnId;
            return 1;
        }
        if (ret) {
            return ret;
        }
        txnLogBuffer.AddTxnLog(txnLog);
    }
    txnLogBuffer.Commit();
    mTxnOrders->emplace_back(id);
    txnResult.readRes = std::vector<TxnResult::ReadRes>(tmpRes);
    txnResult.endStamp = GetTimeStamp();
    return 0;
}

};