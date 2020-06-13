#include "txn_manager.h"
#include "database.h"

namespace mvcc {

bool TxnLogBuffer::FindPrevRead(KeyType key, ValueType &prevRes) {
    auto iter = cacheVal.find(key);
    if (iter == cacheVal.end()) {
        return false;
    }
    prevRes = iter->second;
    return true;
}

void TxnLogBuffer::UpdateCacheVal(KeyType key, const ValueType &val) {
    cacheVal[key] = val;
}

TxnManager::TxnManager(std::shared_ptr<Database> database, int threadIdx): threadIdx(threadIdx) {
    mDatabase = std::shared_ptr<Database>(database);
}

int TxnManager::Execute(const Txn &txn, TxnResult &txnResult) {
    DCHECK(txnResult.opRes.empty());
    const TxnId &id = txn.txnId;
    txnResult.txnId = id;
    TxnLogBuffer txnLogBuffer(id);
    std::vector<TxnResult::OpRes> tmpRes;
    std::map<KeyType, TxnLog> updateLogs;
    std::vector<std::shared_ptr<std::mutex>> locks;
    std::unique_ptr<std::lock_guard<std::mutex>> lockGuard = nullptr;
    bool includeSet = false;
    for (auto &operation: txn.operations) {
        if (operation.op == OP::SET) {
            std::shared_ptr<std::mutex> dbMutex = mDatabase->RequestDbLock();
            lockGuard = std::make_unique<std::lock_guard<std::mutex>>(*dbMutex);
            includeSet = true;
            break;
        }
    }
    mDatabase->BeginTxn(id, includeSet);

    txnResult.startStamp = GetTxnStamp();
    int ret = 0;

    for (auto &operation: txn.operations) {
        TxnLog txnLog;
        auto &key = operation.key;
        if (operation.op == OP::SET) {
            ValueType value;
            if (!txnLogBuffer.FindPrevRead(key, value)) {
                mDatabase->Read(id, key, txnLog, txnResult.startStamp); // FIXME
                value = txnLog.val;
            }
            auto &mOp = operation.mathOp;
            switch (mOp) {
                case MathOp::PLUS:
                    value += operation.value;
                    break;
                case MathOp::MINUS:
                    value -= operation.value;
                    break;
                case MathOp::TIMES:
                    value *= operation.value;
                    break;
                case MathOp::DIVIDE:
                    DCHECK_NE(operation.value, 0);
                    if (operation.value == 0) {
                        LOG(ERROR) << "Invalid operation: Divide 0 in txn " << id;
                        ret = 1;
                    }
                    value /= operation.value;
                    break;
                default:
                    LOG(WARNING) << "Invalid operation";
                    ret = 1;
                    break;
            }
            TxnStamp setStamp = mvcc::GetTxnStamp();
            updateLogs[key] = TxnLog(id, key, value, setStamp);
            tmpRes.emplace_back(operation.op, key, value, setStamp);
            txnLogBuffer.UpdateCacheVal(key, value);
        } else if (operation.op == OP::READ) {
            ValueType value;
            TxnStamp readStamp = mvcc::GetTxnStamp();
            if (!txnLogBuffer.FindPrevRead(key, value)) {
                mDatabase->Read(id, key, txnLog, txnResult.startStamp);
                value = txnLog.val;
                txnLogBuffer.UpdateCacheVal(key, value);
            }
            tmpRes.emplace_back(operation.op, key, value, readStamp);
        } else {
            // Invalid operation in assignment 1;
            LOG(WARNING) << "Invalid operation in txn: " << txn.txnId;
            return 1;
        }
        if (ret) {
            return ret;
        }
    }
    txnResult.opRes = std::vector<TxnResult::OpRes>(tmpRes);
    if (lockGuard != nullptr) {
        mDatabase->Commit(id, updateLogs, txnResult.endStamp, threadIdx);
    } else {
        mDatabase->Commit();
        txnResult.endStamp = mvcc::GetTxnStamp();
    }
    return 0;
}

};