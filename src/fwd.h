/**
 * Forward declare the classes and define the inline functions
 * Include those headers from standard libraries
 */

#ifndef MVCC_FWD_H
#define MVCC_FWD_H

#include <string>
#include <utility>
#include <vector>
#include <list>
#include <map>
#include <limits>
#include <memory>
#include <ctime>
#include <mutex>
#include <chrono>
#include <iostream>
#include <glog/logging.h>


namespace mvcc {


typedef int ValueType;
typedef std::string KeyType;
typedef uint32_t TxnId;
typedef uint64_t TxnStamp;

// Used for initialization in assignment 1
const static TxnId INSERT_NO_ID = std::numeric_limits<TxnId>::max();
const static TxnId INVALID_ID = std::numeric_limits<TxnId>::max() - 1;

static std::mutex timeStampLock;
static TxnStamp curStamp = 0;

inline TxnStamp GetTxnStamp() {
    std::lock_guard<std::mutex> lockGuard(timeStampLock);
    TxnStamp stamp = curStamp ++;
    return stamp;
}

// TODO: Add abort in assignment 3?
enum class OP {
    READ,
    BEGIN,
    COMMIT,
    SET,
    INSERT,
    INVALID
};

enum class MathOp {
    PLUS,
    MINUS,
    TIMES,
    DIVIDE,
    INVALID
};

struct Operation {
    OP op;
    KeyType key;
    ValueType value;
    MathOp mathOp; // + - * /

    Operation() : op(OP::INVALID), mathOp(MathOp::INVALID) {}

    Operation(OP op, ValueType txn) : op(op), value(txn) {
        // Used for initialize the txn operation
        DCHECK(op == OP::BEGIN || op == OP::COMMIT);
    }

    Operation(OP op, KeyType key, ValueType value = 0, MathOp mOp = MathOp::INVALID) :
            op(op), key(std::move(key)), value(value), mathOp(mOp) {
        if (op == OP::SET)
            DCHECK(mathOp != MathOp::INVALID);
    }
};

struct Txn {
    TxnId txnId;
    std::vector<Operation> operations;

    explicit Txn(TxnId id) : txnId(id) {}

    Txn(TxnId id, std::vector<Operation> &operations) : txnId(id), operations(operations) {}

    void AddOp(const Operation &op) { operations.push_back(op); }
};


struct TxnResult {
    struct OpRes {
        OP op;
        KeyType key;
        ValueType val;
        TxnStamp stamp;

        OpRes(OP op, KeyType key, ValueType val, TxnStamp stamp) :
                op(op), key(std::move(key)), val(val), stamp(stamp) {}
    };

    TxnId txnId;
    std::vector<OpRes> opRes;
    TxnStamp startStamp, endStamp;
};

inline std::ostream &operator<<(std::ostream &output, const TxnResult::OpRes &opRes) {
    output << opRes.key << "'s value is " << opRes.val
           << ". Operation's timestamp is " << opRes.stamp;
}

inline std::ostream &operator<<(std::ostream &output, const TxnResult &txnResult) {
    output << txnResult.txnId << ",BEGIN," << txnResult.startStamp << "," << std::endl;
    for (auto &res: txnResult.opRes) {
        if (res.op == OP::READ) {
            // Only care READ time stamp while using.
            output << txnResult.txnId << "," << res.key << "," << res.stamp << "," << res.val << std::endl;
        }
    }
    output << txnResult.txnId << ",END," << txnResult.endStamp << "," << std::endl;
    return output;
}

struct TxnLog {
    TxnId id;
    KeyType key;
    ValueType val;
    TxnStamp stamp;
    bool committed;

    TxnLog(const TxnLog &txnLog) {
        id = txnLog.id;
        key = txnLog.key;
        val = txnLog.val;
        stamp = txnLog.stamp;
        committed = txnLog.committed;
    }

    TxnLog() : id(INVALID_ID), committed(false) {}

    TxnLog(TxnId id, KeyType key, ValueType val, TxnStamp stamp, bool committed = false) :
            id(id), key(key), val(val), stamp(stamp), committed(committed) {}

};

class Parser;

class Database;

class MemoryDB;

class TxnLogBuffer;

}

#endif //MVCC_FWD_H
