/**
 * Forward declare the classes and define the inline functions
 * Include those headers from standard libraries
 */

#ifndef MVCC_FWD_H
#define MVCC_FWD_H

#include <string>
#include <vector>
#include <list>
#include <limits>
#include <memory>
#include <ctime>
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

inline TxnStamp GetTimeStamp() {
    auto now = std::chrono::high_resolution_clock::now();
    uint64_t nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    return nanos;
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
    MathOp mathOp; // + - * /
    ValueType value;

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
    std::vector<Operation> operations;
    bool commited;
    TxnId txnId;

    Txn(TxnId id) : txnId(id), commited(false) {}

    void AddOp(const Operation &op) { operations.push_back(op); }
};

struct TxnResult {
    typedef std::pair<TxnStamp, ValueType> ReadRes;
    TxnId txnId;
    std::vector<ReadRes> readRes;
    TxnStamp startStamp, endStamp;
};

struct TxnLog {
    TxnId id;
    ValueType val;
    TxnStamp stamp;
    bool commited;

    TxnLog(TxnId id, ValueType val, TxnStamp stamp, bool commited = false) :
            id(id), val(val), stamp(stamp), commited(commited) {}
};

class Parser;

class Database;

class MemoryDB;

class TxnLogBuffer;

}

#endif //MVCC_FWD_H
