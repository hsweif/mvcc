/**
 * Forward declare the classes and define the inline functions
 * Include those headers from standard libraries
 */

#ifndef MVCC_FWD_H
#define MVCC_FWD_H

#include <string>
#include <vector>
#include <glog/logging.h>


namespace mvcc {

typedef int VAL_TYPE;

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
    std::string key;
    MathOp mathOp; // + - * /
    VAL_TYPE value;

    Operation() : op(OP::INVALID), mathOp(MathOp::INVALID) {}

    Operation(OP op, VAL_TYPE txn) : op(op), value(txn) {
        // Used for initialize the txn operation
        DCHECK(op == OP::BEGIN || op == OP::COMMIT);
    }

    Operation(OP op, std::string key, VAL_TYPE value = 0, MathOp mOp = MathOp::INVALID) :
            op(op), key(std::move(key)), value(value), mathOp(mOp) {
        if (op == OP::SET)
            DCHECK(mathOp != MathOp::INVALID);
    }
};

struct Txn {
    std::vector<Operation> operations;
    int txnId;

    Txn(int id) : txnId(id) {}

    void AddOp(const Operation &op) { operations.push_back(op); }
};

}

#endif //MVCC_FWD_H
