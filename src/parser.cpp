//
// Created by 范軒瑋 on 2020/5/13.
//

#include "parser.h"
#include <fstream>

namespace mvcc {

int Parser::ParseOperations(const std::string &fileName, std::vector<Operation> &res) const {
    LOG(INFO) << "Start parsing " << fileName;
    CHECK(res.empty());
    std::ifstream file;
    std::string filePath = mFileDir + fileName;
    file.open(filePath, std::ios_base::in);
    if (!file.is_open()) {
        LOG(ERROR) << "Fail to parse. File " << filePath << " does not exsit";
        return 1;
    }
    while (!file.eof()) {
        std::string line;
        getline(file, line);
        if(line[0] == '\0' || line[0] == '\n') {
            // Skip blank lines.
            continue;
        }
        Operation operation = ParseOperation(line);
        if(operation.op == OP::INVALID) {
            LOG(ERROR) << "Fail to parse. Invalid operation";
            return 1;
        }
        res.push_back(operation);
    }
    file.close();
    LOG(INFO) << "Successfully parsed " << filePath;
    return 0;
}

Operation Parser::ParseOperation(const std::string &line) const {
    std::istringstream iss(line);
    std::string operation;
    iss >> operation;
    int txn;
    ValueType val;
    std::string key;
    if (operation == "BEGIN") {
        iss >> txn;
        return Operation(OP::BEGIN, txn);
    } else if (operation == "COMMIT") {
        iss >> txn;
        return Operation(OP::COMMIT, txn);
    } else if (operation == "READ") {
        iss >> key;
        return Operation(OP::READ, key);
    } else if (operation == "INSERT") {
        iss >> key;
        iss >> val;
        return Operation(OP::INSERT, key, val);
    } else if (operation == "SET") {
        iss >> key;
        iss >> key;
        char c;
        iss >> c;
        MathOp mOp = GetMathOp(c);
        CHECK(mOp != MathOp::INVALID);
        iss >> val;
        return Operation(OP::SET, key, val, mOp);
    } else {
        return Operation(); // INVALID
    }
}

MathOp Parser::GetMathOp(char c) const {
    if (c == '+') {
        return MathOp::PLUS;
    } else if (c == '-') {
        return MathOp::MINUS;
    } else if (c == '*') {
        return MathOp::TIMES;
    } else if (c == '/') {
        return MathOp::DIVIDE;
    } else {
        return MathOp::INVALID;
    }
}

int Parser::ParseTxns(const std::vector<Operation> &rawOperations, std::vector<Txn> &res) const {
    CHECK(res.empty());
    auto iter = rawOperations.begin();
    while(iter != rawOperations.end())
    {
        if(iter->op == OP::BEGIN)
        {
            Txn txn(iter->value); // The value of begin operation is the txn id.
            iter ++;
            while(iter->op != OP::COMMIT)
            {
                if(iter == rawOperations.end()) {
                    LOG(ERROR) << "Invalid txns. Unpaired begin-commit exist.";
                    return 1;
                }
                txn.AddOp(*iter);
                iter ++;
            }
            // Commit this txn.
            res.push_back(txn);
        }
        iter ++;
    }
    return 0;
}

} // namespace mvcc