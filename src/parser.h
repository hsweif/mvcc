//
// Created by 范軒瑋 on 2020/5/13.
//

#ifndef MVCC_PARSER_H
#define MVCC_PARSER_H

#include "fwd.h"

namespace mvcc {

class Parser {
public:
    Parser() { LOG(WARNING) << "You should initialize the parser with a directory.";}

    Parser(const std::string &fileDir) : mFileDir(fileDir) {}
    int ParseOperations(const std::string &fileName, std::vector<Operation> &res) const;
    Operation ParseOperation(const std::string &line) const;
    int ParseTxns(const std::vector<Operation> &rawOperations, std::vector<Txn> &res) const;

private:
    std::string mFileDir;
    MathOp GetMathOp(char c) const;
};

} // namespace mvcc

#endif //MVCC_PARSER_H
