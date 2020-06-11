//
// Created by 范軒瑋 on 2020/5/13.
//

#include "fwd.h"
#include "parser.h"
#include <gtest/gtest.h>

namespace mvcc {

class ParserTest : public ::testing::Test {
protected:
    void SetUp() override {
        dataPath = "../judge/";
        parser = std::make_unique<Parser>(dataPath);
    }

    std::unique_ptr<Parser> parser;
    std::string dataPath;
};

TEST_F(ParserTest, SuccessParse) {
    std::vector<Operation> result;
    int res = parser->ParseOperations("thread_1.txt", result);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(result.size(), 8); // 8 lines
    std::vector<Txn> txns;
    res = parser->ParseTxns(result, txns);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(txns.size(), 2); // 2 txns
    auto txn1Ops = txns[0].operations;
    EXPECT_EQ(txn1Ops[0].op, OP::SET);
    EXPECT_EQ(txn1Ops[1].op, OP::SET);
}

TEST_F(ParserTest, FailParse) {
    std::vector<Operation> result;
    int res = parser->ParseOperations("file_doesnt_exist.txt", result);
    EXPECT_EQ(res, 1);
    EXPECT_TRUE(result.empty());
}

}
