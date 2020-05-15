//
// Created by 范軒瑋 on 2020/5/14.
//

#include "fwd.h"
#include "txn_manager.h"
#include "database.h"
#include <gtest/gtest.h>

namespace mvcc {

class TestTxnManager : public ::testing::Test {
protected:
    void SetUp() override {
        database = std::make_shared<MemoryDB>();
        txnOrders = std::make_shared<std::vector<TxnId>>();
        txnManager = std::make_unique<TxnManager>(database, txnOrders);
    }

    std::shared_ptr<Database> database;
    std::unique_ptr<TxnManager> txnManager;
    std::shared_ptr<std::vector<TxnId>> txnOrders;
};


TEST_F(TestTxnManager, TestStampOrder) {
    Txn txn(1);
    const std::string testKey = "test";
    int ret = database->Insert(INSERT_NO_ID, testKey, 233);
    EXPECT_EQ(ret, 0);
    txn.operations = std::vector<Operation>{
            Operation(OP::READ, testKey),
            Operation(OP::SET, testKey, 7, MathOp::PLUS),
            Operation(OP::READ, testKey),
    };
    TxnResult txnResult;
    ret = txnManager->Execute(txn, txnResult);
    EXPECT_EQ(ret, 0);
    size_t rSize = txnResult.readRes.size();
    EXPECT_EQ(rSize, 2);
    TxnStamp txnStamp[txnResult.readRes.size() + 2];
    txnStamp[0] = txnResult.startStamp;
    txnStamp[rSize - 1] = txnResult.endStamp;
    for (size_t i = 0; i < rSize; i++) {
        txnStamp[i + 1] = txnResult.readRes[i].stamp;
    }
    for (size_t i = 1; i < rSize; i++) {
        EXPECT_LT(txnStamp[i - 1], txnStamp[i]);
    }
    EXPECT_EQ(txnResult.readRes.back().val, 240);
}


TEST(TestTxnBuffer, TestAddLxnLog) {
    TxnId id = 3;
    TxnLogBuffer txnLogBuffer(id);
    EXPECT_TRUE(txnLogBuffer.Empty());
    EXPECT_EQ(txnLogBuffer.id, id);
    std::list<std::shared_ptr<TxnLog>> logList;
    logList.push_back(std::make_shared<TxnLog>(id, 1, mvcc::GetTimeStamp()));
    logList.push_back(std::make_shared<TxnLog>(id, 2, mvcc::GetTimeStamp()));
    logList.push_back(std::make_shared<TxnLog>(id, 3, mvcc::GetTimeStamp()));
    for (auto &log: logList) {
        EXPECT_FALSE(log->committed);
        txnLogBuffer.AddTxnLog(log);
    }
    EXPECT_EQ(logList.size(), txnLogBuffer.Size());
    txnLogBuffer.Commit();
    for (auto &log: logList) {
        EXPECT_TRUE(log->committed);
    }
}

}
