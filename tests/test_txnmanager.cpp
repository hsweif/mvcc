//
// Created by 范軒瑋 on 2020/5/14.
//

#include "fwd.h"
#include "txn_manager.h"
#include "database.h"
#include <gtest/gtest.h>

namespace mvcc {

// TODO: Test repeatable read.

class TestTxnLogBuffer : public ::testing::Test {
protected:
    void SetUp() override {
        txnLogBuffer = std::make_unique<TxnLogBuffer>(id);
    }

    const TxnId id = 0;
    std::unique_ptr<TxnLogBuffer> txnLogBuffer;
};

TEST_F(TestTxnLogBuffer, TestCache) {
    KeyType key("test");
    ValueType prevRes;
    EXPECT_FALSE(txnLogBuffer->FindPrevRead(key, prevRes));
    txnLogBuffer->UpdateCacheVal(key, 233);
    EXPECT_TRUE(txnLogBuffer->FindPrevRead(key, prevRes));
    EXPECT_EQ(prevRes, 233);
}

class TestTxnManager : public ::testing::Test {
protected:
    void SetUp() override {
        database = std::make_shared<MemoryDB>();
        txnOrders = std::make_shared<std::vector<TxnId>>();
        txnManager = std::make_shared<TxnManager>(database, txnOrders);
    }

    std::shared_ptr<Database> database;
    std::shared_ptr<TxnManager> txnManager;
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
    size_t rSize = txnResult.opRes.size();
    EXPECT_EQ(rSize, 3);
    TxnStamp txnStamp[txnResult.opRes.size() + 2];
    txnStamp[0] = txnResult.startStamp;
    txnStamp[rSize - 1] = txnResult.endStamp;
    for (size_t i = 0; i < rSize; i++) {
        txnStamp[i + 1] = txnResult.opRes[i].stamp;
    }
    for (size_t i = 1; i < rSize; i++) {
        EXPECT_LT(txnStamp[i - 1], txnStamp[i]);
    }
    EXPECT_EQ(txnResult.opRes.back().val, 240);
}


TEST_F(TestTxnManager, TestReadAfterUpdate) {

}

void ExecuteTxns(const std::shared_ptr<TxnManager> &txnManager, const std::vector<Txn> &txns,
                 std::vector<TxnResult> &txnResults) {
    for (const Txn &txn: txns) {
        TxnResult res;
        int ret = txnManager->Execute(txn, res);
        EXPECT_EQ(ret, 0);
        txnResults.push_back(res);
    }
}


}
