//
// Created by 范軒瑋 on 2020/5/14.
//

#include "fwd.h"
#include "txn_manager.h"
#include <gtest/gtest.h>

namespace mvcc {

class TestTxnManager: public ::testing::Test
{
protected:
    void SetUp() override
    {
    }
};


TEST(TestTxnBuffer, TestAddLxnLog)
{
    TxnId id = 3;
    TxnLogBuffer txnLogBuffer(id);
    EXPECT_TRUE(txnLogBuffer.Empty());
    EXPECT_EQ(txnLogBuffer.id, id);
    std::list<std::shared_ptr<TxnLog>> logList;
    logList.push_back(std::make_shared<TxnLog>(id, 1, mvcc::GetTimeStamp()));
    logList.push_back(std::make_shared<TxnLog>(id, 2, mvcc::GetTimeStamp()));
    logList.push_back(std::make_shared<TxnLog>(id, 3, mvcc::GetTimeStamp()));
    for(auto &log: logList) {
        EXPECT_FALSE(log->commited);
        txnLogBuffer.AddTxnLog(log);
    }
    EXPECT_EQ(logList.size(), txnLogBuffer.Size());
    txnLogBuffer.Commit();
    for(auto &log: logList) {
        EXPECT_TRUE(log->commited);
    }
}

}
