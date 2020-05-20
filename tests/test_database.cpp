//
// Created by 范軒瑋 on 2020/5/14.
//

#include "fwd.h"
#include "database.h"
#include <gtest/gtest.h>

namespace mvcc {

class TestMemoryDB : public ::testing::Test {
protected:
    void SetUp() override {
        mDb = std::make_shared<MemoryDB>();
        mDb->Insert(INSERT_NO_ID, "test", 23);
        mDb->Insert(INSERT_NO_ID, "aa", -666);
    }

    std::shared_ptr<MemoryDB> mDb;
    TxnLog res;
};

TEST_F(TestMemoryDB, TestSuccessRead) {
    TxnId txnId = 0;
    int ret = mDb->Read(txnId, "test", res, mvcc::GetTxnStamp());
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(res.committed, true);
    EXPECT_EQ(res.val, 23);
    TxnStamp stamp1, stamp2;
    stamp1 = res.stamp;
    ret = mDb->Read(txnId, "aa", res, mvcc::GetTxnStamp());
    EXPECT_EQ(ret, 0);
    stamp2 = res.stamp;
    EXPECT_EQ(res.committed, true);
    EXPECT_EQ(res.val, -666);
    EXPECT_LT(stamp1, stamp2);
    EXPECT_LT(stamp2, mvcc::GetTxnStamp());
}

TEST_F(TestMemoryDB, TestFailRead) {
    TxnId txnId = 0;
    int ret = mDb->Read(txnId, "nothing", res, mvcc::GetTxnStamp());
    EXPECT_EQ(ret, 1);
}

} // namespace mvcc