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
    EXPECT_EQ(res.val, 23);
    TxnStamp stamp1, stamp2;
    stamp1 = res.stamp;
    ret = mDb->Read(txnId, "aa", res, mvcc::GetTxnStamp());
    EXPECT_EQ(ret, 0);
    stamp2 = res.stamp;
    EXPECT_EQ(res.val, -666);
    EXPECT_LT(stamp1, stamp2);
    EXPECT_LT(stamp2, mvcc::GetTxnStamp());
}

TEST_F(TestMemoryDB, TestFailRead) {
    TxnId txnId = 0;
    int ret = mDb->Read(txnId, "nothing", res, mvcc::GetTxnStamp());
    EXPECT_EQ(ret, 1);
}

class PersistDBTest: public ::testing::Test {
protected:
    void SetUp() override {
        database = std::make_unique<PersistDB>(dbName, logName);
        database->Reset();
        database->Insert(INSERT_NO_ID, "test", 23);
    }

    std::unique_ptr<PersistDB> database;
    const std::string dbName = "unittestdb.bin";
    const std::string logName = "unittestlog.bin";
};

TEST_F(PersistDBTest, TestIO) {
    EXPECT_EQ(database->SaveSnapshot(), 0);
    EXPECT_EQ(database->LoadSnapshot(), 0);
    auto testDb = std::make_unique<PersistDB>(dbName, logName);
    EXPECT_EQ(testDb->RecordNum(), 0);
    EXPECT_EQ(testDb->LoadSnapshot(), 0);
    EXPECT_EQ(testDb->RecordNum(), database->RecordNum());
}

TEST_F(PersistDBTest, TestRecovery) {
    auto recoverDB = std::make_unique<PersistDB>(dbName, logName);
    const KeyType tmpKey = "test";
    std::map<KeyType, TxnLog> logs;
    logs[tmpKey] = TxnLog(0, tmpKey, 33, 0);
    EXPECT_EQ(0, database->Commit(0, logs, 1));
    TxnLog res, reRes;
    TxnStamp stamp = 2;
    TxnId tmpId = 3;
    EXPECT_EQ(0, database->Read(tmpId, tmpKey, res, stamp));
    EXPECT_EQ(res.val, 33);
    EXPECT_EQ(1, recoverDB->Read(tmpId, tmpKey, reRes, stamp));
    EXPECT_EQ(0, recoverDB->LoadSnapshot());
    EXPECT_EQ(1, recoverDB->Read(tmpId, tmpKey, reRes, stamp));
    EXPECT_EQ(0, recoverDB->Redo());
    EXPECT_EQ(0, recoverDB->Read(tmpId, tmpKey, reRes, stamp));
    EXPECT_EQ(reRes.val, 33);
    EXPECT_EQ(0, database->SaveSnapshot());
}

} // namespace mvcc