//
// Created by 范軒瑋 on 2020/6/13.
//

#include "fwd.h"
#include "log.h"
#include <fstream>
#include <gtest/gtest.h>

using namespace mvcc;

class WALogTest : public ::testing::Test {
protected:
    void SetUp() override {
        logManager = std::make_unique<LogManager>(fileName);
        logManager->ResetLogFile();
    }

    std::unique_ptr<LogManager> logManager;
    std::string fileName = "walog_test.bin";
};

TEST_F(WALogTest, TestLoadMeta) {
    EXPECT_EQ(0, logManager->LoadMeta());
    uint32_t offset = logManager->GetOffset(), checkpointPos = logManager->GetCheckpointPos();
    EXPECT_EQ(offset, 8);
    EXPECT_EQ(checkpointPos, 0);
}

TEST_F(WALogTest, TestFlush) {
    std::map<KeyType, TxnLog> logs, loadedLogs;
    logs["test"] = TxnLog(0, "test", 23, 0);
    logs["attr_a"] = TxnLog(1, "attr_a", 666, 1);
    logManager->Flush(logs);
    EXPECT_TRUE(loadedLogs.empty());
    EXPECT_EQ(0, logManager->Load(loadedLogs));
    EXPECT_EQ(logs.size(), loadedLogs.size());
    for(const auto &item: logs) {
        const KeyType &key = item.first;
        EXPECT_EQ(logs[key], loadedLogs[key]);
    }
    EXPECT_EQ(0, logManager->FlushMeta());
}