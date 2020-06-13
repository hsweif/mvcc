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
    uint32_t offset, checkpointPos;
    EXPECT_EQ(0, logManager->LoadMeta(offset, checkpointPos));
    EXPECT_EQ(offset, 8);
    EXPECT_EQ(checkpointPos, 0);
}

TEST_F(WALogTest, TestFlush) {
    std::map<KeyType, TxnLog> logs, loadedLogs;
    logs["test"] = TxnLog(0, "test", 23, 0);
    logManager->Flush(logs);
    uint32_t offset, checkpointPos;
    EXPECT_EQ(0, logManager->LoadMeta(offset, checkpointPos));
    printf("%d %d\n", offset, checkpointPos);
    EXPECT_TRUE(loadedLogs.empty());
    EXPECT_EQ(0, logManager->Load(loadedLogs));
    EXPECT_FALSE(loadedLogs.empty());
}