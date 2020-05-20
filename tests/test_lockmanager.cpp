//
// Created by 范軒瑋 on 2020/5/14.
//

#include "fwd.h"
#include "lock_manager.h"
#include <gtest/gtest.h>
#include <thread>
#include <cstdlib>
#include <ctime>

namespace mvcc {


class TestLockManager : public ::testing::Test {
protected:
    void SetUp() override {
        lockManager = std::make_shared<LockManager>();
    }

    std::shared_ptr<LockManager> lockManager;
};


TEST_F(TestLockManager, TestGetLock) {
    KeyType key("test");
    std::shared_ptr<std::mutex> lock1(lockManager->GetLock(key));
    std::shared_ptr<std::mutex> lock2(lockManager->GetLock(key));
    std::shared_ptr<std::mutex> lock3(lockManager->GetLock("another"));
    EXPECT_EQ(lock1, lock2);
    EXPECT_NE(lock1, lock3);
}


}
