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

void KeyCollision(std::shared_ptr<std::mutex> lock, int index,
                  const std::shared_ptr<std::vector<int>> order) {
    std::lock_guard<std::mutex> lockGuard(*lock);
    sleep(rand() % 100 + 1);
    order->push_back(index);
}

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

// TEST_F(TestLockManager, TestLockCollide) {
//     KeyType key("test");
//     srand(time(NULL));
//     const int threadNum = 4;
//     std::thread threads[threadNum];
//     std::vector<int> expectedOrder;
//     std::shared_ptr<std::vector<int>> exeOrder = std::make_shared<std::vector<int>>();
//     for (int i = 0; i < threadNum; i++) {
//         threads[i] = std::thread(KeyCollision, lockManager->GetLock(key), i, exeOrder);
//         expectedOrder.push_back(i);
//     }
//     for(int i = 0; i < threadNum; i ++) {
//         threads[i].join();
//     }
//     EXPECT_EQ(expectedOrder, *exeOrder);
// }

}
