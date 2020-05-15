//
// Created by 范軒瑋 on 2020/5/14.
//

#include "lock_manager.h"


namespace mvcc {

std::shared_ptr<std::mutex> LockManager::GetLock(const KeyType &key) {
    auto iter = locks.find(key);
    if(iter == locks.end()) {
        locks[key] = std::make_shared<std::mutex>();
    }
    return locks[key];
}

}