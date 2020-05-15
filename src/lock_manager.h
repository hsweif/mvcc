//
// Created by 范軒瑋 on 2020/5/14.
//

#ifndef MVCC_LOCK_MANAGER_H
#define MVCC_LOCK_MANAGER_H

#include "fwd.h"
#include <mutex>

namespace mvcc {

class LockManager {
public:
    std::shared_ptr<std::mutex> GetLock(const KeyType &key);
private:
    std::map<KeyType, std::shared_ptr<std::mutex>> locks;
};

}

#endif //MVCC_LOCK_MANAGER_H
