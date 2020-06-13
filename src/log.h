//
// Created by 范軒瑋 on 2020/6/12.
//

#ifndef MVCC_LOG_H
#define MVCC_LOG_H

#include "fwd.h"

namespace mvcc {

class LogManager {
public:
    explicit LogManager(const std::string &fileName) :
            fileName(fileName), logNum(0), checkPointPos(0) {}

    int ResetLogFile();
    int Flush(const std::map<KeyType, TxnLog> &logs);
    int LoadMeta();
    int Load(std::map<KeyType, TxnLog> &logs);

private:
    std::string fileName;
    std::mutex flushMutex;
    uint32_t logNum, checkPointPos;
};

} // namespace mvcc
#endif //MVCC_LOG_H
