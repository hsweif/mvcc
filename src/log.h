//
// Created by 范軒瑋 on 2020/6/12.
//

#ifndef MVCC_LOG_H
#define MVCC_LOG_H

#include "fwd.h"

namespace mvcc {

class LogManager {
public:
    explicit LogManager(std::string fileName) : fileName(std::move(fileName)), offset(0), checkPointPos(0) {}

    int ResetLogFile();
    int Flush(const std::map<KeyType, TxnLog> &logs);
    int LoadMeta();
    int FlushMeta() const;
    int UpdateCheckpoint();
    int Load(std::map<KeyType, TxnLog> &logs);
    uint32_t GetOffset() const;
    uint32_t GetCheckpointPos() const;

private:
    std::string fileName;
    mutable std::mutex flushMutex;
    uint32_t offset, checkPointPos;
};

} // namespace mvcc
#endif //MVCC_LOG_H
