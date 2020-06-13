//
// Created by 范軒瑋 on 2020/6/12.
//

#ifndef MVCC_LOG_H
#define MVCC_LOG_H

#include "fwd.h"

namespace mvcc {

class LogManager {
public:
    explicit LogManager(const std::string &fileName) : fileName(fileName) {}

    int ResetLogFile();
    int Flush(const std::map<KeyType, TxnLog> &logs);
    int Redo(PersistDB *database);
    int LoadMeta(uint32_t &offset, uint32_t &checkpointPos);
    int Load(std::map<KeyType, TxnLog> &logs);

private:
    std::string fileName;
    std::mutex flushMutex;
};

} // namespace mvcc
#endif //MVCC_LOG_H
