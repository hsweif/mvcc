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
    int Redo(PersistDB &database);

private:
    std::string fileName;
    std::mutex flushMutex;
    uint32_t flushPos; // Flush 完之后最新的在文件中的位移
};

} // namespace mvcc
#endif //MVCC_LOG_H
