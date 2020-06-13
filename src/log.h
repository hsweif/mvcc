//
// Created by 范軒瑋 on 2020/6/12.
//

#ifndef MVCC_LOG_H
#define MVCC_LOG_H

#include <utility>

#include "fwd.h"

namespace mvcc {

class LogManager {
public:
    explicit LogManager(std::string fileName, int threadNum) :
            fileName(std::move(fileName)), logNum(0), checkPointPos(0), threadNum(threadNum) {
        txnPos = std::unique_ptr<int[]>(new int[threadNum]);
        for (int i = 0; i < threadNum; i++) {
            txnPos[i] = -1;
        }
    }

    int ResetLogFile();
    int Flush(TxnId id, const std::map<KeyType, TxnLog> &logs, int threadIdx);
    int LoadMeta();
    int FlushMeta();
    int Load(std::map<KeyType, TxnLog> &logs);
    int UpdateTxnPos(int threadIdx, TxnId id);

    friend class PersistDB;

private:
    std::string fileName;
    std::mutex flushMutex;
    uint32_t logNum, checkPointPos;
    std::unique_ptr<int[]> txnPos;
    int threadNum;
};

} // namespace mvcc
#endif //MVCC_LOG_H
