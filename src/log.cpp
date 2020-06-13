//
// Created by 范軒瑋 on 2020/6/12.
//

#include <fstream>
#include "log.h"
#include "database.h"

namespace mvcc {


int LogManager::Flush(TxnId id, const std::map<KeyType, TxnLog> &logs, int threadIdx) {
    // TODO: Flush to the file.
    std::lock_guard<std::mutex> lockGuard(flushMutex);
    std::fstream file;
    file.open(fileName, std::ios::binary | std::ios::out | std::ios::in);
    if (!file.is_open()) {
        LOG(WARNING) << "Unable to flush the logs to the disk";
        return 1;
    }
    // file.read(reinterpret_cast<char *>(&logNum), sizeof(logNum));
    // file.read(reinterpret_cast<char *>(&checkPointPos), sizeof(checkPointPos));
    file.seekp(0, std::ios::end);
    // file.seekp(offset, std::ios::beg);
    for (const auto &item: logs) {
        const KeyType &key = item.first;
        const TxnLog &log = item.second;
        Serialize(file, log);
        // offset += delta;
    }
    file.seekp(0, std::ios::beg);
    logNum ++;
    file.write(reinterpret_cast<const char*>(&logNum), sizeof(logNum));
    UpdateTxnPos(threadIdx, id);
    // file.write(reinterpret_cast<const char *>(&offset), sizeof(offset)); // Update the offset.
    file.close();
    return 0;
}

int LogManager::ResetLogFile() {
    std::lock_guard<std::mutex> lockGuard(flushMutex);
    std::ofstream file;
    logNum = checkPointPos = 0;
    file.open(fileName, std::ios::binary | std::ios::out);
    file.write(reinterpret_cast<const char *>(&logNum), sizeof(logNum));
    file.write(reinterpret_cast<const char *>(&checkPointPos), sizeof(checkPointPos));
    file.close();
    return 0;
}

int LogManager::LoadMeta() {
    std::ifstream file;
    file.open(fileName, std::ios::binary);
    if(!file.is_open()) {
        LOG(ERROR) << "Fail to load meta information of log file";
        return 1;
    }
    file.read(reinterpret_cast<char *>(&logNum), sizeof(logNum));
    file.read(reinterpret_cast<char *>(&checkPointPos), sizeof(checkPointPos));
    file.close();
    return 0;
}

int LogManager::FlushMeta() {
    std::lock_guard<std::mutex> lockGuard(flushMutex);
    checkPointPos = logNum;
    std::fstream file;
    file.open(fileName, std::ios::binary | std::ios::in | std::ios::out);
    if(!file.is_open()) {
        LOG(ERROR) << "Fail to load meta information of log file";
        return 1;
    }
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<char *>(&logNum), sizeof(logNum));
    file.write(reinterpret_cast<char *>(&checkPointPos), sizeof(checkPointPos));
    file.close();
    return 0;
}

int LogManager::Load(std::map<KeyType, TxnLog> &logs) {
    logs.clear();
    if(logNum == 0 && checkPointPos == 0 && LoadMeta()) {
        LOG(ERROR) << "Fail to load checkpoints";
        return 1;
    }
    printf("log num: %d, checkpoint pos: %d\n", logNum, checkPointPos);
    std::fstream file;
    file.open(fileName, std::ios::binary | std::ios::in | std::ios::out);
    file.seekg(8, std::ios::beg); // Pass two int
    for(uint32_t i = 0; i < logNum; i ++){
        TxnLog txnLog;
        Deserialize(file, txnLog);
        if(i > checkPointPos) {
            const KeyType &key = txnLog.key;
            logs[key] = txnLog;
        }
        // std::cout << "TxnLog loaded: " << curPos << ", " << txnLog << std::endl;
    }
    // file.seekg(1, std::ios::beg);
    // file.write(reinterpret_cast<const char *>(&curPos), sizeof(curPos));
    return 0;
}

int LogManager::UpdateTxnPos(int threadIdx, TxnId id) {
    if(threadIdx >= threadNum || threadIdx <0 ) {
        return 1;
    }
    txnPos[threadIdx] = std::max(txnPos[threadIdx], id);
    return 0;
}

}
