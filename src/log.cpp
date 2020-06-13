//
// Created by 范軒瑋 on 2020/6/12.
//

#include <fstream>
#include "log.h"
#include "database.h"

namespace mvcc {


int LogManager::Flush(const std::map<KeyType, TxnLog> &logs) {
    std::lock_guard<std::mutex> lockGuard(flushMutex);
    DCHECK_NE(0, offset);
    std::fstream file;
    file.open(fileName, std::ios::binary | std::ios::out | std::ios::in);
    if (!file.is_open()) {
        LOG(WARNING) << "Unable to flush the logs to the disk";
        return 1;
    }
    file.seekp(offset, std::ios::beg);
    for (const auto &item: logs) {
        const KeyType &key = item.first;
        const TxnLog &log = item.second;
        uint32_t delta;
        Serialize(file, log, delta);
        offset += delta;
    }
    // Update the offset.
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<const char *>(&offset), sizeof(offset));
    file.close();
    return 0;
}

int LogManager::ResetLogFile() {
    std::lock_guard<std::mutex> lockGuard(flushMutex);
    std::ofstream file;
    file.open(fileName, std::ios::binary | std::ios::out);
    checkPointPos = 0;
    offset = sizeof(offset) + sizeof(checkPointPos);
    file.write(reinterpret_cast<const char *>(&offset), sizeof(offset));
    file.write(reinterpret_cast<const char *>(&checkPointPos), sizeof(checkPointPos));
    file.close();
    return 0;
}

int LogManager::LoadMeta() {
    // Should not add lock guard, as it will be called by other function with a mutex
    std::ifstream file;
    file.open(fileName, std::ios::binary);
    if (!file.is_open()) {
        LOG(ERROR) << "Fail to load meta information of log file";
        return 1;
    }
    file.read(reinterpret_cast<char *>(&offset), sizeof(offset));
    file.read(reinterpret_cast<char *>(&checkPointPos), sizeof(checkPointPos));
    file.close();
    return 0;
}

int LogManager::Load(std::map<KeyType, TxnLog> &logs) {
    std::lock_guard<std::mutex> lockGuard(flushMutex);
    logs.clear();
    if (LoadMeta()) {
        LOG(ERROR) << "Fail to load checkpoints";
        return 1;
    }
    std::fstream file;
    file.open(fileName, std::ios::binary | std::ios::in | std::ios::out);
    uint32_t curPos = checkPointPos == 0 ? 8 : checkPointPos;
    file.seekg(curPos, std::ios::beg);
    while (curPos < offset) {
        uint32_t delta;
        TxnLog txnLog;
        Deserialize(file, txnLog, delta);
        const KeyType &key = txnLog.key;
        logs[key] = txnLog;
        std::cout << "TxnLog loaded: " << curPos << ", " << txnLog << std::endl;
        curPos += delta;
    }
    checkPointPos = curPos;
    return 0;
}

int LogManager::FlushMeta() const {
    std::lock_guard<std::mutex> lockGuard(flushMutex);
    std::fstream file;
    file.open(fileName, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open()) {
        return 1;
    }
    file.write(reinterpret_cast<const char *>(&offset), sizeof(offset));
    file.write(reinterpret_cast<const char *>(&checkPointPos), sizeof(checkPointPos));
    file.close();
    return 0;
}

int LogManager::UpdateCheckpoint() {
    checkPointPos = offset;
    return 0;
}

uint32_t LogManager::GetOffset() const { return offset; }

uint32_t LogManager::GetCheckpointPos() const { return checkPointPos; }

}
