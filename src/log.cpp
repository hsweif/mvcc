//
// Created by 范軒瑋 on 2020/6/12.
//

#include <fstream>
#include "log.h"
#include "database.h"

namespace mvcc {


int LogManager::Flush(const std::map<KeyType, TxnLog> &logs) {
    // TODO: Flush to the file.
    std::lock_guard<std::mutex> lockGuard(flushMutex);
    std::fstream file;
    file.open(fileName, std::ios::binary | std::ios::out | std::ios::in);
    if (!file.is_open()) {
        LOG(WARNING) << "Unable to flush the logs to the disk";
        return 1;
    }
    uint32_t offset, checkpointPos;
    file.read(reinterpret_cast<char *>(&offset), sizeof(offset));
    file.read(reinterpret_cast<char *>(&checkpointPos), sizeof(checkpointPos));
    file.seekp(offset, std::ios::beg);
    for (const auto &item: logs) {
        const KeyType &key = item.first;
        const TxnLog &log = item.second;
        uint32_t delta;
        Serialize(file, log, delta);
        offset += delta;
    }
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<const char *>(&offset), sizeof(offset)); // Update the offset.
    flushPos = offset;
    file.close();
    return 0;
}

int LogManager::ResetLogFile() {
    std::lock_guard<std::mutex> lockGuard(flushMutex);
    std::ofstream file;
    file.open(fileName, std::ios::binary | std::ios::out);
    uint32_t checkPointPos = 0, offset = 0;
    offset = sizeof(offset) + sizeof(checkPointPos);
    file.write(reinterpret_cast<const char *>(&offset), sizeof(offset));
    file.write(reinterpret_cast<const char *>(&checkPointPos), sizeof(checkPointPos));
    file.close();
    return 0;
}

int LogManager::Redo(PersistDB &database) {
    std::lock_guard<std::mutex> lockGuard(flushMutex);
    std::ifstream file;
    file.open(fileName, std::ios::binary);
    uint32_t offset, checkpointPos;
    file.read(reinterpret_cast<char *>(&offset), sizeof(offset));
    file.read(reinterpret_cast<char *>(&checkpointPos), sizeof(checkpointPos));
    file.seekg(checkpointPos, std::ios::beg);
    uint32_t curPos = checkpointPos;
    while(curPos < offset) {
        uint32_t delta;
        TxnLog txnLog;
        Deserialize(file, txnLog, delta);
        if(txnLog.op == OP::SET) {
            database.Update(txnLog.id, txnLog.key, txnLog.val, txnLog.stamp);
        }
        std::cout << curPos << ", " << txnLog << std::endl;
        curPos += delta;
    }
    return 0;
}


}
