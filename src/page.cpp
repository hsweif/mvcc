//
// Created by 范軒瑋 on 2020/6/11.
//

#include <cmath>
#include <cstring>
#include "page.h"

namespace mvcc {

void PageManager::SetBuffer(const BufType &buf) {
    this->buffer = buf;
}

void BitMap::Set(uint32_t index, bool value) {
    uint32_t shift = std::floor(index / 32);
    if (shift >= bufferSize) {
        LOG(ERROR) << "Invalid bufferSize in bit map set";
    }
    uint32_t offset = index - shift * 32;
    uint32_t mask = value ? 1 << offset : 0;
    buffer[shift] = buffer[shift] | mask;
}

bool BitMap::Get(uint32_t index) {
    uint32_t shift = std::floor(index / 32);
    if (shift >= bufferSize) {
        LOG(ERROR) << "Invalid bufferSize in bit map get";
    }
    uint32_t offset = index - shift * 32;
    uint32_t mask = 1 << offset;
    return mask & buffer[shift];
}

void BitMap::Init() {
    std::memset(buffer, 0, sizeof(unsigned int) * bufferSize);
}

bool BitMap::FindSlot(uint32_t &slot) {
    for (uint32_t i = 0; i < recordNum; i++) {
        if (!Get(i)) {
            slot = i;
            return true;
        }
    }
    return false;
}


RecordPage::RecordPage(const BufType &buf) : Page(buf) {
    uint32_t recordNum = std::floor(MaxSize() / RecordSize());
    uint32_t bitMapSize = std::ceil(recordNum / 32); // How many uint32
    this->maxRecordNum = std::floor((MaxSize() - bitMapSize - metaSize) / RecordSize());
    bitMap = new BitMap(buffer + metaSize, bitMapSize, this->maxRecordNum);
}

}