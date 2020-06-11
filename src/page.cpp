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
    if(shift >= size) {
        LOG(ERROR) << "Invalid size in bit map set";
    }
    uint32_t offset = index - shift * 32;
    uint32_t mask = value ? 1 << offset : 0;
    buffer[shift] = buffer[shift] | mask;
}

bool BitMap::Get(uint32_t index) {
    uint32_t shift = std::floor(index / 32);
    if(shift >= size) {
        LOG(ERROR) << "Invalid size in bit map get";
    }
    uint32_t offset = index - shift * 32;
    uint32_t mask = 1 << offset;
    return mask & buffer[shift];
}

void BitMap::Init() {
    std::memset(buffer, 0, sizeof(unsigned int) * size);
}

}