//
// Created by 范軒瑋 on 2020/6/11.
//

#ifndef MVCC_PAGE_H
#define MVCC_PAGE_H

#include "fwd.h"
#include "fileio/FileManager.h"
#include "bufmanager/BufPageManager.h"
#include "utils/MyBitMap.h"

namespace mvcc {

struct BitMap {
    BitMap(const BufType &buf, uint32_t bufferSize, uint32_t recordNum) :
    buffer(buf), bufferSize(bufferSize), recordNum(recordNum) {}

    ~BitMap() {};
    bool Get(uint32_t index);
    void Set(uint32_t index, bool value);
    bool FindSlot(uint32_t &slot);
    void Init();

    BufType buffer;
    uint32_t bufferSize, recordNum;
};

class Page {
public:
    Page(const BufType &buf) : buffer(buf) {}

    constexpr static uint32_t MaxSize() { return PAGE_SIZE / 4; } // bytes -> uint32
    virtual const uint32_t RecordSize() = 0;

protected:
    BufType buffer;
};

class RecordPage : public Page {
public:
    explicit RecordPage(const BufType &buf);

    const uint32_t RecordSize() override {
        return keySize + valueSize;
    }

private:
    BitMap *bitMap; // No need to delete
    uint32_t maxRecordNum;
    const static int metaSize = 2; // (key size), (value size)
    const static int keySize = 2; // two uint32: 8 bytes
    const static int valueSize = 1; // a int: 4 bytes
};

class PageManager {
public:
    PageManager(BufPageManager *bufPageManager) {
        this->bufPageManager = bufPageManager;
    }


    ~PageManager() = delete;
    void SetBuffer(const BufType &buf);
private:
    BufPageManager *bufPageManager;
    BufType buffer;
};

}

#endif //MVCC_PAGE_H
