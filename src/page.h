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
    BitMap(const BufType &buf, uint32_t size): buffer(buf), size(size) {}
    ~BitMap() {};
    bool Get(uint32_t index);
    void Set(uint32_t index, bool value);
    void Init();

    BufType buffer;
    uint32_t size;
};

class Page {
public:
    Page(const BufType &buf): buffer(buf) {}
    constexpr static uint32_t MaxSize() { return PAGE_SIZE; }

protected:
    BufType buffer;
};

class RecordPage: public Page{
public:
private:
    const static int keySize = 8; // 8 bytes
    const static int valueSize = 4; // int: 4 bytes
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
