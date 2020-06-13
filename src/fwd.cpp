//
// Created by 范軒瑋 on 2020/6/13.
//

#include "fwd.h"

namespace mvcc {

TxnStamp curStamp = 0;

TxnStamp GetTxnStamp() {
    std::lock_guard<std::mutex> lockGuard(timeStampLock);
    TxnStamp stamp = curStamp++;
    return stamp;
}

void InitTxnStamp(TxnStamp prevStamp) {
    curStamp = prevStamp;
}

}