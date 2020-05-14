//
// Created by 范軒瑋 on 2020/5/13.
//

#ifndef MVCC_DATABASE_H
#define MVCC_DATABASE_H

#include "fwd.h"
#include <map>
#include <list>

namespace mvcc {


class Database {
public:
    virtual int Read(TxnId id, const KeyType &key, std::shared_ptr<TxnLog> &res) const = 0;
    virtual int Insert(TxnId id, const KeyType &key, const ValueType &val) = 0;
    virtual int Update(TxnId id, const KeyType &key, MathOp mOP,
                       const ValueType &val, std::shared_ptr<TxnLog> &res) = 0;
};

class MemoryDB : public Database {
public:
    MemoryDB() = default;

    typedef std::list<std::shared_ptr<TxnLog>> LogList;
    int Read(TxnId id, const KeyType &key, std::shared_ptr<TxnLog> &res) const override;
    int Insert(TxnId id, const KeyType &key, const ValueType &val) override;
    int Update(TxnId id, const KeyType &key, MathOp mOP,
               const ValueType &val, std::shared_ptr<TxnLog> &res) override;

private:
    std::map<KeyType, LogList> mStorage; // Save the key-value data in a STL map.

};

} // namespace mvcc

#endif //MVCC_DATABASE_H
