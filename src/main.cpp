//
// Created by 范軒瑋 on 2020/5/13.
//

#include "fwd.h"
#include "parser.h"
#include "txn_manager.h"
#include "database.h"
#include <glog/logging.h>
#include <thread>
#include <fstream>

using namespace mvcc;

void Preparation(const std::string &fileDir, const std::string &fileName,
                 std::shared_ptr<Database> &database) {
    Parser parser(fileDir);
    std::vector<Operation> operations;
    int ret = parser.ParseOperations(fileName, operations);
    DCHECK_EQ(ret, 0);
    for (auto &operation: operations) {
        DCHECK(operation.op == OP::INSERT); // Condition for assignment 1
        if (operation.op != OP::INSERT) {
            LOG(WARNING) << "Operations except insertions appear in data preparation";
            continue;
        }
        ret = database->Insert(INSERT_NO_ID, operation.key, operation.value);
        DCHECK_EQ(ret, 0);
    }
}

void ExecuteTxns(const std::string &fileDir, int index, const std::shared_ptr<Database> &database,
                 const std::shared_ptr<std::vector<TxnId>> &txnOrders) {
    const std::string fileName = "thread_" + std::to_string(index) + ".txt";
    LOG(INFO) << "Executing txns in " << fileName;
    Parser parser = Parser(fileDir);
    std::vector<Operation> operations;
    std::vector<Txn> txns;
    int ret = parser.ParseOperations(fileName, operations);
    DCHECK_EQ(ret, 0);
    ret = parser.ParseTxns(operations, txns);
    DCHECK_EQ(ret, 0);
    std::unique_ptr<TxnManager> txnManager = std::make_unique<TxnManager>(database, txnOrders);
    std::vector<TxnResult> txnResults;
    for (Txn &txn: txns) {
        TxnResult res;
        int ret = txnManager->Execute(txn, res);
        DCHECK_EQ(ret, 0);
        LOG(INFO) << "Txn info of " << txn.txnId << " :\n" << res;

        // Output read operation info.
        if (!res.readRes.empty()) {
            std::cout << "Results of read operations of txn " << txn.txnId << " are: " << std::endl;
            for (auto &rRes: res.readRes) {
                std::cout << "  " << rRes << std::endl;
            }
        }

        txnResults.push_back(res);
    }
    LOG(INFO) << "Executed txns in " << fileName;
    std::ofstream output;
    const std::string outputName = fileDir + "output_" + std::to_string(index) + ".csv";
    output.open(outputName, std::ios::out | std::ios::trunc);
    output << "transaction_id,type,time,value" << std::endl;
    for (TxnResult &txnRes: txnResults) {
        output << txnRes;
    }
    output.close();
}

int main(int argc, char **argv) {
    // Setup glog
    google::InitGoogleLogging(argv[0]);
    FLAGS_minloglevel = 0; // level: INFO
    FLAGS_logtostderr = true;
    LOG(INFO) << "Start MVCC";
    std::string fileDir = "../data/";

    std::shared_ptr<Database> database = std::make_shared<MemoryDB>();
    std::shared_ptr<std::vector<TxnId>> commitOrder = std::make_shared<std::vector<TxnId>>();
    Preparation(fileDir, "data_prepare.txt", database);

    const int threadNum = 3;
    std::thread threads[threadNum];

    for (int i = 0; i < threadNum; i++) {
        threads[i] = std::thread(ExecuteTxns, fileDir, i + 1, database, commitOrder);
    }

    for (int i = 0; i < threadNum; i++) {
        threads[i].join();
    }

    std::shared_ptr<MemoryDB> mDB = std::dynamic_pointer_cast<MemoryDB>(database);
    std::cout << "Txn commit order is: " << std::endl;
    for(auto &id: *commitOrder) {
        std::cout << id << " ";
    }
    std::cout << std::endl;
    std::cout << *mDB;
    return 0;
}
