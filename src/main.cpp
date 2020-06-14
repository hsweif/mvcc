//
// Created by 范軒瑋 on 2020/5/13.
//

#include "fwd.h"
#include "parser.h"
#include "txn_manager.h"
#include "database.h"
#include "page.h"
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

void ExecuteTxns(const std::string &fileDir, int threadIdx, const std::shared_ptr<Database> database) {
    int index = threadIdx + 1;
    const std::string fileName = "thread_" + std::to_string(index) + ".txt";
    LOG(INFO) << "Executing txns in " << fileName;
    Parser parser = Parser(fileDir);
    std::vector<Operation> operations;
    std::vector<Txn> txns;
    int ret = parser.ParseOperations(fileName, operations);
    DCHECK_EQ(ret, 0);
    ret = parser.ParseTxns(operations, txns);
    DCHECK_EQ(ret, 0);
    std::unique_ptr<TxnManager> txnManager = std::make_unique<TxnManager>(database, threadIdx);
    std::vector<TxnResult> txnResults;
    printf("Thread %d begin first txn at%llu\n", index, GetClock());
    for (Txn &txn: txns) {
        if(database->Executed(threadIdx, txn.txnId)) {
            continue;
        }
        TxnResult res;
        int ret = txnManager->Execute(txn, res);
        DCHECK_EQ(ret, 0);
        LOG(INFO) << "Txn info of " << txn.txnId << " :\n" << res;
        // Output read operation info.
        // if (!res.opRes.empty()) {
        //     std::cout << "Results of read operations of txn " << txn.txnId << " are: ";
        //     for (auto &rRes: res.opRes) {
        //         std::cout << "  " << rRes << std::endl;
        //     }
        // }
        txnResults.push_back(res);
    }
    printf("Thread %d commit last txn at%llu\n", index, GetClock());
    std::shared_ptr<MemoryDB> mDB = std::dynamic_pointer_cast<MemoryDB>(database);
    printf("----Database status of thread %d----\n", index);
    std::cout << *mDB << std::endl;


    LOG(INFO) << "Executed txns in " << fileName;
    std::ofstream output;
    const std::string outputName = fileDir + "output_thread_" + std::to_string(index) + ".csv";
    output.open(outputName, std::ios::out | std::ios::trunc);
    output << "transaction_id,type,time,value" << std::endl;
    for (TxnResult &txnRes: txnResults) {
        output << txnRes;
    }
    output.close();
}

void TestMVCC() {
    auto beginStamp = GetClock();

    LOG(INFO) << "Start MVCC";
    std::string fileDir = "../judge/";
    std::shared_ptr<Database> database = std::make_shared<MemoryDB>();
    Preparation(fileDir, "data_prepare.txt", database);

    auto prepareStamp = GetClock();

    const int threadNum = 4;
    std::thread threads[threadNum];


    for (int i = 0; i < threadNum; i++) {
        threads[i] = std::thread(ExecuteTxns, fileDir, i, database);
    }

    for (int i = 0; i < threadNum; i++) {
        threads[i].join();
    }

    auto endStamp = GetClock();
    std::shared_ptr<MemoryDB> mDB = std::dynamic_pointer_cast<MemoryDB>(database);
    std::cout << std::endl;
    std::cout << "-----------Final database status-------------" << std::endl;
    std::cout << *mDB;
    std::cout << "- - - - - Time Spent - - - - - " << std::endl;
    std::cout << "Preparation: " << (prepareStamp - beginStamp) << " ms" << std::endl;
    std::cout << "Executed all transactions: " << (endStamp - prepareStamp) << " ms" << std::endl;
}

void TestPersistDB(bool redoFlag) {
    std::string fileDir = "../judge/";
    const int threadNum = 4;
    auto database = std::make_shared<PersistDB>(fileDir + "testdb.bin", fileDir + "log.bin", threadNum);
    if(redoFlag) {
        database->LoadSnapshot();
        std::cout << *database;
    }
    else {
        database->ResetLog();
        std::shared_ptr<Database> base = std::dynamic_pointer_cast<Database>(database);
        Preparation(fileDir, "data_prepare.txt", base);
    }
    std::thread threads[threadNum];
    for (int i = 0; i < threadNum; i++) {
        threads[i] = std::thread(ExecuteTxns, fileDir, i, database);
    }
    for (int i = 0; i < threadNum; i++) {
        threads[i].join();
    }
    database->SaveSnapshot();
    std::cout << *database;
}

int main(int argc, char **argv) {
    // Setup glog
    google::InitGoogleLogging(argv[0]);
    FLAGS_minloglevel = 1; // level: INFO
    FLAGS_logtostderr = true;
    bool redoFlag = false;
    if(argc > 1) {
        if(std::string(argv[1]) == "redo") {
            redoFlag = true;
        }
    }
    TestPersistDB(redoFlag);
    return 0;
}
