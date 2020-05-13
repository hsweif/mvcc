//
// Created by 范軒瑋 on 2020/5/13.
//

#include <cstdio>
#include <glog/logging.h>


int main(int argc, char **argv)
{
    // Setup glog
    google::InitGoogleLogging(argv[0]);
    FLAGS_minloglevel = 0; // level: INFO
    FLAGS_logtostderr = true;
    LOG(INFO) << "Start MVCC";
    return 0;
}
