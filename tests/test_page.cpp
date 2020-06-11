//
// Created by 范軒瑋 on 2020/6/11.
//

#include "fwd.h"
#include "page.h"
#include <gtest/gtest.h>

using namespace mvcc;

class BitMapTest : public ::testing::Test {
protected:
    void SetUp() override {
        uint32_t bufSize = 64;
        BufType buffer = new unsigned int[bufSize];
        bitMap = std::make_unique<BitMap>(buffer, bufSize);
    }

    void TearDown() override {
        delete (bitMap->buffer);
    }

    std::unique_ptr<BitMap> bitMap;
};

TEST_F(BitMapTest, TestInit) {
    for (int i = 0; i < bitMap->size; i++) {
        bitMap->buffer[i] = 1;
    }
    bitMap->Init();
    for (int i = 0; i < bitMap->size; i++) {
        EXPECT_EQ(bitMap->buffer[i], 0);
    }
}

TEST_F(BitMapTest, TestSetGet) {
    bitMap->Init(); 
    EXPECT_FALSE(bitMap->Get(0));
    EXPECT_FALSE(bitMap->Get(33));
    bitMap->Set(33, true);
    EXPECT_TRUE(bitMap->Get(33));
}
    